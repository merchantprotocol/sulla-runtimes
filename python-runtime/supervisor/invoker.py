"""Routine invoker — runs handlers as subprocesses in their per-function venv.

Each invocation spawns a fresh Python subprocess using the function's dedicated
virtualenv binary (or sys.executable for functions without requirements.txt).
Inputs are piped to stdin as JSON; outputs are read from stdout.

Isolation guarantees:
  - No sys.modules pollution: each subprocess starts with a clean import state.
  - No package-version conflicts: each function's venv is fully isolated.
  - Handler crash or OOM doesn't affect the supervisor process.
  - Secrets are injected as env vars into the subprocess only, never into the
    supervisor's own os.environ.

Secrets:
  Caller provides a capability secretsToken + secretsHostUrl. The invoker
  fetches each declared env var just-in-time from the host, injects them into
  the subprocess environment, and invalidates the token in finally. Secret
  values never appear in logs or exception messages.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from supervisor.loader import RoutineLoader, RoutineLoadError

logger = logging.getLogger(__name__)

SECRETS_FETCH_TIMEOUT_S = 5.0

# Runner executed inside each invocation subprocess via `python -c`.
# Env vars _SULLA_FN_FILE, _SULLA_FN_FUNC, _SULLA_FN_DIR are set by the invoker.
# Using env vars avoids any shell-quoting issues with paths that contain spaces.
_FN_RUNNER = """\
import json, sys, importlib.util, os, inspect

_fn_file = os.environ['_SULLA_FN_FILE']
_fn_func = os.environ['_SULLA_FN_FUNC']
_fn_dir  = os.environ['_SULLA_FN_DIR']

sys.path.insert(0, _fn_dir)

_spec = importlib.util.spec_from_file_location(
    '__sulla_fn__', _fn_file,
    submodule_search_locations=[_fn_dir],
)
_mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_mod)

_handler = getattr(_mod, _fn_func)
_inputs = json.load(sys.stdin)

if inspect.iscoroutinefunction(_handler):
    import asyncio as _asyncio
    _result = _asyncio.run(_handler(_inputs))
else:
    _result = _handler(_inputs)

json.dump(_result, sys.stdout)
"""


class InvocationError(Exception):
    """Wraps any exception raised by a routine handler. Mapped to HTTP 500."""


class SecretsFetchError(Exception):
    """Raised when a declared env var cannot be fetched from the host."""


@dataclass
class InvocationResult:
    outputs:     dict[str, Any]
    duration_ms: int


def _redact(text: str, secrets: list[str]) -> str:
    if not text or not secrets:
        return text
    out = text
    for s in sorted((v for v in secrets if v), key=len, reverse=True):
        if s and s in out:
            out = out.replace(s, "***")
    return out


def _collect_env_var_names(manifest: dict[str, Any]) -> list[str]:
    spec = manifest.get("spec") or {}
    integrations = spec.get("integrations") or []
    if not isinstance(integrations, list):
        return []
    seen: set[str] = set()
    duplicates: set[str] = set()
    ordered: list[str] = []
    for entry in integrations:
        if not isinstance(entry, dict):
            continue
        env_map = entry.get("env") or {}
        if not isinstance(env_map, dict):
            continue
        for key in env_map.keys():
            if not isinstance(key, str) or not key:
                continue
            if key in seen:
                duplicates.add(key)
            else:
                seen.add(key)
                ordered.append(key)
    if duplicates:
        logger.warning("Duplicate env var names declared (last-wins): %s", sorted(duplicates))
    return ordered


def _fetch_one_secret(host_url: str, token: str, key: str) -> str:
    body = json.dumps({"token": token, "key": key}).encode("utf-8")
    req = urllib.request.Request(
        url=host_url.rstrip("/") + "/secrets/fetch",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=SECRETS_FETCH_TIMEOUT_S) as resp:
            payload = json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as err:
        raise SecretsFetchError(
            f"fetch denied for env var {key!r} (status {err.code})"
        ) from None
    except (urllib.error.URLError, TimeoutError, OSError) as err:
        raise SecretsFetchError(
            f"fetch transport failure for env var {key!r}: {type(err).__name__}"
        ) from None
    except json.JSONDecodeError:
        raise SecretsFetchError(
            f"fetch returned malformed JSON for env var {key!r}"
        ) from None
    if not isinstance(payload, dict) or "value" not in payload:
        raise SecretsFetchError(f"fetch returned no value for env var {key!r}")
    value = payload["value"]
    if not isinstance(value, str):
        raise SecretsFetchError(f"fetch returned non-string value for env var {key!r}")
    return value


def _invalidate_token(host_url: str, token: str) -> None:
    body = json.dumps({"token": token}).encode("utf-8")
    req = urllib.request.Request(
        url=host_url.rstrip("/") + "/secrets/invalidate",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=SECRETS_FETCH_TIMEOUT_S) as resp:
            resp.read()
    except Exception as err:
        logger.warning("secrets/invalidate best-effort failed: %s", type(err).__name__)


class RoutineInvoker:
    def __init__(self, loader: RoutineLoader):
        self.loader = loader

    async def invoke(
        self,
        name:             str,
        version:          str,
        inputs:           dict[str, Any],
        secrets_token:    str | None = None,
        secrets_host_url: str | None = None,
        direct_env:       dict[str, str] | None = None,
    ) -> InvocationResult:
        loaded = self.loader.get(name, version)
        if loaded is None:
            loaded = self.loader.load(name, version)

        env_var_names = _collect_env_var_names(loaded.manifest)
        needs_secrets = bool(secrets_token) and bool(env_var_names)

        fetched_env: dict[str, str] = {}
        secret_values: list[str] = []
        start = time.monotonic()

        try:
            if needs_secrets:
                if not secrets_host_url:
                    raise InvocationError("secretsToken provided without secretsHostUrl")
                for key in env_var_names:
                    try:
                        value = await asyncio.to_thread(
                            _fetch_one_secret, secrets_host_url, secrets_token, key,
                        )
                    except SecretsFetchError as err:
                        raise InvocationError(str(err)) from None
                    fetched_env[key] = value
                secret_values = [v for v in fetched_env.values() if v]

            # Build the subprocess env: base OS env + fetched secrets + direct env + fn locators.
            # Secrets go directly to the subprocess — the supervisor's os.environ
            # is never modified, so concurrent invocations can't race on env vars.
            python_bin = (
                str(loaded.venv_path / "bin" / "python")
                if loaded.venv_path
                else sys.executable
            )
            proc_env = {
                **os.environ,
                **fetched_env,
                **(direct_env or {}),
                "_SULLA_FN_FILE": str(loaded.module_file),
                "_SULLA_FN_FUNC": loaded.func_name,
                "_SULLA_FN_DIR":  str(loaded.path),
            }

            proc = await asyncio.create_subprocess_exec(
                python_bin, "-c", _FN_RUNNER,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=proc_env,
            )
            try:
                stdout_data, stderr_data = await asyncio.wait_for(
                    proc.communicate(json.dumps(inputs).encode("utf-8")),
                    timeout=loaded.timeout_s,
                )
            except asyncio.TimeoutError:
                proc.kill()
                await proc.wait()
                raise InvocationError(
                    f"Handler timed out after {loaded.timeout_s}s"
                ) from None

            if proc.returncode != 0:
                stderr_text = _redact(
                    stderr_data.decode("utf-8", errors="replace").strip(),
                    secret_values,
                )
                logger.error(
                    "Handler subprocess exited %d for %s@%s",
                    proc.returncode, name, version,
                )
                raise InvocationError(
                    f"Function subprocess exited {proc.returncode}: {stderr_text}"
                )

            try:
                result = json.loads(stdout_data.decode("utf-8"))
            except json.JSONDecodeError as err:
                raise InvocationError(
                    f"Function output was not valid JSON: {err}"
                ) from None

            if not isinstance(result, dict):
                raise InvocationError(
                    f"Handler returned {type(result).__name__}, expected dict"
                )

        finally:
            fetched_env.clear()
            secret_values = []
            if needs_secrets and secrets_token and secrets_host_url:
                try:
                    await asyncio.to_thread(
                        _invalidate_token, secrets_host_url, secrets_token,
                    )
                except Exception as err:
                    logger.warning(
                        "token invalidate raised unexpectedly: %s",
                        type(err).__name__,
                    )

        duration_ms = int((time.monotonic() - start) * 1000)
        return InvocationResult(outputs=result, duration_ms=duration_ms)
