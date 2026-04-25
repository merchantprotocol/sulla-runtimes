"""Shell invoker — forks bash subprocess per invocation, pipes inputs/outputs as JSON.

Contract with shell entrypoints:
  - Inputs are delivered on stdin as a single JSON document.
  - Output is expected on stdout as a single JSON document (a JSON object).
  - Non-zero exit WITH valid JSON on stdout — treated as success (shell routines
    like shell-command legitimately return exit_code/stdout/stderr and can be
    non-zero). Non-zero exit WITHOUT valid JSON on stdout — treated as failure.
  - stderr is captured and included in the InvocationError message on failure.

Secrets never cross the invoke body. The caller provides a capability
`secretsToken` + `secretsHostUrl`; we fetch each declared env var just-in-time
from the host, pass them via subprocess env=, and invalidate the token after.
Alpine base image does not ship httpx, so we use `urllib.request` from stdlib.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from supervisor.loader import ShellLoader

logger = logging.getLogger(__name__)

SECRETS_FETCH_TIMEOUT_S = 5.0


class InvocationError(Exception):
    """Wraps any failure from a shell handler. Mapped to HTTP 500."""


class SecretsFetchError(Exception):
    """Raised when a declared env var cannot be fetched from the host.

    Message includes the env var NAME only — never the token, integration slug,
    or fetched value.
    """


@dataclass
class InvocationResult:
    outputs:     dict[str, Any]
    duration_ms: int


def _redact(text: str, secrets: list[str]) -> str:
    """Replace any occurrence of a secret VALUE with '***'. Longest-first."""
    if not text or not secrets:
        return text
    out = text
    for s in sorted((v for v in secrets if v), key=len, reverse=True):
        if s and s in out:
            out = out.replace(s, "***")
    return out


def _collect_env_var_names(manifest: dict[str, Any]) -> list[str]:
    """Union the KEYS of every `spec.integrations[].env` object in the manifest.

    Emits a one-line WARN if two integrations declare the same env var name
    (last-wins). The integration slug(s) are deliberately NOT logged.
    """
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
        logger.warning(
            "Duplicate env var names declared across integrations (last-wins): %s",
            sorted(duplicates),
        )
    return ordered


def _fetch_one_secret(host_url: str, token: str, key: str) -> str:
    """Blocking POST to {host_url}/secrets/fetch. Intended for thread use."""
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
        raise SecretsFetchError(
            f"fetch returned no value for env var {key!r}"
        )
    value = payload["value"]
    if not isinstance(value, str):
        raise SecretsFetchError(
            f"fetch returned non-string value for env var {key!r}"
        )
    return value


def _invalidate_token(host_url: str, token: str) -> None:
    """Best-effort token invalidation. Never raises. Never logs the token."""
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
        logger.warning(
            "secrets/invalidate best-effort failed: %s", type(err).__name__,
        )


class ShellInvoker:
    def __init__(self, loader: ShellLoader):
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

        start = time.monotonic()
        inputs_json = json.dumps(inputs).encode("utf-8")

        env_var_names = _collect_env_var_names(loaded.manifest)
        needs_secrets = bool(secrets_token) and bool(env_var_names)

        fetched_env: dict[str, str] = {}
        secret_values: list[str] = []
        subproc_env: dict[str, str] | None = None

        try:
            if needs_secrets:
                if not secrets_host_url:
                    raise InvocationError(
                        "secretsToken provided without secretsHostUrl"
                    )
                for key in env_var_names:
                    try:
                        value = await asyncio.to_thread(
                            _fetch_one_secret, secrets_host_url, secrets_token, key,
                        )
                    except SecretsFetchError as err:
                        raise InvocationError(str(err)) from None
                    fetched_env[key] = value

                secret_values = [v for v in fetched_env.values() if v]

            # Build subprocess env: base OS env + fetched secrets + direct env.
            # Always create a new dict so the supervisor's os.environ is never mutated.
            if fetched_env or direct_env:
                subproc_env = {**os.environ, **fetched_env, **(direct_env or {})}

            try:
                try:
                    proc = await asyncio.create_subprocess_exec(
                        "bash", str(loaded.script),
                        stdin=asyncio.subprocess.PIPE,
                        stdout=asyncio.subprocess.PIPE,
                        stderr=asyncio.subprocess.PIPE,
                        cwd=str(loaded.path),
                        env=subproc_env,
                    )
                except FileNotFoundError as err:
                    raise InvocationError(f"bash not available: {err}") from err

                try:
                    stdout, stderr = await asyncio.wait_for(
                        proc.communicate(input=inputs_json),
                        timeout=loaded.timeout_s,
                    )
                except asyncio.TimeoutError as err:
                    proc.kill()
                    await proc.wait()
                    raise InvocationError(
                        f"Handler timed out after {loaded.timeout_s}s"
                    ) from err

                duration_ms = int((time.monotonic() - start) * 1000)

                stdout_str = stdout.decode("utf-8", errors="replace")
                stderr_str = stderr.decode("utf-8", errors="replace")

                outputs: dict[str, Any] | None = None
                parse_err: Exception | None = None
                if stdout_str.strip():
                    try:
                        parsed = json.loads(stdout_str)
                        if isinstance(parsed, dict):
                            outputs = parsed
                        else:
                            parse_err = TypeError(
                                f"Expected JSON object on stdout, got {type(parsed).__name__}"
                            )
                    except json.JSONDecodeError as err:
                        parse_err = err

                if outputs is not None:
                    return InvocationResult(outputs=outputs, duration_ms=duration_ms)

                # Failed to get structured outputs. Redact secret values from
                # stderr/stdout before surfacing.
                safe_stderr = _redact(stderr_str, secret_values)
                safe_stdout = _redact(stdout_str, secret_values)
                safe_parse_err = _redact(str(parse_err), secret_values) if parse_err else ""

                details: list[str] = []
                if proc.returncode:
                    details.append(f"exit={proc.returncode}")
                if parse_err:
                    details.append(f"parse_error={safe_parse_err}")
                if safe_stderr.strip():
                    details.append(f"stderr={safe_stderr.strip()[:500]}")
                if safe_stdout.strip():
                    details.append(f"stdout={safe_stdout.strip()[:200]}")
                raise InvocationError("; ".join(details) or "handler produced no output")
            except InvocationError as err:
                # Defensive second pass — any InvocationError we raise must be
                # redacted, even ones from the timeout / spawn path above.
                msg = _redact(str(err), secret_values)
                if msg != str(err):
                    raise InvocationError(msg) from None
                raise
        finally:
            # Zero out local references to secret material BEFORE attempting
            # the invalidate RPC so that state is cleared even if it raises.
            if subproc_env is not None:
                subproc_env.clear()
            subproc_env = None
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
