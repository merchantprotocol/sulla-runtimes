"""Routine invoker — dispatches to loaded handlers, handles sync/async uniformly.

Secrets are NEVER passed in the /invoke request body. Instead, the caller
provides a capability `secretsToken` + `secretsHostUrl`. Before running the
handler, the invoker enumerates the env var NAMES declared in the function's
`spec.integrations[].env` and fetches each value just-in-time from the host.
After the handler completes (success or failure), the token is invalidated.

We deliberately use `urllib.request` from the stdlib here to avoid pulling in
another HTTP client. The calls are small, synchronous, and happen inside an
`asyncio.to_thread` so they do not block the event loop.
"""

from __future__ import annotations

import asyncio
import inspect
import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from supervisor.loader import RoutineLoader

logger = logging.getLogger(__name__)

SECRETS_FETCH_TIMEOUT_S = 5.0


class InvocationError(Exception):
    """Wraps any exception raised by a routine handler. Mapped to HTTP 500."""


class SecretsFetchError(Exception):
    """Raised when a declared env var cannot be fetched from the host.

    The message includes the env var NAME only — never the token, never the
    integration slug, never the fetched value.
    """


@dataclass
class InvocationResult:
    outputs:     dict[str, Any]
    duration_ms: int


def _redact(text: str, secrets: list[str]) -> str:
    """Replace any occurrence of a secret VALUE in `text` with '***'.

    Operates on raw strings and applies to every element of `secrets` that is
    non-empty. Sorted longest-first so overlapping prefixes don't leak.
    """
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
    (last-wins). We deliberately do NOT log the integration slug(s) involved.
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
        # 4xx/5xx. Do NOT include the response body in the raised message —
        # the host echoes {"error": "..."} but we must not surface anything
        # that could imply the key's value or the token.
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
        # Don't raise. Log type only.
        logger.warning(
            "secrets/invalidate best-effort failed: %s", type(err).__name__,
        )


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
    ) -> InvocationResult:
        loaded = self.loader.get(name, version)
        if loaded is None:
            # Lazy-load on first invocation.
            loaded = self.loader.load(name, version)

        handler = loaded.handler
        start = time.monotonic()

        env_var_names = _collect_env_var_names(loaded.manifest)

        # Branch 1: no secrets requested OR function declares no integrations.
        # Skip all secret handling and run the handler directly.
        needs_secrets = bool(secrets_token) and bool(env_var_names)

        fetched_env: dict[str, str] = {}
        secret_values: list[str] = []
        original_env_snapshot: dict[str, str] = {}

        try:
            if needs_secrets:
                if not secrets_host_url:
                    raise InvocationError(
                        "secretsToken provided without secretsHostUrl"
                    )
                # Fetch every declared env var up-front. If any one fails,
                # abort the whole invocation (no partial execution).
                for key in env_var_names:
                    try:
                        value = await asyncio.to_thread(
                            _fetch_one_secret, secrets_host_url, secrets_token, key,
                        )
                    except SecretsFetchError as err:
                        # Message contains only the env var NAME, never the value.
                        raise InvocationError(str(err)) from None
                    fetched_env[key] = value

                secret_values = [v for v in fetched_env.values() if v]
                original_env_snapshot = os.environ.copy()

                # Overlay onto os.environ for the duration of this handler call.
                # This leaks to any concurrently-running handler in the same
                # process; the supervisor accepts that tradeoff for the Python
                # runtime (see TODO: sub-interpreter).
                for k, v in fetched_env.items():
                    os.environ[k] = v

            try:
                if inspect.iscoroutinefunction(handler):
                    result = await handler(inputs)
                else:
                    # Run sync handlers in a worker thread so they can't block
                    # the event loop handling concurrent invocations.
                    result = await asyncio.to_thread(handler, inputs)
            except Exception as err:
                # Redact secret VALUES from the exception message before
                # surfacing it. Do NOT log the env dict at any level.
                safe_type = type(err).__name__
                safe_msg = _redact(str(err), secret_values)
                # Use a non-exception log line so traceback strings (which may
                # contain secrets via repr of locals) are not emitted.
                logger.error(
                    "Handler raised for %s@%s: %s", name, version, safe_type,
                )
                raise InvocationError(f"{safe_type}: {safe_msg}") from None
        finally:
            # Restore os.environ exactly — remove added keys, restore any
            # overwritten originals.
            if fetched_env and original_env_snapshot is not None:
                for k in list(fetched_env.keys()):
                    if k in original_env_snapshot:
                        os.environ[k] = original_env_snapshot[k]
                    else:
                        os.environ.pop(k, None)
            # Zero local references to secret material BEFORE attempting the
            # invalidate RPC — in case that RPC raises, we still clear state.
            fetched_env.clear()
            secret_values = []
            original_env_snapshot = {}

            # Best-effort invalidate the capability token — run off-loop, never
            # raise. Guarded by `needs_secrets` because hosts that weren't
            # asked for secrets shouldn't be pinged.
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

        if not isinstance(result, dict):
            raise InvocationError(
                f"Handler returned {type(result).__name__}, expected dict"
            )

        return InvocationResult(outputs=result, duration_ms=duration_ms)
