"""Shell routine/function loader — manifest parsing + path caching.

Unlike python-runtime, there's no importlib step. Shell scripts are invoked
as subprocesses on each /invoke call. "Loading" here just means: validate the
manifest, confirm the entrypoint file exists, and cache the resolved path.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class ShellLoadError(Exception):
    """Raised when a routine/function cannot be loaded. Mapped to HTTP 400."""


@dataclass
class LoadedShell:
    name:       str
    version:    str
    path:       Path
    entrypoint: str          # relative path, e.g. "main.sh" or "functions/main.sh"
    script:     Path         # absolute path to the .sh file
    kind:       str          # "Routine" | "Function"
    manifest:   dict[str, Any]
    timeout_s:  float


class ShellLoader:
    DEFAULT_TIMEOUT_S = 300.0  # matches spec.timeout default in shell-command routine

    def __init__(self, routines_dir: str):
        self.routines_dir = Path(routines_dir)
        self._loaded: dict[str, LoadedShell] = {}

    @staticmethod
    def _key(name: str, version: str) -> str:
        return f"{name}@{version}"

    def load(self, name: str, version: str, path: str | None = None) -> LoadedShell:
        unit_path = Path(path) if path else self.routines_dir / name
        if not unit_path.is_dir():
            raise ShellLoadError(f"Routine directory not found: {unit_path}")

        manifest_path = unit_path / "function.yaml"
        if not manifest_path.is_file():
            raise ShellLoadError(
                f"function.yaml not found in {unit_path}"
            )

        try:
            with manifest_path.open() as f:
                manifest: dict[str, Any] = yaml.safe_load(f) or {}
        except yaml.YAMLError as err:
            raise ShellLoadError(f"Invalid YAML in {manifest_path}: {err}") from err

        kind = manifest.get("kind")
        if kind != "Function":
            raise ShellLoadError(
                f"shell-runtime only loads functions; got kind: {kind!r}. "
                f"Routines are orchestrated by the workflow engine, not runtimes."
            )

        spec = manifest.get("spec") or {}
        if spec.get("runtime") != "shell":
            raise ShellLoadError(
                f"Wrong runtime for shell-runtime: got {spec.get('runtime')!r}"
            )

        entrypoint = spec.get("entrypoint")
        if not entrypoint:
            raise ShellLoadError("spec.entrypoint is required")
        if "::" in entrypoint:
            raise ShellLoadError(
                "shell entrypoint should be a plain file path (no '::function_name')"
            )

        script = unit_path / entrypoint
        if not script.is_file():
            raise ShellLoadError(f"Entrypoint file not found: {script}")

        timeout_s = self._parse_timeout(spec.get("timeout"))

        loaded = LoadedShell(
            name=name,
            version=version,
            path=unit_path,
            entrypoint=entrypoint,
            script=script,
            kind=kind,
            manifest=manifest,
            timeout_s=timeout_s,
        )
        self._loaded[self._key(name, version)] = loaded
        logger.info("Loaded %s %s@%s from %s", kind, name, version, unit_path)
        return loaded

    @classmethod
    def _parse_timeout(cls, raw: Any) -> float:
        if raw is None:
            return cls.DEFAULT_TIMEOUT_S
        if isinstance(raw, (int, float)):
            return float(raw)
        s = str(raw).strip().lower()
        units = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
        for suffix, mult in units.items():
            if s.endswith(suffix):
                try:
                    return float(s[:-len(suffix)]) * mult
                except ValueError:
                    break
        return cls.DEFAULT_TIMEOUT_S

    def unload(self, name: str, version: str) -> bool:
        key = self._key(name, version)
        if key not in self._loaded:
            return False
        self._loaded.pop(key)
        logger.info("Unloaded %s", key)
        return True

    def get(self, name: str, version: str) -> LoadedShell | None:
        return self._loaded.get(self._key(name, version))

    def list_loaded(self) -> dict[str, dict[str, Any]]:
        return {
            key: {
                "kind":       r.kind,
                "name":       r.name,
                "version":    r.version,
                "path":       str(r.path),
                "entrypoint": r.entrypoint,
                "script":     str(r.script),
                "timeout_s":  r.timeout_s,
            }
            for key, r in self._loaded.items()
        }
