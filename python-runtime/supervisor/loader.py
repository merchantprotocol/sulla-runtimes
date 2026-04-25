"""Function loader — per-function venv isolation via uv.

Each loaded function gets its own virtualenv, content-addressed by the SHA256
of its requirements.txt. The venv is created on first load and reused on
subsequent loads with identical requirements.

Functions without a requirements.txt run against the supervisor Python
environment without any extra packages.

Invocation uses a subprocess for each call (see invoker.py), so each function
runs in full isolation: no sys.modules conflicts, no package-version shadowing,
no handler crash taking down the supervisor.

Routines (workflow DAGs) are NEVER loaded here.
"""

from __future__ import annotations

import hashlib
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)


class RoutineLoadError(Exception):
    """Raised when a function cannot be loaded. Mapped to HTTP 400."""


@dataclass
class LoadedRoutine:
    name:        str
    version:     str
    path:        Path
    entrypoint:  str
    module_file: Path        # absolute path to the .py entrypoint file
    func_name:   str         # handler function name within the module
    manifest:    dict[str, Any]
    kind:        str = "Function"
    venv_path:   Path | None = field(default=None)  # None → use sys.executable
    timeout_s:   float = field(default=60.0)


class RoutineLoader:
    VENV_CACHE_DIR = Path(os.environ.get("SULLA_VENV_CACHE", "/opt/sulla/cache/venvs"))

    def __init__(self, routines_dir: str):
        self.routines_dir = Path(routines_dir)
        self._loaded: dict[str, LoadedRoutine] = {}
        self.VENV_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _key(name: str, version: str) -> str:
        return f"{name}@{version}"

    @staticmethod
    def _parse_timeout_s(raw: Any, default: float = 60.0) -> float:
        if raw is None:
            return default
        if isinstance(raw, (int, float)):
            return float(raw)
        s = str(raw).strip().lower()
        units = {"ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0}
        for suffix, mult in units.items():
            if s.endswith(suffix):
                try:
                    return float(s[: -len(suffix)]) * mult
                except ValueError:
                    break
        return default

    def resolve_venv(self, entrypoint_file: Path) -> Path | None:
        """Create (or reuse) a per-function venv for the function's requirements.

        Content-addressed by SHA256[:16] of requirements.txt so two functions
        with identical deps share one venv. Returns the venv Path, or None if
        the function has no requirements.txt. Raises RoutineLoadError on failure.
        """
        reqs = entrypoint_file.parent / "requirements.txt"
        if not reqs.is_file():
            return None

        content = reqs.read_bytes()
        digest = hashlib.sha256(content).hexdigest()[:16]
        venv_dir = self.VENV_CACHE_DIR / digest
        marker = venv_dir / ".installed"

        if marker.exists():
            logger.debug("Venv cache hit: %s (%s)", reqs, digest)
            return venv_dir

        if shutil.which("uv") is None:
            logger.warning(
                "uv not on PATH; skipping venv creation for %s. "
                "Function will run against the supervisor Python environment.", reqs,
            )
            return None

        # Clean any partial venv from a previous failed attempt.
        if venv_dir.exists():
            shutil.rmtree(venv_dir, ignore_errors=True)

        logger.info("Creating venv %s for %s", venv_dir, reqs)
        try:
            subprocess.run(
                ["uv", "venv", str(venv_dir), "--python", sys.executable],
                check=True,
                capture_output=True,
                text=True,
                timeout=60,
            )
        except subprocess.CalledProcessError as err:
            shutil.rmtree(venv_dir, ignore_errors=True)
            raise RoutineLoadError(
                f"uv venv creation failed for {reqs}: "
                f"exit {err.returncode}\n{err.stderr or err.stdout}"
            ) from err
        except subprocess.TimeoutExpired:
            shutil.rmtree(venv_dir, ignore_errors=True)
            raise RoutineLoadError(f"uv venv creation timed out for {reqs}") from None

        logger.info("Installing deps for %s into venv %s", reqs, venv_dir)
        try:
            subprocess.run(
                [
                    "uv", "pip", "install",
                    "--python", str(venv_dir / "bin" / "python"),
                    "--requirement", str(reqs),
                ],
                check=True,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.CalledProcessError as err:
            shutil.rmtree(venv_dir, ignore_errors=True)
            raise RoutineLoadError(
                f"uv pip install failed for {reqs}: "
                f"exit {err.returncode}\n{err.stderr or err.stdout}"
            ) from err
        except subprocess.TimeoutExpired:
            shutil.rmtree(venv_dir, ignore_errors=True)
            raise RoutineLoadError(f"uv pip install timed out for {reqs}") from None

        marker.touch()
        logger.info("Venv ready: %s", venv_dir)
        return venv_dir

    def load(self, name: str, version: str, path: str | None = None) -> LoadedRoutine:
        routine_path = Path(path) if path else self.routines_dir / name
        if not routine_path.is_dir():
            raise RoutineLoadError(f"Routine directory not found: {routine_path}")

        manifest_path = routine_path / "function.yaml"
        if not manifest_path.is_file():
            raise RoutineLoadError(f"function.yaml not found in {routine_path}")

        try:
            with manifest_path.open() as f:
                manifest: dict[str, Any] = yaml.safe_load(f) or {}
        except yaml.YAMLError as err:
            raise RoutineLoadError(f"Invalid YAML in {manifest_path}: {err}") from err

        kind = manifest.get("kind")
        if kind != "Function":
            raise RoutineLoadError(
                f"python-runtime only loads functions; got kind: {kind!r}. "
                f"Routines are orchestrated by the workflow engine, not runtimes."
            )

        manifest_name = manifest.get("slug") or manifest.get("name")
        if manifest_name and manifest_name != name:
            logger.warning(
                "Load request name=%s but manifest says slug=%s", name, manifest_name,
            )

        spec = manifest.get("spec") or {}
        if spec.get("runtime") != "python":
            raise RoutineLoadError(
                f"Wrong runtime for python-runtime: got {spec.get('runtime')!r}"
            )

        entrypoint = spec.get("entrypoint")
        if not entrypoint or "::" not in entrypoint:
            raise RoutineLoadError(
                "spec.entrypoint must be in format 'relative/path.py::function_name'"
            )

        file_rel, func_name = entrypoint.split("::", 1)
        module_file = routine_path / file_rel
        if not module_file.is_file():
            raise RoutineLoadError(f"Entrypoint file not found: {module_file}")

        venv_path = self.resolve_venv(module_file)
        timeout_s = self._parse_timeout_s(spec.get("timeout"))

        loaded = LoadedRoutine(
            name=name,
            version=version,
            path=routine_path,
            entrypoint=entrypoint,
            module_file=module_file,
            func_name=func_name,
            manifest=manifest,
            kind=kind,
            venv_path=venv_path,
            timeout_s=timeout_s,
        )
        self._loaded[self._key(name, version)] = loaded
        logger.info(
            "Loaded %s %s@%s from %s (venv: %s)",
            kind, name, version, routine_path,
            str(venv_path) if venv_path else "none",
        )
        return loaded

    def unload(self, name: str, version: str) -> bool:
        key = self._key(name, version)
        if key not in self._loaded:
            return False
        self._loaded.pop(key)
        logger.info("Unloaded %s", key)
        return True

    def get(self, name: str, version: str) -> LoadedRoutine | None:
        return self._loaded.get(self._key(name, version))

    def list_loaded(self) -> dict[str, dict[str, Any]]:
        return {
            key: {
                "kind":       r.kind,
                "name":       r.name,
                "version":    r.version,
                "path":       str(r.path),
                "entrypoint": r.entrypoint,
                "venv_path":  str(r.venv_path) if r.venv_path else None,
                "timeout_s":  r.timeout_s,
            }
            for key, r in self._loaded.items()
        }
