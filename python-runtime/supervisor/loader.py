"""Function loader — importlib-based with per-function namespaces and
content-addressable dep caching via `uv`.

Each loaded function gets a unique `sys.modules` namespace so module-level
globals don't collide. If the function ships a `requirements.txt` in the same
directory as the entrypoint, the supervisor installs those deps into a cache
directory keyed by the file's SHA256 and prepends the install target to
`sys.path` for the duration of the function's lifetime.

Routines (workflow DAGs) are NEVER loaded here — they're orchestrated by the
workflow engine. Manifests declaring `kind: Routine` are rejected at load.

MVP omissions tracked in runtime-containers.md:
  - no filesystem watcher (reload is explicit via /load)
  - no permission enforcement (network proxy, env injection, fs sandbox)
  - no sub-interpreter isolation (conflicting dep versions: first-wins)
"""

from __future__ import annotations

import hashlib
import importlib.util
import logging
import os
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

import yaml

logger = logging.getLogger(__name__)


class RoutineLoadError(Exception):
    """Raised when a function cannot be loaded. Mapped to HTTP 400."""


@dataclass
class LoadedRoutine:
    name:       str
    version:    str
    path:       Path
    entrypoint: str
    handler:    Callable[[dict[str, Any]], Any]
    manifest:   dict[str, Any]
    kind:       str = "Function"
    deps_path:  str | None = field(default=None)  # sys.path entry we added


class RoutineLoader:
    DEPS_CACHE_DIR = Path(os.environ.get("SULLA_DEPS_CACHE", "/opt/sulla/cache/deps"))

    def __init__(self, routines_dir: str):
        self.routines_dir = Path(routines_dir)
        self._loaded: dict[str, LoadedRoutine] = {}
        self.DEPS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _key(name: str, version: str) -> str:
        return f"{name}@{version}"

    @staticmethod
    def _module_name(name: str, version: str) -> str:
        safe_version = version.replace(".", "_").replace("-", "_").replace("+", "_")
        safe_name = name.replace("-", "_")
        return f"sulla.routines.{safe_name}_v{safe_version}"

    def _resolve_deps(self, entrypoint_file: Path) -> str | None:
        """Install deps from requirements.txt next to the entrypoint.

        Returns the absolute path to prepend to sys.path, or None if no
        requirements.txt exists. Install target is cached by content-hash so
        repeated loads hit the cache.
        """
        reqs = entrypoint_file.parent / "requirements.txt"
        if not reqs.is_file():
            return None

        content = reqs.read_bytes()
        digest = hashlib.sha256(content).hexdigest()[:16]
        target = self.DEPS_CACHE_DIR / digest
        marker = target / ".installed"

        if marker.exists():
            logger.debug("Deps cache hit: %s (%s)", reqs, digest)
            return str(target)

        if shutil.which("uv") is None:
            # Allow local dev without uv installed. Container always has it.
            logger.warning(
                "uv not on PATH; skipping dep install for %s. "
                "Falling back to ambient site-packages.", reqs,
            )
            return None

        target.mkdir(parents=True, exist_ok=True)
        logger.info("Installing deps for %s into %s", reqs, target)
        try:
            subprocess.run(
                ["uv", "pip", "install",
                 "--target", str(target),
                 "--requirement", str(reqs)],
                check=True,
                capture_output=True,
                text=True,
                timeout=180,
            )
        except subprocess.CalledProcessError as err:
            shutil.rmtree(target, ignore_errors=True)
            raise RoutineLoadError(
                f"uv install failed for {reqs}: "
                f"exit {err.returncode}\n{err.stderr or err.stdout}"
            ) from err
        except subprocess.TimeoutExpired as err:
            shutil.rmtree(target, ignore_errors=True)
            raise RoutineLoadError(f"uv install timed out for {reqs}") from err

        marker.touch()
        return str(target)

    def load(self, name: str, version: str, path: str | None = None) -> LoadedRoutine:
        routine_path = Path(path) if path else self.routines_dir / name
        if not routine_path.is_dir():
            raise RoutineLoadError(
                f"Routine directory not found: {routine_path}"
            )

        manifest_path = routine_path / "function.yaml"
        if not manifest_path.is_file():
            raise RoutineLoadError(
                f"function.yaml not found in {routine_path}"
            )

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

        # Resolve deps (install into content-hashed cache) before import.
        deps_path = self._resolve_deps(module_file)

        module_name = self._module_name(name, version)

        # Evict any prior incarnation of this module before re-import.
        if module_name in sys.modules:
            del sys.modules[module_name]

        if deps_path and deps_path not in sys.path:
            sys.path.insert(0, deps_path)

        spec_obj = importlib.util.spec_from_file_location(
            module_name, module_file, submodule_search_locations=[str(routine_path)],
        )
        if spec_obj is None or spec_obj.loader is None:
            raise RoutineLoadError(
                f"Failed to create import spec for {module_file}"
            )

        module = importlib.util.module_from_spec(spec_obj)
        sys.modules[module_name] = module
        try:
            spec_obj.loader.exec_module(module)
        except Exception as err:
            sys.modules.pop(module_name, None)
            if deps_path and deps_path in sys.path:
                sys.path.remove(deps_path)
            raise RoutineLoadError(
                f"Failed to import {module_file}: {type(err).__name__}: {err}"
            ) from err

        handler = getattr(module, func_name, None)
        if handler is None:
            raise RoutineLoadError(
                f"Entrypoint function {func_name!r} not found in {module_file}"
            )
        if not callable(handler):
            raise RoutineLoadError(
                f"Entrypoint {func_name!r} in {module_file} is not callable"
            )

        loaded = LoadedRoutine(
            name=name,
            version=version,
            path=routine_path,
            entrypoint=entrypoint,
            handler=handler,
            manifest=manifest,
            kind=kind,
            deps_path=deps_path,
        )
        self._loaded[self._key(name, version)] = loaded
        logger.info("Loaded %s %s@%s from %s", kind, name, version, routine_path)
        return loaded

    def unload(self, name: str, version: str) -> bool:
        key = self._key(name, version)
        loaded = self._loaded.pop(key, None)
        if loaded is None:
            return False
        sys.modules.pop(self._module_name(name, version), None)
        # Remove the deps path from sys.path — but only if no other loaded
        # unit still references the same cache directory.
        if loaded.deps_path:
            still_used = any(
                other.deps_path == loaded.deps_path
                for other in self._loaded.values()
            )
            if not still_used and loaded.deps_path in sys.path:
                sys.path.remove(loaded.deps_path)
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
                "deps_path":  r.deps_path,
            }
            for key, r in self._loaded.items()
        }
