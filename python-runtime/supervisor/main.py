"""python-runtime supervisor — FastAPI app listening on an HTTP port.

Loads Sulla functions (single units of code) and dispatches invocations.
Routines (workflow DAGs) are orchestrated by the workflow engine — this
runtime only runs functions.
"""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path

import uvicorn
import yaml
from fastapi import FastAPI, HTTPException

from supervisor.invoker import InvocationError, RoutineInvoker
from supervisor.loader import RoutineLoader, RoutineLoadError
from supervisor.schemas import (
    HealthResponse,
    InstallRequest,
    InstallResponse,
    InvokeRequest,
    InvokeResponse,
    ListRoutinesResponse,
    LoadRequest,
    LoadResponse,
    UnloadRequest,
    UnloadResponse,
)

_log_level = os.environ.get("SULLA_LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=_log_level,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger("sulla.python-runtime")

HTTP_HOST     = os.environ.get("SULLA_HTTP_HOST",      "0.0.0.0")
HTTP_PORT     = int(os.environ.get("SULLA_HTTP_PORT",  "8080"))
FUNCTIONS_DIR = os.environ.get("SULLA_FUNCTIONS_DIR",  "/var/functions")

loader   = RoutineLoader(routines_dir=FUNCTIONS_DIR)
invoker  = RoutineInvoker(loader=loader)

app = FastAPI(title="sulla.python-runtime", version="0.1.0")


@app.get("/health", response_model=HealthResponse)
async def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        loaded_routines=sorted(loader.list_loaded().keys()),
        routines_dir=FUNCTIONS_DIR,
    )


@app.get("/routines", response_model=ListRoutinesResponse)
async def routines() -> ListRoutinesResponse:
    return ListRoutinesResponse(routines=loader.list_loaded())


@app.post("/install", response_model=InstallResponse)
async def install(req: InstallRequest) -> InstallResponse:
    """Pre-install a function's dependencies without loading it.

    Idempotent: subsequent calls with the same requirements.txt are a no-op
    (cache hit). Returns whether the venv was already cached.
    """
    routine_path = Path(req.path) if req.path else Path(FUNCTIONS_DIR) / req.name
    manifest_path = routine_path / "function.yaml"

    if not manifest_path.is_file():
        raise HTTPException(
            status_code=400,
            detail=f"function.yaml not found in {routine_path}",
        )
    try:
        with manifest_path.open() as f:
            manifest = yaml.safe_load(f) or {}
    except yaml.YAMLError as err:
        raise HTTPException(status_code=400, detail=f"Invalid YAML: {err}") from err

    spec = manifest.get("spec") or {}
    entrypoint = spec.get("entrypoint", "")
    if "::" not in entrypoint:
        raise HTTPException(status_code=400, detail="spec.entrypoint missing or invalid")

    file_rel = entrypoint.split("::")[0]
    module_file = routine_path / file_rel

    reqs = module_file.parent / "requirements.txt"
    if not reqs.is_file():
        return InstallResponse(
            installed=False,
            cached=False,
            message="No requirements.txt — nothing to install.",
        )

    digest = hashlib.sha256(reqs.read_bytes()).hexdigest()[:16]
    already_cached = (loader.VENV_CACHE_DIR / digest / ".installed").exists()

    try:
        venv_path = loader.resolve_venv(module_file)
    except RoutineLoadError as err:
        raise HTTPException(status_code=500, detail=str(err)) from err

    return InstallResponse(
        installed=True,
        cached=already_cached,
        message=(
            f"Venv {'(cached) ' if already_cached else ''}ready at {venv_path}"
        ),
    )


@app.post("/load", response_model=LoadResponse)
async def load(req: LoadRequest) -> LoadResponse:
    try:
        loaded = loader.load(req.name, req.version, req.path)
    except RoutineLoadError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    return LoadResponse(
        loaded=True,
        name=loaded.name,
        version=loaded.version,
        entrypoint=loaded.entrypoint,
    )


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(req: InvokeRequest) -> InvokeResponse:
    try:
        result = await invoker.invoke(
            req.name,
            req.version,
            req.inputs,
            secrets_token=req.secretsToken,
            secrets_host_url=req.secretsHostUrl,
            direct_env=req.env or None,
        )
    except RoutineLoadError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    except InvocationError as err:
        raise HTTPException(status_code=500, detail=str(err)) from err
    return InvokeResponse(outputs=result.outputs, duration_ms=result.duration_ms)


@app.post("/unload", response_model=UnloadResponse)
async def unload(req: UnloadRequest) -> UnloadResponse:
    return UnloadResponse(unloaded=loader.unload(req.name, req.version))


def main() -> None:
    logger.info(
        "python-runtime starting: %s:%d functions=%s",
        HTTP_HOST, HTTP_PORT, FUNCTIONS_DIR,
    )
    uvicorn.run(
        app,
        host=HTTP_HOST,
        port=HTTP_PORT,
        log_level=_log_level.lower(),
        access_log=False,
    )


if __name__ == "__main__":
    main()
