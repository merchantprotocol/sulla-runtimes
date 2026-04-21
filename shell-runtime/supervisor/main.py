"""shell-runtime supervisor — FastAPI app on an HTTP port.

Same protocol as python-runtime. Loads shell **functions** and dispatches
invocations to bash subprocesses. Routines (workflow DAGs) are orchestrated
by the workflow engine, not here.
"""

from __future__ import annotations

import logging
import os

import uvicorn
from fastapi import FastAPI, HTTPException

from supervisor.invoker import InvocationError, ShellInvoker
from supervisor.loader import ShellLoader, ShellLoadError
from supervisor.schemas import (
    HealthResponse,
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
logger = logging.getLogger("sulla.shell-runtime")

HTTP_HOST     = os.environ.get("SULLA_HTTP_HOST",     "0.0.0.0")
HTTP_PORT     = int(os.environ.get("SULLA_HTTP_PORT", "8080"))
FUNCTIONS_DIR = os.environ.get("SULLA_FUNCTIONS_DIR", "/var/functions")

loader  = ShellLoader(routines_dir=FUNCTIONS_DIR)
invoker = ShellInvoker(loader=loader)

app = FastAPI(title="sulla.shell-runtime", version="0.1.0")


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


@app.post("/load", response_model=LoadResponse)
async def load(req: LoadRequest) -> LoadResponse:
    try:
        loaded = loader.load(req.name, req.version, req.path)
    except ShellLoadError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    return LoadResponse(
        loaded=True,
        name=loaded.name,
        version=loaded.version,
        entrypoint=loaded.entrypoint,
    )


@app.post("/invoke", response_model=InvokeResponse)
async def invoke(req: InvokeRequest) -> InvokeResponse:
    # NOTE: req.secretsToken is a capability token — NEVER log it.
    try:
        result = await invoker.invoke(
            req.name,
            req.version,
            req.inputs,
            secrets_token=req.secretsToken,
            secrets_host_url=req.secretsHostUrl,
        )
    except ShellLoadError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    except InvocationError as err:
        # Message has already been redacted inside the invoker.
        raise HTTPException(status_code=500, detail=str(err)) from err
    return InvokeResponse(outputs=result.outputs, duration_ms=result.duration_ms)


@app.post("/unload", response_model=UnloadResponse)
async def unload(req: UnloadRequest) -> UnloadResponse:
    return UnloadResponse(unloaded=loader.unload(req.name, req.version))


def main() -> None:
    logger.info(
        "shell-runtime starting: %s:%d functions=%s",
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
