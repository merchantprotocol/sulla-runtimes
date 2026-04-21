"""Pydantic schemas for the python-runtime supervisor's HTTP API."""

from typing import Any

from pydantic import BaseModel, Field


class LoadRequest(BaseModel):
    name: str
    version: str
    path: str | None = Field(
        default=None,
        description=(
            "Absolute path to the routine directory inside the container. "
            "When omitted, resolves to SULLA_ROUTINES_DIR/<name>/."
        ),
    )


class LoadResponse(BaseModel):
    loaded: bool
    name: str
    version: str
    entrypoint: str


class InvokeRequest(BaseModel):
    name: str
    version: str
    inputs: dict[str, Any] = Field(default_factory=dict)
    # Capability-token-scoped secret fetching. The runtime pulls each declared
    # env var just-in-time from the host using `secretsToken` against
    # `secretsHostUrl`. No plaintext ever crosses the invoke boundary.
    # Both are optional — handlers that declare no integrations work without
    # them.
    secretsToken:   str | None = None
    secretsHostUrl: str | None = None


class InvokeResponse(BaseModel):
    outputs: dict[str, Any]
    duration_ms: int


class UnloadRequest(BaseModel):
    name: str
    version: str


class UnloadResponse(BaseModel):
    unloaded: bool


class HealthResponse(BaseModel):
    status: str
    loaded_routines: list[str]
    routines_dir: str


class ListRoutinesResponse(BaseModel):
    routines: dict[str, dict[str, Any]]
