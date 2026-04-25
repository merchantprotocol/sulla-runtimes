"""Pydantic schemas for the shell-runtime supervisor's HTTP API.

Mirrors python-runtime.supervisor.schemas — kept separate per-runtime so each
container can evolve its protocol independently.
"""

from typing import Any

from pydantic import BaseModel, Field


class LoadRequest(BaseModel):
    name: str
    version: str
    path: str | None = Field(
        default=None,
        description="Absolute path to the routine/function directory inside the container.",
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
    # Capability-token-scoped secret fetching. The runtime fetches each
    # declared env var just-in-time from the host, passes them to the bash
    # subprocess via env=, and never exports to the supervisor's own env.
    # Both optional — handlers that declare no integrations work without them.
    secretsToken:   str | None = None
    secretsHostUrl: str | None = None
    # Direct env injection: caller resolves secrets and passes values here.
    # Values are merged into the subprocess env — supervisor's own os.environ
    # is never modified. Repr is suppressed so values are not logged.
    env: dict[str, str] = Field(default_factory=dict, repr=False)


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
