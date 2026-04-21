FROM python:3.12-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Supervisor deps + uv for per-function dependency resolution.
RUN pip install \
      fastapi==0.115.6 \
      uvicorn[standard]==0.32.1 \
      pyyaml==6.0.2 \
      pydantic==2.10.4 \
      uv==0.5.13

WORKDIR /opt/sulla

COPY supervisor/ ./supervisor/
COPY sdk/ ./sdk/

ENV SULLA_HTTP_HOST=0.0.0.0 \
    SULLA_HTTP_PORT=8080 \
    SULLA_FUNCTIONS_DIR=/var/functions \
    SULLA_DEPS_CACHE=/opt/sulla/cache/deps \
    SULLA_LOG_LEVEL=INFO \
    PYTHONPATH=/opt/sulla

# Functions bind-mount from the host's ~/sulla/functions/ via the Lima VM.
# Deps cache volume persists installed requirements across container restarts.
RUN mkdir -p /var/functions /opt/sulla/cache/deps

EXPOSE 8080
VOLUME ["/var/functions", "/opt/sulla/cache/deps"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:8080/health || exit 1

CMD ["python", "-m", "supervisor.main"]
