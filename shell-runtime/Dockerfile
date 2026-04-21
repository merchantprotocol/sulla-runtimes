FROM alpine:3.20

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Core shell + networking + JSON + common media/crypto tools. Functions can
# rely on these being present without declaring them.
RUN apk add --no-cache \
      bash \
      coreutils \
      curl \
      ca-certificates \
      git \
      jq \
      openssl \
      ffmpeg \
      imagemagick \
      python3 \
      py3-pip

# Supervisor deps — shell-runtime's invoker is just subprocess.run, so this
# layer is small.
RUN pip install --break-system-packages --no-cache-dir \
      fastapi==0.115.6 \
      uvicorn[standard]==0.32.1 \
      pyyaml==6.0.2 \
      pydantic==2.10.4

WORKDIR /opt/sulla
COPY supervisor/ ./supervisor/

ENV SULLA_HTTP_HOST=0.0.0.0 \
    SULLA_HTTP_PORT=8080 \
    SULLA_FUNCTIONS_DIR=/var/functions \
    SULLA_LOG_LEVEL=INFO \
    PYTHONPATH=/opt/sulla

RUN mkdir -p /var/functions

EXPOSE 8080
VOLUME ["/var/functions"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:8080/health || exit 1

CMD ["python3", "-m", "supervisor.main"]
