FROM node:20-slim

ENV NODE_ENV=production

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /opt/sulla

COPY package.json ./
RUN npm install --omit=dev

COPY supervisor/ ./supervisor/

ENV SULLA_HTTP_HOST=0.0.0.0 \
    SULLA_HTTP_PORT=8080 \
    SULLA_FUNCTIONS_DIR=/var/functions \
    SULLA_DEPS_CACHE=/opt/sulla/cache/node_modules \
    SULLA_LOG_LEVEL=info

RUN mkdir -p /var/functions /opt/sulla/cache/node_modules

EXPOSE 8080
VOLUME ["/var/functions", "/opt/sulla/cache/node_modules"]

HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -fsS http://localhost:8080/health || exit 1

CMD ["node", "supervisor/main.js"]
