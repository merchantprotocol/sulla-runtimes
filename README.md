# sulla-runtimes

Docker runtime images that host user-defined **Functions** for [Sulla Desktop](https://github.com/merchantprotocol/sulla-desktop). Each image runs a long-lived supervisor that loads Function code on demand and serves it over a uniform HTTP API.

This repo is intentionally decoupled from Sulla Desktop so runtime images can be released on their own cadence.

## Shared contract

Every runtime listens on **HTTP `0.0.0.0:8080`** inside the container and implements the same five endpoints:

| Method | Path        | Purpose                                           |
|--------|-------------|---------------------------------------------------|
| GET    | `/health`   | Liveness check                                    |
| GET    | `/routines` | List Functions discovered under `/var/functions` |
| POST   | `/load`     | Load (or reload) a Function by slug               |
| POST   | `/invoke`   | Invoke a loaded Function with a JSON payload      |
| POST   | `/unload`   | Unload a Function and free its resources         |

Functions are discovered from **`/var/functions`** (bind-mounted by the caller). Each Function is a directory containing a `manifest.yaml` plus a handler file; Function code stays on the host, never baked into the image.

## Runtimes

### `python-runtime/`
- **Base:** `python:3.12-slim-bookworm`
- **Purpose:** Run Python Functions. Per-function dependency isolation via `uv` into `/opt/sulla/cache/deps`.

### `shell-runtime/`
- **Base:** `alpine:3.20`
- **Purpose:** Run shell/bash Functions. Ships with `bash`, `jq`, `curl`, `git`, `openssl`, `ffmpeg`, `imagemagick` so Functions can rely on common tooling without declaring deps.

### `node-runtime/`
- **Base:** `node:20-slim`
- **Purpose:** Run Node.js Functions. Per-function `node_modules` cached under `/opt/sulla/cache/node_modules`.

## CI / publishing

`.github/workflows/build.yml` builds all three images on every push to `main`, on version tags (`v*.*.*`), and on pull requests (PRs build only — no push). Images publish to **GHCR** at:

```
ghcr.io/merchantprotocol/sulla-python-runtime
ghcr.io/merchantprotocol/sulla-shell-runtime
ghcr.io/merchantprotocol/sulla-node-runtime
```

Tag rules:
- push to `main` → `latest`
- tag `v1.2.3` → `1.2.3` + `latest`
- PR `#42` → `pr-42` (built only, not pushed)

A path filter scopes the matrix: editing `python-runtime/**` only rebuilds the Python leg. Images are built for `linux/amd64` + `linux/arm64`.

## Local build

```sh
docker build -t sulla/python-runtime:dev python-runtime
docker build -t sulla/shell-runtime:dev  shell-runtime
docker build -t sulla/node-runtime:dev   node-runtime
```

Smoke test after starting a container with `-p 8080:8080 -v ~/sulla/functions:/var/functions`:

```sh
./python-runtime/tests/invoke.sh
```

## License

MIT — see [LICENSE](LICENSE).
