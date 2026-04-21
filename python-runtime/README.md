# python-runtime

Long-lived Docker container that loads Sulla **routines** (and later, custom
functions) written in Python, then dispatches workflow invocations to their
handlers over a Unix socket.

One supervisor per container. Many routines loaded into isolated `importlib`
namespaces inside that supervisor. Function-call-fast after first load.

## MVP scope (this revision)

- ✅ Load a routine by name+version from `/var/routines/<name>/`
- ✅ Invoke a loaded routine's handler (sync or async) with JSON inputs
- ✅ Unload a routine
- ✅ Health + list endpoints
- ⏳ Per-routine venv caching via `uv` (stub — deps pre-installed in base image)
- ⏳ File-watcher hot reload (stub — reload is explicit via `/load`)
- ⏳ Permission enforcement (network filter, env injection, fs sandbox)
- ⏳ Signature verification of published routines
- ⏳ gRPC option (currently HTTP-over-UDS only)

## Build

```bash
cd sulla-desktop/runtimes/python-runtime
docker build -t sulla/python-runtime:dev .
```

## Run

Bind-mount the routines dir and a host directory for the Unix socket:

```bash
mkdir -p /tmp/sulla-sock
docker run --rm \
  --name sulla-python-runtime \
  -v ~/sulla/routines:/var/routines:ro \
  -v /tmp/sulla-sock:/run/sulla \
  sulla/python-runtime:dev
```

The supervisor listens on `/tmp/sulla-sock/python-runtime.sock` on the host.

## Protocol

HTTP over Unix domain socket. All endpoints return JSON.

| Endpoint    | Method | Body                                             | Response                                       |
|-------------|--------|--------------------------------------------------|------------------------------------------------|
| `/health`   | GET    |                                                  | `{ status, loaded_routines[], routines_dir }`  |
| `/routines` | GET    |                                                  | `{ routines: { "name@ver": {...} } }`          |
| `/load`     | POST   | `{ name, version, path? }`                       | `{ loaded, name, version, entrypoint }`        |
| `/invoke`   | POST   | `{ name, version, inputs }`                      | `{ outputs, duration_ms }`                     |
| `/unload`   | POST   | `{ name, version }`                              | `{ unloaded }`                                 |

Errors map to HTTP status:
- 400 — routine load error (manifest missing, wrong runtime, import failed)
- 500 — handler raised during invocation

## Test

```bash
./tests/invoke.sh
```

Runs the `transform-json` sample routine end-to-end against the running
container. Expects the container to be reachable via the socket at
`/tmp/sulla-sock/python-runtime.sock`.

## Environment

| Variable              | Default                                  | Purpose                                |
|-----------------------|------------------------------------------|----------------------------------------|
| `SULLA_SOCKET`        | `/run/sulla/python-runtime.sock`         | Unix socket path (inside container).   |
| `SULLA_ROUTINES_DIR`  | `/var/routines`                          | Routines bind-mount path.              |
| `SULLA_LOG_LEVEL`     | `INFO`                                   | `DEBUG` / `INFO` / `WARNING` / `ERROR` |
