# shell-runtime

Long-lived Alpine container that loads Sulla **shell routines and functions**
and dispatches invocations to bash subprocesses. Baseline tools: `bash`,
`coreutils`, `curl`, `git`, `jq`, `openssl`, `ffmpeg`, `imagemagick`.

## Scope (this revision)

- ✅ Same five-endpoint protocol as python-runtime (`/health`, `/load`,
  `/invoke`, `/unload`, `/routines`)
- ✅ Loads either `routine.yaml` or `function.yaml` manifests
- ✅ Inputs piped to handler on stdin as JSON; outputs read from stdout as JSON
- ✅ Timeout enforced by `spec.timeout` (default 300s)
- ⏳ Per-routine apk package install (shell routines declaring extra tools)
- ⏳ Permission enforcement (network filter, filesystem sandbox)

## Contract with shell entrypoints

- Read inputs JSON on stdin: `inputs_json="$(cat)"`
- Emit a single JSON object on stdout
- Non-zero exit *with valid JSON on stdout* → treated as success (allows
  routines like `shell-command` to report `exit_code: 1` in their outputs)
- Non-zero exit *without valid JSON* → treated as failure, HTTP 500

## Build

```bash
cd sulla-desktop/runtimes/shell-runtime
docker build -t sulla/shell-runtime:dev .
```

## Run

```bash
mkdir -p /tmp/sulla-shell-sock
docker run --rm \
  --name sulla-shell-runtime \
  -v ~/sulla/routines:/var/routines:ro \
  -v /tmp/sulla-shell-sock:/run/sulla \
  sulla/shell-runtime:dev
```

## Test

```bash
./tests/invoke.sh
```

Runs the `shell-command` sample routine end-to-end.

## Environment

| Variable             | Default                             | Purpose                           |
|----------------------|-------------------------------------|-----------------------------------|
| `SULLA_SOCKET`       | `/run/sulla/shell-runtime.sock`     | Unix socket path inside container |
| `SULLA_ROUTINES_DIR` | `/var/routines`                     | Routines bind-mount path          |
| `SULLA_LOG_LEVEL`    | `INFO`                              | Log verbosity                     |
