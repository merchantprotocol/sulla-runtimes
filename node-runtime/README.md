# node-runtime

Long-lived Node 20 container that loads Sulla routines and functions written
in JavaScript/TypeScript and dispatches invocations to their handlers over a
Unix socket.

## Scope (this revision)

- ✅ Same five-endpoint protocol as python-runtime and shell-runtime
- ✅ Dynamic ESM import of routine/function entrypoints
- ✅ Cache-busting reload on `/load` (URL query-param)
- ✅ Timeout enforcement per `spec.timeout`
- ✅ Sync + async handlers both supported (Promise.resolve normalizes)
- ⏳ Per-routine `npm install` with content-hash caching
- ⏳ TypeScript transform at load time (currently only plain `.js` / `.mjs` entrypoints)
- ⏳ Permission enforcement (network filter, env injection, fs sandbox)

## Routine / function layout

Node routines and functions must include a `package.json` with `"type": "module"`
so the supervisor's dynamic `import()` resolves ESM correctly:

```json
{
  "name": "greet-node",
  "version": "0.0.0",
  "type": "module",
  "main": "main.js"
}
```

This is also where third-party deps get declared — once the per-routine
`npm install` cache lands, each routine's `package.json` + lockfile will
feed the content-hashed cache.

## Entrypoint format

`spec.entrypoint` in routine.yaml / function.yaml:

```yaml
spec:
  runtime: node
  entrypoint: main.js::handler
```

The handler is a function that accepts `inputs` (plain object) and returns a
plain object (sync or async):

```js
export function handler(inputs) {
  return { greeting: `Hello, ${inputs.name ?? 'world'}!` };
}
```

Or async:

```js
export async function handler(inputs) {
  const resp = await fetch(inputs.url);
  return { status: resp.status, body: await resp.text() };
}
```

## Build

```bash
cd sulla-desktop/runtimes/node-runtime
docker build -t sulla/node-runtime:dev .
```

## Run

```bash
mkdir -p /tmp/sulla-node-sock
docker run --rm \
  --name sulla-node-runtime \
  -v ~/sulla/routines:/var/routines:ro \
  -v /tmp/sulla-node-sock:/run/sulla \
  sulla/node-runtime:dev
```

## Environment

| Variable             | Default                            | Purpose                           |
|----------------------|------------------------------------|-----------------------------------|
| `SULLA_SOCKET`       | `/run/sulla/node-runtime.sock`     | Unix socket path inside container |
| `SULLA_ROUTINES_DIR` | `/var/routines`                    | Routines bind-mount path          |
| `SULLA_DEPS_CACHE`   | `/opt/sulla/cache/node_modules`    | Reserved for future dep caching   |
| `SULLA_LOG_LEVEL`    | `info`                             | pino log level                    |
