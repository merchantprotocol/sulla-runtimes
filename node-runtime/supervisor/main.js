// node-runtime supervisor — Fastify over HTTP.
//
// Same protocol as python-runtime and shell-runtime: /health, /load, /invoke,
// /unload, /routines. Inputs/outputs are JSON. Only loads functions; routines
// (workflow DAGs) are orchestrated by the workflow engine.

import Fastify from 'fastify';

import { NodeLoader, NodeLoadError } from './loader.js';
import { NodeInvoker, InvocationError } from './invoker.js';

const HTTP_HOST     = process.env.SULLA_HTTP_HOST      ?? '0.0.0.0';
const HTTP_PORT     = Number(process.env.SULLA_HTTP_PORT ?? 8080);
const FUNCTIONS_DIR = process.env.SULLA_FUNCTIONS_DIR  ?? '/var/functions';
const LOG_LEVEL     = (process.env.SULLA_LOG_LEVEL     ?? 'info').toLowerCase();

const loader = new NodeLoader(FUNCTIONS_DIR);

const app = Fastify({
  logger: {
    level: LOG_LEVEL,
  },
});

// Pass the Fastify logger to the invoker so secret-handling warnings share
// the structured log stream.
const invoker = new NodeInvoker(loader, app.log);

app.get('/health', async () => ({
  status:          'ok',
  loaded_routines: Object.keys(loader.listLoaded()).sort(),
  routines_dir:    FUNCTIONS_DIR,
}));

app.get('/routines', async () => ({ routines: loader.listLoaded() }));

app.post('/load', async (req, reply) => {
  const { name, version, path: p } = req.body ?? {};
  if (!name || !version) {
    return reply.code(400).send({ detail: 'name and version are required' });
  }
  try {
    const rec = await loader.load(name, version, p);
    return {
      loaded:     true,
      name:       rec.name,
      version:    rec.version,
      entrypoint: rec.entrypoint,
    };
  } catch (err) {
    if (err instanceof NodeLoadError) {
      return reply.code(400).send({ detail: err.message });
    }
    req.log.error({ err }, 'unexpected error in /load');
    return reply.code(500).send({ detail: String(err?.message ?? err) });
  }
});

app.post('/invoke', async (req, reply) => {
  // NOTE: req.body.secretsToken is a capability token — NEVER log it.
  const { name, version, inputs, secretsToken, secretsHostUrl } = req.body ?? {};
  if (!name || !version) {
    return reply.code(400).send({ detail: 'name and version are required' });
  }
  try {
    const result = await invoker.invoke(
      name, version, inputs ?? {}, secretsToken, secretsHostUrl,
    );
    return { outputs: result.outputs, duration_ms: result.durationMs };
  } catch (err) {
    if (err instanceof NodeLoadError) {
      return reply.code(400).send({ detail: err.message });
    }
    if (err instanceof InvocationError) {
      // err.message has already been redacted inside the invoker.
      return reply.code(500).send({ detail: err.message });
    }
    // Don't pass `err` to the logger — it may contain secret material via
    // captured locals. Log only a generic marker.
    req.log.error({ errName: err?.name ?? 'Error' }, 'unexpected error in /invoke');
    return reply.code(500).send({ detail: String(err?.message ?? err) });
  }
});

app.post('/unload', async (req) => {
  const { name, version } = req.body ?? {};
  return { unloaded: loader.unload(name, version) };
});

async function main() {
  try {
    await app.listen({ host: HTTP_HOST, port: HTTP_PORT });
    app.log.info({ host: HTTP_HOST, port: HTTP_PORT, functions: FUNCTIONS_DIR }, 'node-runtime listening');
  } catch (err) {
    app.log.error({ err }, 'failed to start');
    process.exit(1);
  }
}

main();
