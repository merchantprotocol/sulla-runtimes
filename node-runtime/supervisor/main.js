// node-runtime supervisor — Fastify over HTTP.
//
// Same protocol as python-runtime and shell-runtime: /health, /routines,
// /install, /load, /invoke, /unload. Only loads functions; routines
// (workflow DAGs) are orchestrated by the workflow engine.

import fs from 'node:fs/promises';
import path from 'node:path';
import Fastify from 'fastify';
import yaml from 'js-yaml';

import { NodeLoader, NodeLoadError } from './loader.js';
import { NodeInvoker, InvocationError } from './invoker.js';

const HTTP_HOST     = process.env.SULLA_HTTP_HOST      ?? '0.0.0.0';
const HTTP_PORT     = Number(process.env.SULLA_HTTP_PORT ?? 8080);
const FUNCTIONS_DIR = process.env.SULLA_FUNCTIONS_DIR  ?? '/var/functions';
const LOG_LEVEL     = (process.env.SULLA_LOG_LEVEL     ?? 'info').toLowerCase();

const loader = new NodeLoader(FUNCTIONS_DIR);

const app = Fastify({ logger: { level: LOG_LEVEL } });
const invoker = new NodeInvoker(loader, app.log);

app.get('/health', async () => ({
  status:          'ok',
  loaded_routines: Object.keys(loader.listLoaded()).sort(),
  routines_dir:    FUNCTIONS_DIR,
}));

app.get('/routines', async () => ({ routines: loader.listLoaded() }));

app.post('/install', async (req, reply) => {
  const { name, version, path: p } = req.body ?? {};
  if (!name || !version) {
    return reply.code(400).send({ detail: 'name and version are required' });
  }

  const unitDir = p ?? path.join(FUNCTIONS_DIR, name);
  const manifestPath = path.join(unitDir, 'function.yaml');

  let manifest;
  try {
    const raw = await fs.readFile(manifestPath, 'utf-8');
    manifest = yaml.load(raw) ?? {};
  } catch (err) {
    return reply.code(400).send({ detail: `function.yaml not found or invalid in ${unitDir}` });
  }

  const pkgJsonPath = path.join(unitDir, 'package.json');
  let hasPkgJson = false;
  try {
    await fs.access(pkgJsonPath);
    hasPkgJson = true;
  } catch { /* no package.json */ }

  if (!hasPkgJson) {
    return { installed: false, cached: false, message: 'No package.json — nothing to install.' };
  }

  try {
    await loader._installDeps(unitDir);
  } catch (err) {
    if (err instanceof NodeLoadError) {
      return reply.code(500).send({ detail: err.message });
    }
    return reply.code(500).send({ detail: String(err?.message ?? err) });
  }

  return { installed: true, cached: false, message: `Dependencies installed in ${unitDir}/node_modules/` };
});

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
  const { name, version, inputs, secretsToken, secretsHostUrl, env } = req.body ?? {};
  if (!name || !version) {
    return reply.code(400).send({ detail: 'name and version are required' });
  }
  try {
    const result = await invoker.invoke(
      name, version, inputs ?? {}, secretsToken, secretsHostUrl, env ?? null,
    );
    return { outputs: result.outputs, duration_ms: result.durationMs };
  } catch (err) {
    if (err instanceof NodeLoadError) {
      return reply.code(400).send({ detail: err.message });
    }
    if (err instanceof InvocationError) {
      return reply.code(500).send({ detail: err.message });
    }
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
