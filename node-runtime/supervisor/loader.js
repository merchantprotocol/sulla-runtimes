// Node routine/function loader.
//
// Reads routine.yaml / function.yaml, dynamically imports the entrypoint,
// and caches the resolved handler. Cache-busts on reload via a timestamp
// query param on the import URL (Node's ESM loader keys by URL).
//
// MVP limitations mirrored from python-runtime:
//   - no per-routine npm install (pre-install deps in base image or commit
//     node_modules for now; content-hash caching is follow-up work)
//   - no permission enforcement
//   - no filesystem watcher (reload is explicit via /load)

import fs from 'node:fs/promises';
import path from 'node:path';
import url from 'node:url';
import yaml from 'js-yaml';

export class NodeLoadError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'NodeLoadError';
  }
}

export class NodeLoader {
  constructor(routinesDir) {
    this.routinesDir = routinesDir;
    this.loaded = new Map(); // key = `${name}@${version}` → loaded record
  }

  _key(name, version) { return `${name}@${version}`; }

  async load(name, version, unitPath) {
    const unitDir = unitPath ?? path.join(this.routinesDir, name);
    try {
      const stat = await fs.stat(unitDir);
      if (!stat.isDirectory()) throw new Error('not a directory');
    } catch (err) {
      throw new NodeLoadError(`Routine directory not found: ${unitDir}`);
    }

    const manifestPath = path.join(unitDir, 'function.yaml');
    try {
      await fs.access(manifestPath);
    } catch {
      throw new NodeLoadError(`function.yaml not found in ${unitDir}`);
    }

    let manifest;
    try {
      const raw = await fs.readFile(manifestPath, 'utf-8');
      manifest = yaml.load(raw) ?? {};
    } catch (err) {
      throw new NodeLoadError(`Invalid YAML in ${manifestPath}: ${err.message}`);
    }

    const kind = manifest.kind;
    if (kind !== 'Function') {
      throw new NodeLoadError(
        `node-runtime only loads functions; got kind: ${JSON.stringify(kind)}. ` +
        `Routines are orchestrated by the workflow engine, not runtimes.`,
      );
    }

    const spec = manifest.spec ?? {};
    if (spec.runtime !== 'node') {
      throw new NodeLoadError(
        `Wrong runtime for node-runtime: got ${JSON.stringify(spec.runtime)}`,
      );
    }

    const entrypoint = spec.entrypoint;
    if (!entrypoint || !entrypoint.includes('::')) {
      throw new NodeLoadError(
        'spec.entrypoint must be in format "relative/path.js::exportName"',
      );
    }
    const [fileRel, exportName] = entrypoint.split('::');
    const fullFile = path.join(unitDir, fileRel);
    try {
      await fs.access(fullFile);
    } catch {
      throw new NodeLoadError(`Entrypoint file not found: ${fullFile}`);
    }

    // Cache-bust the ESM loader by appending a unique query param.
    // Without this, a second load() would return the previously cached module.
    const importUrl = `${url.pathToFileURL(fullFile).href}?v=${Date.now()}`;

    let mod;
    try {
      mod = await import(importUrl);
    } catch (err) {
      throw new NodeLoadError(
        `Failed to import ${fullFile}: ${err.name}: ${err.message}`,
      );
    }

    // Accept both `export function handler()` and `export default { handler }`.
    const handler = mod[exportName] ?? mod.default?.[exportName];
    if (typeof handler !== 'function') {
      throw new NodeLoadError(
        `Export ${JSON.stringify(exportName)} not found or not a function in ${fullFile}`,
      );
    }

    const timeoutSec = parseTimeoutSec(spec.timeout, 60);

    const record = {
      name,
      version,
      path: unitDir,
      entrypoint,
      handler,
      manifest,
      kind,
      timeoutSec,
    };
    this.loaded.set(this._key(name, version), record);
    return record;
  }

  unload(name, version) {
    const key = this._key(name, version);
    if (!this.loaded.has(key)) return false;
    this.loaded.delete(key);
    // Note: Node's ESM cache holds the old module until the process exits.
    // That's fine — we bump the ?v= query on next load to get a fresh copy.
    return true;
  }

  get(name, version) {
    return this.loaded.get(this._key(name, version));
  }

  listLoaded() {
    const out = {};
    for (const [key, r] of this.loaded) {
      out[key] = {
        kind:       r.kind,
        name:       r.name,
        version:    r.version,
        path:       r.path,
        entrypoint: r.entrypoint,
        timeoutSec: r.timeoutSec,
      };
    }
    return out;
  }
}

function parseTimeoutSec(raw, fallback) {
  if (raw == null) return fallback;
  if (typeof raw === 'number') return raw;
  const s = String(raw).trim().toLowerCase();
  const units = { ms: 0.001, s: 1, m: 60, h: 3600 };
  for (const [suffix, mult] of Object.entries(units)) {
    if (s.endsWith(suffix)) {
      const n = parseFloat(s.slice(0, -suffix.length));
      if (!Number.isNaN(n)) return n * mult;
      break;
    }
  }
  return fallback;
}
