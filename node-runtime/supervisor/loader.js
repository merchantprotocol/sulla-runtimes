// Node routine/function loader.
//
// Reads function.yaml, installs npm dependencies if package.json is present,
// then dynamically imports the entrypoint and caches the resolved handler.
// Cache-busts on reload via a timestamp query param on the import URL.
//
// Dependency install uses npm into the function's own directory so Node's
// native ESM resolution picks up the packages without any NODE_PATH tricks.
// A marker file (node_modules/.sulla-installed) stores the package.json hash
// to skip reinstall when the function is reloaded with unchanged deps.

import crypto from 'node:crypto';
import { execFile } from 'node:child_process';
import fs from 'node:fs/promises';
import path from 'node:path';
import url from 'node:url';
import { promisify } from 'node:util';
import yaml from 'js-yaml';

const execFileAsync = promisify(execFile);

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

  async _installDeps(unitDir) {
    const pkgJsonPath = path.join(unitDir, 'package.json');
    try {
      await fs.access(pkgJsonPath);
    } catch {
      return; // No package.json — nothing to install.
    }

    let pkgContent;
    try {
      pkgContent = await fs.readFile(pkgJsonPath, 'utf-8');
    } catch (err) {
      throw new NodeLoadError(`Failed to read package.json: ${err.message}`);
    }

    const hash = crypto.createHash('sha256').update(pkgContent).digest('hex').slice(0, 16);
    const markerPath = path.join(unitDir, 'node_modules', '.sulla-installed');

    let existingHash = '';
    try {
      existingHash = (await fs.readFile(markerPath, 'utf-8')).trim();
    } catch {
      // Marker missing → needs install.
    }

    if (existingHash === hash) {
      return; // Already installed with this exact package.json.
    }

    try {
      await execFileAsync(
        'npm',
        ['install', '--prefer-offline', '--no-audit', '--no-fund', '--no-save'],
        { cwd: unitDir, timeout: 120_000 },
      );
    } catch (err) {
      throw new NodeLoadError(
        `npm install failed in ${unitDir}: ${err.stderr ?? err.message}`,
      );
    }

    // Write marker so subsequent loads skip reinstall.
    await fs.mkdir(path.join(unitDir, 'node_modules'), { recursive: true });
    await fs.writeFile(markerPath, hash, 'utf-8');
  }

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

    // Install deps before importing so the ESM resolution finds node_modules.
    await this._installDeps(unitDir);

    // Cache-bust the ESM loader by appending a unique query param.
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
