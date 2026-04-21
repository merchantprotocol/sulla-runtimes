// Node invoker — calls loaded handlers with inputs, times, enforces timeout,
// validates output shape.
//
// Secrets never cross the /invoke body. The caller provides a capability
// `secretsToken` + `secretsHostUrl`; we fetch every declared env var
// just-in-time from the host, set them on process.env for the duration of the
// handler, delete after, and invalidate the token in `finally`. Node 20+ has
// global `fetch`, so no deps added.

const SECRETS_FETCH_TIMEOUT_MS = 5000;

export class InvocationError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'InvocationError';
  }
}

export class SecretsFetchError extends Error {
  constructor(msg) {
    super(msg);
    this.name = 'SecretsFetchError';
  }
}

/**
 * Replace any occurrence of a secret VALUE in `text` with '***'. Applies
 * longest-first so overlapping prefixes don't leak.
 */
function redact(text, secrets) {
  if (!text || !secrets || secrets.length === 0) return text;
  let out = String(text);
  const ordered = secrets
    .filter((v) => typeof v === 'string' && v.length > 0)
    .sort((a, b) => b.length - a.length);
  for (const s of ordered) {
    if (s && out.includes(s)) {
      out = out.split(s).join('***');
    }
  }
  return out;
}

/**
 * Union the KEYS of every `spec.integrations[].env` object in the manifest.
 * Warns (one line, no slug) if the same env var is declared by multiple
 * integrations; last-wins.
 */
function collectEnvVarNames(manifest, logger) {
  const spec = manifest?.spec ?? {};
  const integrations = Array.isArray(spec.integrations) ? spec.integrations : [];
  const seen = new Set();
  const duplicates = new Set();
  const ordered = [];
  for (const entry of integrations) {
    if (!entry || typeof entry !== 'object') continue;
    const envMap = entry.env;
    if (!envMap || typeof envMap !== 'object') continue;
    for (const key of Object.keys(envMap)) {
      if (typeof key !== 'string' || key.length === 0) continue;
      if (seen.has(key)) {
        duplicates.add(key);
      } else {
        seen.add(key);
        ordered.push(key);
      }
    }
  }
  if (duplicates.size > 0 && logger?.warn) {
    logger.warn(
      { duplicates: Array.from(duplicates).sort() },
      'Duplicate env var names declared across integrations (last-wins)',
    );
  }
  return ordered;
}

async function fetchWithTimeout(url, init, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    return await fetch(url, { ...init, signal: controller.signal });
  } finally {
    clearTimeout(timer);
  }
}

async function fetchOneSecret(hostUrl, token, key) {
  const base = hostUrl.replace(/\/+$/, '');
  let resp;
  try {
    resp = await fetchWithTimeout(
      `${base}/secrets/fetch`,
      {
        method:  'POST',
        headers: { 'content-type': 'application/json' },
        body:    JSON.stringify({ token, key }),
      },
      SECRETS_FETCH_TIMEOUT_MS,
    );
  } catch (err) {
    // Transport-level failure (DNS, refused, timeout/abort). Type only.
    throw new SecretsFetchError(
      `fetch transport failure for env var ${JSON.stringify(key)}: ${err?.name ?? 'Error'}`,
    );
  }
  if (!resp.ok) {
    throw new SecretsFetchError(
      `fetch denied for env var ${JSON.stringify(key)} (status ${resp.status})`,
    );
  }
  let payload;
  try {
    payload = await resp.json();
  } catch {
    throw new SecretsFetchError(
      `fetch returned malformed JSON for env var ${JSON.stringify(key)}`,
    );
  }
  if (!payload || typeof payload !== 'object' || typeof payload.value !== 'string') {
    throw new SecretsFetchError(
      `fetch returned no value for env var ${JSON.stringify(key)}`,
    );
  }
  return payload.value;
}

async function invalidateToken(hostUrl, token, logger) {
  const base = hostUrl.replace(/\/+$/, '');
  try {
    await fetchWithTimeout(
      `${base}/secrets/invalidate`,
      {
        method:  'POST',
        headers: { 'content-type': 'application/json' },
        body:    JSON.stringify({ token }),
      },
      SECRETS_FETCH_TIMEOUT_MS,
    );
  } catch (err) {
    // Best-effort. Log name only, never the token.
    logger?.warn?.(
      { errName: err?.name ?? 'Error' },
      'secrets/invalidate best-effort failed',
    );
  }
}

export class NodeInvoker {
  constructor(loader, logger) {
    this.loader = loader;
    // Optional Fastify-style logger; falls back to console-free no-ops.
    this.logger = logger ?? null;
  }

  /**
   * Invoke a loaded handler.
   *
   * secretsToken / secretsHostUrl: capability-token-scoped secret fetching.
   *   When present AND the function declares at least one integration env
   *   var, each value is fetched just-in-time from the host, placed on
   *   process.env for the duration of the handler call, and deleted in
   *   finally. The token is invalidated in finally, best-effort.
   *
   * TODO(isolation): process.env is process-global. Concurrent invocations
   * of the same function with different env values will race. Real isolation
   * requires worker_threads or sub-interpreters per call — tracked.
   */
  async invoke(name, version, inputs, secretsToken, secretsHostUrl) {
    let record = this.loader.get(name, version);
    if (!record) {
      record = await this.loader.load(name, version);
    }

    const start = process.hrtime.bigint();
    const timeoutMs = record.timeoutSec * 1000;

    const envVarNames = collectEnvVarNames(record.manifest, this.logger);
    const needsSecrets = Boolean(secretsToken) && envVarNames.length > 0;

    const fetchedEnv = new Map();
    let secretValues = [];
    const previousValues = new Map();
    const setKeys = [];

    try {
      if (needsSecrets) {
        if (!secretsHostUrl) {
          throw new InvocationError('secretsToken provided without secretsHostUrl');
        }
        for (const key of envVarNames) {
          let value;
          try {
            value = await fetchOneSecret(secretsHostUrl, secretsToken, key);
          } catch (err) {
            if (err instanceof SecretsFetchError) {
              throw new InvocationError(err.message);
            }
            throw err;
          }
          fetchedEnv.set(key, value);
        }
        secretValues = Array.from(fetchedEnv.values()).filter(
          (v) => typeof v === 'string' && v.length > 0,
        );

        for (const [k, v] of fetchedEnv) {
          if (Object.prototype.hasOwnProperty.call(process.env, k)) {
            previousValues.set(k, process.env[k]);
          }
          process.env[k] = v;
          setKeys.push(k);
        }
      }

      const handlerPromise = Promise.resolve().then(() => record.handler(inputs ?? {}));

      let timeoutHandle;
      const timeoutPromise = new Promise((_, reject) => {
        timeoutHandle = setTimeout(
          () => reject(new InvocationError(`Handler timed out after ${record.timeoutSec}s`)),
          timeoutMs,
        );
      });

      let result;
      try {
        result = await Promise.race([handlerPromise, timeoutPromise]);
      } catch (err) {
        if (err instanceof InvocationError) {
          throw new InvocationError(redact(err.message, secretValues));
        }
        const errName = err?.name ?? 'Error';
        const msg = err?.message ?? String(err);
        throw new InvocationError(redact(`${errName}: ${msg}`, secretValues));
      } finally {
        clearTimeout(timeoutHandle);
      }

      if (result == null || typeof result !== 'object' || Array.isArray(result)) {
        throw new InvocationError(
          `Handler returned ${typeof result}, expected a plain object`,
        );
      }

      const durationMs = Number((process.hrtime.bigint() - start) / 1_000_000n);
      return { outputs: result, durationMs };
    } finally {
      // Restore / remove per-invocation env keys.
      for (const k of setKeys) {
        if (previousValues.has(k)) {
          process.env[k] = previousValues.get(k);
        } else {
          delete process.env[k];
        }
      }
      // Zero local references.
      setKeys.length = 0;
      previousValues.clear();
      fetchedEnv.clear();
      secretValues.length = 0;

      if (needsSecrets && secretsToken && secretsHostUrl) {
        await invalidateToken(secretsHostUrl, secretsToken, this.logger);
      }
    }
  }
}
