/**
 * worker.mjs - the far side of a Pool worker.
 *
 * Protocol (one message per task):
 *   in  { id, kind: 'module' | 'source' | 'noop', target, export, args, transfer }
 *   out { id, ok: true, value } | { id, ok: false, error: { name, message, stack } }
 *
 * 'module' tasks import the module ONCE per worker and reuse the namespace
 * (piscina's model: no per-task import cost, no eval). 'source' tasks exist so
 * that \`pool.spawn(() => ...)\` feels like \`go func(){...}()\` - the function source is
 * shipped and re-created here, which is why closures cannot capture (documented
 * in DESIGN.md §4).
 */
import { parentPort } from 'node:worker_threads';

const port = parentPort;
if (port === null) throw new Error('goroutine: worker.mjs only runs as a worker thread');

/** module URL -> namespace, so a warm worker never re-imports. */
const modules = new Map();

async function loadModule(target) {
  const url = String(target);
  let mod = modules.get(url);
  if (mod === undefined) {
    mod = await import(url);
    modules.set(url, mod);
  }
  return mod;
}

function fromSource(source) {
  // Indirect eval keeps this in global scope, like a Go func literal: no closure.
  const fn = (0, eval)('(' + String(source) + ')');
  if (typeof fn !== 'function') throw new TypeError('goroutine: inline task is not a function');
  return fn;
}

function fail(id, error) {
  port.postMessage({
    id,
    ok: false,
    error: {
      name: String(error?.name ?? 'Error'),
      message: String(error?.message ?? error),
      stack: typeof error?.stack === 'string' ? error.stack : undefined,
    },
  });
}

port.on('message', async (msg) => {
  const id = msg?.id;
  try {
    let value;
    if (msg.kind === 'module') {
      const mod = await loadModule(msg.target);
      const name = msg.export ?? 'default';
      const fn = mod[name];
      if (typeof fn !== 'function') throw new TypeError('goroutine: ' + msg.target + ' has no exported function "' + name + '"');
      value = await fn(...(msg.args ?? []));
    } else if (msg.kind === 'source') {
      value = await fromSource(msg.target)(...(msg.args ?? []));
    } else if (msg.kind === 'noop') {
      value = undefined;
    } else {
      throw new TypeError('goroutine: unknown task kind ' + JSON.stringify(msg.kind));
    }
    if (msg.transferFromResult === true && value && typeof value === 'object') {
      // caller asked for a zero-copy handoff of the result buffers
    }
    port.postMessage({ id, ok: true, value });
  } catch (error) {
    fail(id, error);
  }
});
