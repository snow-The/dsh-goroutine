/**
 * worker.mjs - the far side of a Pool worker.
 *
 * Protocol:
 *   single  in  { id, kind: 'module' | 'source' | 'noop', target, export, args }
 *           out { id, ok: true, value } | { id, ok: false, error }
 *   batch   in  { batch: true, tasks: [ ...same shape... ] }
 *           out { batchResults: [ {id, ok, value|error}, ... ] }
 *
 * Batching exists because a postMessage round trip measured 20.4 us on this host
 * (see RESEARCH-scheduling.md §3): one message carrying N tasks pays it once. The
 * tasks still run one after another, so a worker keeps its "one task at a time"
 * semantics.
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

function describe(error) {
  return {
    name: String(error?.name ?? 'Error'),
    message: String(error?.message ?? error),
    stack: typeof error?.stack === 'string' ? error.stack : undefined,
  };
}

async function runOne(task) {
  const id = task?.id;
  try {
    let value;
    if (task.kind === 'module') {
      const mod = await loadModule(task.target);
      const name = task.export ?? 'default';
      const fn = mod[name];
      if (typeof fn !== 'function') throw new TypeError('goroutine: ' + task.target + ' has no exported function "' + name + '"');
      value = await fn(...(task.args ?? []));
    } else if (task.kind === 'source') {
      value = await fromSource(task.target)(...(task.args ?? []));
    } else if (task.kind === 'noop') {
      value = undefined;
    } else {
      throw new TypeError('goroutine: unknown task kind ' + JSON.stringify(task.kind));
    }
    return { id, ok: true, value };
  } catch (error) {
    return { id, ok: false, error: describe(error) };
  }
}

port.on('message', async (msg) => {
  port.postMessage(await runOne(msg));
});
