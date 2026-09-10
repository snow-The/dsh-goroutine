/**
 * group.js - WaitGroup, errgroup-style Group, and the \`go\` / \`parallel\` helpers.
 *
 * WaitGroup follows ref/go/src/sync/waitgroup.go (counter + waiter count, cascade
 * wakeups) including the modern \`WaitGroup.Go\` entry point. \`Group\` follows
 * golang.org/x/sync/errgroup: the first error cancels every sibling through one
 * AbortController, which is JS's context.Context.
 */

/** Counting semaphore used to wait for a set of tasks. */
export class WaitGroup {
  #count = 0;
  #waiters = [];

  get count() { return this.#count; }

  add(delta = 1) {
    if (!Number.isFinite(delta)) throw new TypeError('goroutine: WaitGroup.add wants a number');
    this.#count += delta;
    if (this.#count < 0) throw new Error('goroutine: WaitGroup counter went negative');
    if (this.#count === 0) this.#release();
    return this;
  }

  done() { return this.add(-1); }

  /** Go 1.25's WaitGroup.Go: run fn, track it, and ignore its result. */
  go(fn) {
    this.add(1);
    Promise.resolve().then(fn).then(
      () => this.done(),
      (error) => { this.done(); queueMicrotask(() => { throw error; }); },
    );
    return this;
  }

  wait() {
    if (this.#count === 0) return Promise.resolve();
    return new Promise((resolve) => this.#waiters.push(resolve));
  }

  #release() {
    const waiters = this.#waiters.splice(0);
    for (const w of waiters) w();
  }
}

/** errgroup semantics: first rejection cancels the rest, wait() surfaces that error. */
export class Group {
  #wg = new WaitGroup();
  #controller = new AbortController();
  #firstError = null;
  #limit = Infinity;
  #running = 0;
  #gateWaiters = [];

  constructor({ signal, limit } = {}) {
    this.#limit = Number.isFinite(limit) && limit > 0 ? Math.floor(limit) : Infinity;
    if (signal !== undefined) {
      if (signal.aborted) this.#controller.abort(signal.reason);
      else signal.addEventListener('abort', () => this.#controller.abort(signal.reason), { once: true });
    }
  }

  get signal() { return this.#controller.signal; }
  get error() { return this.#firstError; }

  /** Start one task; the group waits for it and cancels siblings on the first error. */
  go(fn) {
    this.#wg.add(1);
    (async () => {
      if (this.#running >= this.#limit) await new Promise((r) => this.#gateWaiters.push(r));
      this.#running++;
      try {
        await fn(this.#controller.signal);
      } catch (error) {
        if (this.#firstError === null) { this.#firstError = error; this.#controller.abort(error); }
      } finally {
        this.#running--;
        const next = this.#gateWaiters.shift();
        if (next !== undefined) next();
        this.#wg.done();
      }
    })();
    return this;
  }

  async wait() {
    await this.#wg.wait();
    if (this.#firstError !== null) throw this.#firstError;
  }
}

/**
 * Fire-and-forget on the micro lane - the closest thing to \`go f()\` that stays on
 * this thread. Use it for IO-shaped work; CPU work belongs in a Pool.
 * @returns {{ promise: Promise<any>, cancel: (reason?: any) => void }}
 */
export function go(fn) {
  const controller = new AbortController();
  const promise = Promise.resolve().then(() => fn(controller.signal));
  promise.catch(() => {}); // never an unhandled rejection: the caller may ignore it
  return { promise, cancel: (reason) => controller.abort(reason) };
}

/** Bounded fan-out over a list (errgroup.SetLimit): resolves in input order. */
export async function parallel(items, mapper, { limit = Infinity, signal } = {}) {
  const list = [...items];
  const results = new Array(list.length);
  const group = new Group({ limit, signal });
  list.forEach((item, index) => {
    group.go(async (sig) => { results[index] = await mapper(item, index, sig); });
  });
  await group.wait();
  return results;
}

export function sleep(ms, { signal } = {}) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => { cleanup(); resolve(); }, ms);
    const onAbort = () => { cleanup(); reject(signal.reason ?? new Error('goroutine: aborted')); };
    function cleanup() { clearTimeout(timer); signal?.removeEventListener?.('abort', onAbort); }
    if (signal !== undefined) {
      if (signal.aborted) { onAbort(); return; }
      signal.addEventListener('abort', onAbort, { once: true });
    }
  });
}
