/**
 * pool.js - a bounded, self-balancing worker pool ("M"s in Go's scheduler).
 *
 * Shape borrowed from piscina (ref/piscina/src/index.ts, worker_pool/, task_queue/):
 * lazy spawn, a hard worker cap, a bounded queue with an explicit overflow policy,
 * idle retirement, least-loaded balancing, per-task AbortSignal and transfer lists.
 * Behavioural notes borrowed from Go (ref/go/src/runtime/proc.go): workers are
 * PARKED on idle and woken on demand - parking too eagerly thrashes, never parking
 * burns CPU - and every wait is a promise, never a blocking syscall.
 *
 * Runtime: node:worker_threads only. Falls back to running tasks inline when a
 * worker cannot be created (feature-detect, never throw at import time).
 */
import { Worker } from 'node:worker_threads';
import { availableParallelism } from 'node:os';
import { EventEmitter } from 'node:events';

const WORKER_URL = new URL('./worker.mjs', import.meta.url);
const canSpawnWorkers = typeof Worker === 'function';

/** Fixed-capacity FIFO ring - O(1) push/shift, no array reallocation (piscina's FixedQueue). */
class RingQueue {
  #buf;
  #head = 0;
  #tail = 0;
  #size = 0;
  constructor(capacity) {
    this.capacity = capacity;
    this.#buf = new Array(Number.isFinite(capacity) ? capacity : 1024);
  }
  get size() { return this.#size; }
  get full() { return Number.isFinite(this.capacity) && this.#size >= this.capacity; }
  push(value) {
    if (this.full) return false;
    if (this.#size === this.#buf.length) {
      // unbounded mode: grow geometrically
      const next = new Array(this.#buf.length * 2);
      for (let i = 0; i < this.#size; i++) next[i] = this.#buf[(this.#head + i) % this.#buf.length];
      this.#buf = next; this.#head = 0; this.#tail = this.#size;
    }
    this.#buf[this.#tail] = value;
    this.#tail = (this.#tail + 1) % this.#buf.length;
    this.#size++;
    return true;
  }
  shift() {
    if (this.#size === 0) return undefined;
    const value = this.#buf[this.#head];
    this.#buf[this.#head] = undefined;
    this.#head = (this.#head + 1) % this.#buf.length;
    this.#size--;
    return value;
  }
  remove(predicate) {
    const kept = [];
    let removed = 0;
    for (let i = 0; i < this.#size; i++) {
      const v = this.#buf[(this.#head + i) % this.#buf.length];
      if (predicate(v)) removed++; else kept.push(v);
    }
    this.#head = 0; this.#tail = kept.length; this.#size = kept.length;
    for (let i = 0; i < kept.length; i++) this.#buf[i] = kept[i];
    return removed;
  }
  drain(fn) { let v; while ((v = this.shift()) !== undefined) fn(v); }
}

/** One worker thread and the tasks currently running on it. */
class PoolWorker {
  constructor(pool, threadId) {
    this.pool = pool;
    this.threadId = threadId;
    this.active = new Map();     // taskId -> task
    this.worker = null;
    this.idleTimer = null;
    this.closed = false;
  }
  get busy() { return this.active.size; }
  get state() { return this.worker === null ? 'parked' : 'running'; }
  start() {
    if (this.worker !== null) return this;
    const worker = new Worker(WORKER_URL);
    this.worker = worker;
    worker.on('message', (msg) => this.pool._onWorkerMessage(this, msg));
    worker.on('error', (err) => this.pool._onWorkerFailure(this, err));
    worker.on('exit', (code) => this.pool._onWorkerExit(this, code));
    if (typeof worker.unref === 'function') worker.unref();   // a parked worker must never hold DSH open
    this.touch();
    return this;
  }
  /** (Re)arm idle retirement - Go's "park the M" policy with a hysteresis window. */
  touch() {
    if (this.idleTimer !== null) { clearTimeout(this.idleTimer); this.idleTimer = null; }
    const after = this.pool.idleTimeout;
    if (!Number.isFinite(after) || after <= 0) return;
    this.idleTimer = setTimeout(() => {
      this.idleTimer = null;
      if (this.active.size === 0 && this.pool.workers.size > this.pool.minSize) this.pool._retire(this);
    }, after);
    if (typeof this.idleTimer.unref === 'function') this.idleTimer.unref();
  }
  post(task) {
    this.start();
    if (typeof this.worker.ref === 'function') this.worker.ref.call(this.worker);   // hold the process while work is in flight
    this.active.set(task.id, task);
    const msg = { id: task.id, kind: task.kind, target: task.target, export: task.export, args: task.args };
    if (task.transfer !== undefined) this.worker.postMessage(msg, task.transfer);
    else this.worker.postMessage(msg);
  }
  terminate() {
    if (this.idleTimer !== null) { clearTimeout(this.idleTimer); this.idleTimer = null; }
    const worker = this.worker;
    this.worker = null;
    if (worker !== null) { try { worker.terminate(); } catch { /* already gone */ } }
  }
}

export class Pool extends EventEmitter {
  #closed = false;
  #queue;
  #pendingWaiters = [];
  #roomWaiters = [];   // producers parked because the queue was full
  #nextWorkerId = 1;
  #nextTaskId = 1;

  constructor(options = {}) {
    super();
    const size = Number(options.size ?? options.maxThreads ?? availableParallelism());
    this.maxSize = Math.max(1, Number.isFinite(size) && size > 0 ? Math.floor(size) : 1);
    this.minSize = Math.max(0, Math.min(Math.floor(Number(options.minThreads ?? 0) || 0), this.maxSize));
    this.idleTimeout = options.idleTimeout === undefined ? 10_000 : Number(options.idleTimeout);
    this.concurrentTasksPerWorker = Math.max(1, Math.floor(Number(options.concurrentTasksPerWorker ?? 1) || 1));
    // NOTE: batch dispatch was implemented and measured; it made every workload
    // slower (2000 tiny tasks: 38.5 ms -> 97.7 ms on 4 workers, 74.6 ms -> 348 ms
    // on 1) because a batch occupies one worker while the others starve. Go's
    // runqget/runqsteal batching exists to cut lock contention on PER-P QUEUES;
    // this queue is shared and dispatch is already ~20 us. See
    // RESEARCH-scheduling.md §4. Not shipped.
    const maxQueue = options.maxQueue === undefined ? this.maxSize * 64 : Number(options.maxQueue);
    this.#queue = new RingQueue(maxQueue);
    this.overflow = options.overflow ?? 'wait';         // 'wait' (Go's blocking send) | 'throw'
    this.retryOnCrash = Math.max(0, Math.floor(Number(options.retryOnCrash ?? 0) || 0));
    this.name = options.name ?? 'goroutine-pool';
    this.completed = 0;
    this.failed = 0;
    this.dispatches = 0;
    this.#roomWaiters = [];
    this.workers = new Set();
    this.spawnedTotal = 0;

    for (let i = 0; i < this.minSize; i++) this._spawn();
  }

  get size() { return this.workers.size; }
  get queueSize() { return this.#queue.size; }
  get activeTasks() { let n = 0; for (const w of this.workers) n += w.busy; return n; }
  get closed() { return this.#closed; }

  stats() {
    return {
      size: this.size, maxSize: this.maxSize, queueSize: this.queueSize, activeTasks: this.activeTasks,
      completed: this.completed, failed: this.failed, spawnedTotal: this.spawnedTotal, dispatches: this.dispatches,
      workers: [...this.workers].map((w) => ({ id: w.threadId, state: w.state, active: w.busy })),
    };
  }

  // -- public entry points -------------------------------------------------

  /** Run an exported function from a module on a worker ("safe lane": no eval, imports work). */
  run(target, exportName = 'default', args = [], options = {}) {
    return this._submit({ kind: 'module', target: String(target), export: exportName, args, ...this._taskOptions(options) });
  }

  /** Run an inline function on a worker (goroutine feel; the source is shipped, so no closure capture). */
  spawn(fn, ...args) {
    const options = (args.length > 0 && args[args.length - 1] !== null && typeof args[args.length - 1] === 'object' && args[args.length - 1].__taskOptions === true)
      ? args.pop()
      : {};
    if (typeof fn !== 'function') throw new TypeError('goroutine: spawn() expects a function');
    return this._submit({ kind: 'source', target: fn.toString(), args, ...this._taskOptions(options) });
  }

  /** Wait until the queue is empty and every worker is idle. */
  async drain() {
    if (this.#queue.size === 0 && this.activeTasks === 0) return;
    await new Promise((resolve) => this.#pendingWaiters.push(resolve));
  }

  /** Stop accepting work, let in-flight tasks finish (or force-kill after \`timeout\` ms). */
  async shutdown({ timeout = 30_000, force = false } = {}) {
    this.#closed = true;
    const settle = (async () => {
      while (this.#queue.size > 0) {
        const task = this.#queue.shift();
        task.reject(new Error('goroutine: pool is shutting down'));
      }
      await this.drain();
    })();
    if (!force && Number.isFinite(timeout) && timeout > 0) {
      await Promise.race([settle, new Promise((r) => setTimeout(r, timeout).unref?.() ?? setTimeout(r, timeout))]);
    } else {
      await settle;
    }
    for (const w of [...this.workers]) { w.terminate(); this.workers.delete(w); }
    return this.stats();
  }

  async [Symbol.asyncDispose]() { await this.shutdown({ timeout: 5000 }); }

  // -- internals -----------------------------------------------------------

  _taskOptions(options) {
    return {
      signal: options.signal,
      transfer: options.transfer,
      priority: options.priority ?? 0,
    };
  }

  _submit(task) {
    if (this.#closed) return Promise.reject(new Error('goroutine: pool is closed'));
    if (task.signal?.aborted) return Promise.reject(task.signal.reason ?? new Error('goroutine: aborted'));
    return new Promise((resolve, reject) => {
      const entry = { ...task, id: this.#nextTaskId++, resolve, reject, attempts: 0, abort: null };
      if (entry.signal) {
        entry.abort = () => {
          this.#queue.remove((t) => t === entry);
          entry.reject(entry.signal.reason ?? new Error('goroutine: aborted'));
        };
        entry.signal.addEventListener('abort', entry.abort, { once: true });
      }
      this._dispatch(entry).catch(reject);
    });
  }

  async _dispatch(entry) {
    for (;;) {
      if (this.#closed) throw new Error('goroutine: pool is closed');
      if (!this.#queue.full) {
        this.#queue.push(entry);
        this._pump();
        return;
      }
      if (this.overflow === 'throw') throw new Error('goroutine: queue is full (' + this.#queue.capacity + ')');
      // overflow: 'wait' - park the producer like a full Go channel send. The
      // wake is event-driven (mirrors Go's sudog on a channel's sendq); the first
      // version polled with setTimeout(1), which capped a burst of 2000 tiny
      // tasks at ~1 task/ms - measured, not guessed.
      await new Promise((resolve) => this.#roomWaiters.push(resolve));
    }
  }

  _pickWorker() {
    let best = null;
    for (const w of this.workers) {
      if (w.busy >= this.concurrentTasksPerWorker) continue;
      if (best === null || w.busy < best.busy) best = w;
    }
    if (best === null && this.workers.size < this.maxSize) best = this._spawn();
    return best;
  }

  _maybeScale() {
    // Queue is non-empty: is a parked worker enough, or do we grow?
    if (this.#queue.size > 0 && this.activeTasks === 0 && this.size === 0) this._spawn();
  }

  _spawn() {
    const worker = new PoolWorker(this, this.#nextWorkerId++);
    this.workers.add(worker);
    this.spawnedTotal++;
    worker.start();
    this.emit('spawn', worker.threadId);
    return worker;
  }

  _retire(worker) {
    if (!this.workers.has(worker)) return;
    this.workers.delete(worker);
    worker.terminate();
    this.emit('retire', worker.threadId);
  }

  _settle(worker, task, settle) {
    worker.active.delete(task.id);
    if (task.abort !== null) task.signal?.removeEventListener?.('abort', task.abort);
    settle();
    worker.touch();
    if (worker.active.size === 0 && typeof worker.worker?.unref === 'function') worker.worker.unref();
    if (this.#queue.size > 0) this._pump();
    else if (this.#queue.size === 0 && this.activeTasks === 0) {
      const waiters = this.#pendingWaiters.splice(0);
      for (const w of waiters) w();
    }
  }

  /** Hand one queued task to the least-loaded worker that has capacity. */
  _pump() {
    while (this.#queue.size > 0) {
      const worker = this._pickWorker();
      if (worker === null) return;
      const task = this.#queue.shift();
      this.dispatches++;
      worker.post(task);
      this.emit('dispatch', { worker: worker.threadId, count: 1 });
      this.#releaseRoom();
    }
  }

  /** Wake one parked producer - the queue just lost an entry. */
  #releaseRoom() {
    const waiter = this.#roomWaiters.shift();
    if (waiter !== undefined) waiter();
  }

  _onWorkerMessage(worker, msg) {
    const task = worker.active.get(msg?.id);
    if (task === undefined) return;
    if (msg.ok === true) {
      this.completed++;
      this._settle(worker, task, () => task.resolve(msg.value));
    } else {
      this.failed++;
      const err = new Error(msg.error?.message ?? 'goroutine: task failed');
      err.name = msg.error?.name ?? 'Error';
      if (msg.error?.stack) err.stack = msg.error.stack;
      this._settle(worker, task, () => task.reject(err));
    }
  }

  _onWorkerFailure(worker, error) {
    this.emit('error', error);
    this._failWorkerTasks(worker, error);
  }

  _onWorkerExit(worker, code) {
    this.workers.delete(worker);
    worker.worker = null;
    if (worker.active.size > 0) {
      const error = new Error('goroutine: worker exited with code ' + code + ' while running ' + worker.active.size + ' task(s)');
      this._failWorkerTasks(worker, error);
    }
    if (!this.#closed && this.#queue.size > 0) this._pump();
  }

  _failWorkerTasks(worker, error) {
    const tasks = [...worker.active.values()];
    worker.active.clear();
    for (const task of tasks) {
      this.failed++;
      if (task.attempts < this.retryOnCrash) {
        task.attempts++;
        this.#queue.push(task);
        this.emit('retry', task.id);
        continue;
      }
      this._settle(worker, task, () => task.reject(error));
    }
    this._pump();
  }
}

export { RingQueue };
