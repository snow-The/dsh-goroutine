# @snow-the/goroutine — design notes

Go's concurrency vocabulary, mapped onto `node:worker_threads` so the same code runs
on Node, Bun and Deno. Read together with `ref/piscina` and `ref/go`.

## 1. What we borrow, and from where

| Go concept (source) | Here | Notes |
|---|---|---|
| `G` goroutine (`runtime/proc.go`) | **Task** | A unit of work. Two lanes: **micro** (promise/microtask — IO, trivial work) and **worker** (a real thread — CPU work). |
| `M` machine thread | **Worker** | One `worker_threads` Worker. Never more than `size`. |
| `P` processor / `GOMAXPROCS` | **Slot** (`concurrentTasksPerWorker`) | How many tasks one worker may run at once. Default 1 (a worker is a "thread of execution"). |
| `go f(x)` | `go(fn)` / `pool.spawn(fn)` | Fire-and-forget; returns a handle with `.cancel()` + `.then()`. |
| `chan T` (`runtime/chan.go`: `hchan` = ring buffer + `sendq`/`recvq`) | `Channel` | Bounded ring buffer; `send` awaits when full, `recv` awaits when empty, `close()` wakes everyone; `for await (const v of ch)` == `for v := range ch`. |
| `sync.WaitGroup` (`sync/waitgroup.go`: counter + waiter count + sema cascade) | `WaitGroup` | `add/done/wait`, plus `wg.go(fn)` (the modern `WaitGroup.Go`). |
| `errgroup` (x/sync) | `Group` | First error wins, cancels the rest via an internal `AbortController`. |
| `context.Context` | `AbortSignal` | Accepted by every entry point; cancelling rejects/aborts queued and running tasks. |
| `sync.Pool` (`sync/pool.go`) | **Idle worker reuse** | Workers are kept warm and only retired after `idleTimeout` — the "park the M, don't kill it" half of Go's parking policy. |
| `runtime.Gosched` / fairness | — | JS has no preemption; `concurrentTasksPerWorker > 1` is an explicit opt-in to interleaving, documented as such. |

From **piscina** we take the operational rules rather than the code shape:

* `count` / `minThreads` + `maxThreads` and **lazy spawn** — never create a worker you don't need.
* **Bounded queue + overflow policy** (`maxQueue`, `overflow: 'wait' | 'throw' | 'drop'`). `wait` is our default because it mirrors Go's blocking channel send; piscina historically threw.
* **Idle timeout** ("park the thread") — Go's proc.go discusses exactly this trade-off: parking too eagerly thrashes, parking never burns CPU. We park after `idleTimeout` (default 10 s) and wake on demand.
* **Transfer lists** (`{ transfer: [ArrayBuffer] }`) for zero-copy results — `structuredClone` semantics, no serialization.
* **Per-task AbortSignal**, and `concurrentTasksPerWorker` as the throughput knob.
* Ring-buffer task queue (`piscina/src/task_queue/fixed_queue.ts`) — O(1) enqueue, no array shifts; we use the same shape.
* `AsyncResource`/AsyncLocalStorage context propagation: **not** a hard dependency here (Bun only implements part of it). We expose an optional `context` passthrough instead of silently relying on Node internals.

## 2. What we deliberately do NOT copy

* Go's distributed per-P run queues, work stealing and `netpoller`. At this scale (a handful of workers inside a plugin host) one mutex-free JS queue plus a least-loaded balancer is strictly simpler and fast enough; proc.go itself lists the centralized design as "works badly" only for machines with many cores, which is not our case.
* piscina's heavy internals (its own `ThreadPool`, `AsynchronouslyCreatedResourcePool`, typed generic surface). We keep the public shape small enough to read in one sitting.
* Preemption. JS cannot interrupt a tight loop; every "goroutine" here is cooperative at `await` boundaries. Documented, not papered over.

## 3. Public surface (target)

```js
import { Pool, go, WaitGroup, Group, Channel, parallel } from '@snow-the/goroutine';

const pool = new Pool({ size: 4 });              // defaults to availableParallelism()
await pool.run('./jobs.js', 'parseLog', [path]);  // module lane: no eval, fastest
await pool.spawn(() => 1 + 1);                    // inline lane: source is shipped to the worker
const ch = new Channel(16);                       // buffered channel
const wg = new WaitGroup();
for (const file of files) wg.go(() => pool.spawn(() => hashFile(file)));
await wg.wait();
const g = new Group(); g.go(a); g.go(b); await g.wait();  // first error cancels the rest
await pool.shutdown({ timeout: 5000 });
```

## 4. Compatibility rules (Node / Bun / Deno)

* ESM only; the worker entry is `.mjs` so ESM is unambiguous without `type` hints.
* Only `node:worker_threads`, `node:os`, `node:events`, `node:url`. No native addons, no `AsyncResource`, no `process.binding`, nothing Bun lacks.
* **Never block on the main thread**: no `Atomics.wait`/`receiveMessageOnPort` spins — every wait is a promise.
* Inline functions are shipped by source (`fn.toString()`) and `eval`-ed in the worker: closures do **not** capture. That is a documented sharp edge; use the module lane when you need imports/state.
* Feature-detect: if `Worker` is missing, the pool degrades to running tasks inline on the micro lane (correct, just not parallel).
