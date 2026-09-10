# Go's scheduler vs Node's reality — what actually ports

Sources are checked out under `ref/`: `ref/go` (sparse: `src/runtime`, `src/sync`) and
`ref/piscina`. Every Go claim below cites a file and line in that tree, so it can be
re-read rather than trusted. Every Node number was measured on this host
(Windows, 8 usable cores, Node v26.7.0) — see §4 for the harness.

## 1. The Go scheduler, reduced to the mechanisms that matter

| mechanism | where | what it does |
|---|---|---|
| **G / M / P** | `runtime/proc.go` header (~L18-40) | G = goroutine, M = OS thread, P = a *permission to execute Go code*. `GOMAXPROCS` Ps; many more Ms can exist blocked in syscalls. |
| **per-P run queue** | `runqput` L7530, `runqget` L7650 | Each P owns a 256-slot local queue; `runqput` has a **fast path** that tries the lock-free slot first, then the queue, and only spills to the global queue when full (`runqputslow` L7576). Contention is avoided by making the common case local. |
| **work stealing, in batches** | `runqsteal` L7782 | An idle P steals **half** of a victim's local queue — one atomic exchange instead of 128 individual steals. Comment at L3528: *"Spinning Ms: steal work from other Ps."* |
| **spinning Ms** | L3528-3530, `wakep` L3228 | A limited number of Ms spin looking for work instead of parking immediately: *"Limit the number of spinning Ms to half the number of busy Ps."* Latency vs wasted CPU, tuned by a constant. |
| **park/unpark** | `stopm`, `startm`, `notesleep` | Parked Ms are cheap to wake; the policy exists because **thread creation is expensive** relative to a wake. |
| **sysmon** | `sysmon` L6538, `retake` L6682, `preemptone` L6717 | A background thread that retakes Ps from long syscalls and **preempts** long-running goroutines (async preemption since Go 1.14). |
| **netpoller** | `netpoll` L29, `netpollready` L494, `netpollblock` L548 | IO readiness is just another event source that makes a G runnable (`goready`). Blocking IO never consumes an M. |
| **semaphore: spin, then park** | `sema.go` `cansemacquire` L291, `semacquire1` L146, `semrelease1` L207 | Uncontended acquire = one CAS; contended = park on a treap. Same philosophy: fast path lock-free, slow path parked. |
| **\`sync.Pool\` per-P sharding** | `sync/pool.go` `poolLocalInternal` | Each P gets a **private** slot plus a shared chain, padded to 128 bytes to avoid false sharing; other Ps steal from the *tail*. |

## 2. What Node actually gives us (and what it does not)

* **libuv's thread pool is not worker_threads.** `fs`, `dns`, `crypto`, `zlib` already run on libuv's pool (default 4 threads, `UV_THREADPOOL_SIZE`). So "Node is single-threaded" is only true of *JS execution*; the IO is threaded for you — which is why the hot path we fixed (sync `readFileSync` + parse) froze the UI even though IO could have been async.
* **Each `worker_threads` Worker gets its own V8 isolate and its own event loop.** That is the closest thing to an M, and it is why worker startup is not free (measured below).
* **Messages go through structured clone**, not shared memory. Cost scales with payload size, and functions cannot be sent (hence our two lanes: module reference vs function *source*).
* **`SharedArrayBuffer` + `Atomics` are the only true shared-memory primitives**, and they exist in Node, Deno and Bun — so they are the cross-platform way to build a fast control path.
* **The main thread must never block.** `Atomics.wait` is legal on Node's main thread, but using it there would recreate exactly the freeze we removed. Only workers may park; the main thread waits with promises.
* **No preemption.** A tight JS loop cannot be interrupted. Go's sysmon/preemptone has no port; the answer is task granularity, not a scheduler.

## 3. Measured costs (this host, Node v26.7.0, 8 cores)

| primitive | result | consequence |
|---|---|---|
| Worker cold start | **17.2 ms** (16.3–18.4) | Never spawn per task. Park instead of kill: an idle worker is 17 ms cheaper than a new one. Our `idleTimeout` retirement is therefore a *memory* decision, not a latency one. |
| `postMessage` round trip, tiny payload | **20.4 µs** | This is the real per-task dispatch overhead. A task must cost ≥ ~0.2 ms for dispatch to stay under 10%; batch dispatch amortises it further. |
| `postMessage` round trip, 1 MB `Uint8Array` | **583.5 µs** | ~29× the tiny payload for 1 MB → **clone, not dispatch, dominates**. Use `transfer`/`SharedArrayBuffer` for anything large. |
| SAB + busy-spin wake | **p50 3.4 µs / p95 12.6 µs** | A shared-memory control path is ~6× faster than postMessage — worth it only for very small, very frequent handoffs. |
| SAB + `Atomics.wait` park/unpark | **p50 11.5 µs / p95 35.9 µs** | Parking in JS costs about the same as a message round trip; libuv already blocks an idle worker's thread for free, so hand-rolled parking buys nothing by itself. |

## 4. Mapping: what ports, what does not

| Go mechanism | verdict for JS | why |
|---|---|---|
| Local run queue + fast path before contention | **adopt** | Our dispatch path already prefers "an idle worker, else the queue"; the measurement says the queue hop is 20 µs, not the problem. |
| **Batch handoff** ("runqget"/"runqsteal" take half) | **rejected - measured slower** | Implemented, then benchmarked on 2000 tiny tasks: **38.5 ms -> 97.7 ms** (batch 8, 4 workers) and **74.6 ms -> 348 ms** (batch 8, one worker). Fewer messages, worse wall time: a batch occupies one worker while the rest starve. Go batches to cut **lock contention on per-P queues**; this queue is shared and dispatch is already ~20 us. Removed from the API so nobody re-adds it. |
| Work *stealing* between workers | **reject** | Stealing exists because each P owns a private queue with no central arbiter. Our queue is shared and already centralised; a steal is strictly more work than a dequeue. |
| Spinning Ms | **partial** | Only meaningful together with a shared-memory queue. With `postMessage` dispatch the receiving isolate is woken by libuv's own poll — measured at 20 µs, close to the 12 µs spin p95. A spin loop would burn a core to save ~10 µs. **Not implemented**; documented so nobody "optimises" it back in. |
| Park/unpark policy | **adopt via idleTimeout** | We already keep workers warm and retire on idle; the 17 ms spawn number is the justification. |
| sysmon / preemption | **reject (impossible)** | No preemption in JS. Replace with granularity limits (see §5). |
| netpoller | **already have it** | libuv is the netpoller. Async IO in a plugin should `await`, not spawn a worker. |
| semaphore spin-then-park | **adopt in spirit** | Our queue is lock-free in the uncontended case (array/ring push) and only allocates on overflow — the same "fast path first" discipline. |
| `sync.Pool` per-P sharding | **adopt, adapted** | Per-*worker* scratch reuse avoids per-task allocation and, more importantly, avoids *sending* buffers at all (the 583 µs number). Documented as a pattern: keep the buffer in the worker, send the command. |

## 5. Consequences adopted in this library

1. **Lazy spawn, warm park, retire only on idle** — 17.2 ms per worker start.
2. **Event-driven producer backpressure.** Chasing the batching result exposed the real
   bug: the "wait" overflow policy parked the producer with `setTimeout(1)`, capping a
   burst at ~1 task/ms. Producers now park on a waiter queue and are woken by the
   dequeue that frees room - Go's `sendq` sudog, not a poll. 2000 tiny tasks:
   **74.6 ms -> 48 ms** (1 worker), **38.5 ms -> 29.4 ms** (4 workers).
3. **Transfer or share, never clone big payloads** — 583 µs per MB.
4. **Granularity rule of thumb** — dispatch is ~20 µs; keep tasks ≥ 0.2 ms and chunk the rest. A 10 ms task with a 20 µs dispatch is 0.2% overhead; 10 000 tiny tasks is 0.2 s of pure messaging.
5. **Never block the main thread.** Workers may spin/park (Atomics); the host thread only awaits.
6. **No preemption, so bound the work instead**: `concurrentTasksPerWorker`, `maxQueue`, per-task `AbortSignal` — the only levers JS actually has.

## 6. Reproducing the numbers

```bash
node -e "…"   # see test/fixtures/bench-worker.mjs for the worker side
```

`test/fixtures/bench-worker.mjs` holds the echo/spin worker used for §3; the harness
itself is a ~60-line script (spawn 5 workers, 300 tiny ping-pongs, 20 × 1 MB ping-pongs,
200 spin wake-ups, 200 `Atomics.wait` wake-ups).
