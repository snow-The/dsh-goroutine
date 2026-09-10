# @snow-the/goroutine

Go's concurrency vocabulary for JS runtimes — **goroutines, channels, WaitGroups and
an errgroup** — built only on `node:worker_threads`, so the same code runs on Node,
Deno and (by construction) Bun. No native addons, no `AsyncResource`, no blocking waits.

```js
import { Pool, go, WaitGroup, Group, Channel, parallel } from '@snow-the/goroutine';

const pool = new Pool({ size: 4 });                    // worker threads ("M"s)
await pool.run('./jobs.mjs', 'parseLog', [path]);      // module lane: no eval, imports work
await pool.spawn(() => heavyMath());                   // inline lane: goroutine feel (no closures)

const ch = new Channel(16);                            // buffered; 0 = rendezvous
const wg = new WaitGroup();
for (const file of files) wg.go(() => pool.run('./jobs.mjs', 'hash', [file]));
await wg.wait();
for await (const value of ch) { /* for v := range ch */ }

const group = new Group();                             // errgroup: first error cancels the rest
group.go(() => fetchA(group.signal));
group.go(() => fetchB(group.signal));
await group.wait();

await pool.shutdown({ timeout: 5000 });
async function heavyMath() { return 6 * 7; }
```

## API

| export | what it is |
|---|---|
| `Pool` | bounded worker pool: `run` (module lane), `spawn` (inline lane), `drain`, `shutdown`, `stats`; options `size/minThreads/maxQueue/overflow/idleTimeout/concurrentTasksPerWorker/retryOnCrash` |
| `Channel` | Go channel: buffered ring, FIFO senders/receivers, `send/recv/trySend/tryRecv/close`, async-iterable |
| `WaitGroup` | `add/done/wait` plus `go(fn)` (Go 1.25's `WaitGroup.Go`) |
| `Group` | errgroup: `go(fn)`, `signal`, first error wins and aborts siblings |
| `go(fn)` | micro-lane fire-and-forget ("go func()" for IO-shaped work) |
| `parallel` | bounded fan-out over a list, results in input order |
| `sleep` | abortable sleep |

## Runtime matrix (this repo's suite, 17 tests)

| runtime | result |
|---|---|
| Node v26.7.0 (`npm test`) | **17/17 pass** |
| Deno 2.4.4 (`deno test --no-check --allow-read --allow-env test/`) | **17/17 pass** |
| Bun | not installed here — untested; only `node:worker_threads`/`node:os`/`node:events` are used, so it should work. Install Bun and run `bun test test/` to confirm. |

## Design

See [DESIGN.md](./DESIGN.md) for the Go→JS mapping (G/M/P, `hchan`, `sync.WaitGroup`,
`sync.Pool`), what was adopted from [piscina](https://github.com/piscinajs/piscina),
what was deliberately rejected, and the compatibility rules. The studied sources are
checked out under `ref/` (git-ignored): `ref/piscina` and `ref/go` (sparse:
`src/runtime` + `src/sync`).

## Sharp edges (documented, not hidden)

* JS cannot preempt: every "goroutine" is cooperative at `await` boundaries.
* `pool.spawn(fn)` ships `fn.toString()` to the worker — **closures do not capture**.
  Use the module lane when you need imports or captured state.
* Task arguments and results must be structured-cloneable; pass `{ transfer: [...] }`
  for zero-copy `ArrayBuffer` handoff.
