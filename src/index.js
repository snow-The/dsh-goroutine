/**
 * @snow-the/goroutine - Go's concurrency vocabulary on worker_threads.
 *
 *   import { Pool, go, WaitGroup, Group, Channel, parallel } from '@snow-the/goroutine';
 *
 * See DESIGN.md for the Go/piscina lineage and the Node/Bun/Deno compatibility rules.
 */
export { Pool, RingQueue } from './pool.js';
export { Channel } from './channel.js';
export { WaitGroup, Group, go, parallel, sleep } from './group.js';
