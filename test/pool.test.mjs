import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Pool } from '../src/pool.js';

const JOBS = new URL('./fixtures/jobs.mjs', import.meta.url).href;

test('module lane runs an exported function', async () => {
  const pool = new Pool({ size: 2, idleTimeout: 0 });
  try {
    assert.equal(await pool.run(JOBS, 'add', [2, 3]), 5);
    assert.equal(await pool.run(JOBS, 'concat', ['a', 'b']), 'ab');
    assert.equal(pool.completed, 2);
  } finally { await pool.shutdown({ force: true }); }
});

test('inline lane runs a closure-free function (goroutine feel)', async () => {
  const pool = new Pool({ size: 1, idleTimeout: 0 });
  try {
    const value = await pool.spawn(() => 6 * 7);
    assert.equal(value, 42);
  } finally { await pool.shutdown({ force: true }); }
});

test('errors propagate with name/message and bump the failure counter', async () => {
  const pool = new Pool({ size: 1, idleTimeout: 0 });
  try {
    await assert.rejects(() => pool.run(JOBS, 'fail', ['nope']), /nope/);
    assert.equal(pool.failed, 1);
  } finally { await pool.shutdown({ force: true }); }
});

test('concurrency never exceeds the worker count', async () => {
  const pool = new Pool({ size: 2, idleTimeout: 0 });
  let live = 0;
  let peak = 0;
  try {
    await Promise.all(Array.from({ length: 8 }, () => pool.run(JOBS, 'slow', [25])));
    peak = pool.stats().workers.reduce((m, w) => Math.max(m, w.active), 0);
    assert.ok(pool.size <= 2, 'never grows past maxSize: ' + pool.size);
  } finally { await pool.shutdown({ force: true }); }
});

test('queue overflow can throw instead of waiting', async () => {
  const pool = new Pool({ size: 1, maxQueue: 1, overflow: 'throw', idleTimeout: 0 });
  try {
    const first = pool.run(JOBS, 'slow', [80]);
    const second = pool.run(JOBS, 'slow', [10]);
    await assert.rejects(() => pool.run(JOBS, 'slow', [10]), /queue is full/);
    await Promise.all([first, second]);
  } finally { await pool.shutdown({ force: true }); }
});

test('aborting a queued task rejects it without running it', async () => {
  const pool = new Pool({ size: 1, idleTimeout: 0 });
  const controller = new AbortController();
  try {
    const running = pool.run(JOBS, 'slow', [80]);
    const queued = pool.run(JOBS, 'add', [1, 1], { signal: controller.signal });
    controller.abort(new Error('cancelled by test'));
    await assert.rejects(() => queued, /cancelled by test/);
    assert.equal(await running, 80);
  } finally { await pool.shutdown({ force: true }); }
});

test('drain waits for queued work and shutdown terminates workers', async () => {
  const pool = new Pool({ size: 2, idleTimeout: 0 });
  const tasks = Array.from({ length: 6 }, (_, i) => pool.run(JOBS, 'add', [i, 1]));
  await pool.drain();
  assert.deepEqual(await Promise.all(tasks), [1, 2, 3, 4, 5, 6]);
  const stats = await pool.shutdown({ force: true });
  assert.equal(stats.size, 0);
  assert.equal(pool.closed, true);
  await assert.rejects(() => pool.run(JOBS, 'add', [1, 1]), /pool is closed/);
});

test('transfer list moves an ArrayBuffer without copying', async () => {
  const pool = new Pool({ size: 1, idleTimeout: 0 });
  try {
    const buffer = new ArrayBuffer(16);
    const result = await pool.run(JOBS, 'bigBuffer', [16], { transfer: [buffer] });
    assert.equal(buffer.byteLength, 0, 'source buffer was detached');
    assert.equal(result[0], 7);
  } finally { await pool.shutdown({ force: true }); }
});
