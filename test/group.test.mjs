import { test } from 'node:test';
import assert from 'node:assert/strict';
import { WaitGroup, Group, go, parallel, sleep } from '../src/group.js';

test('WaitGroup waits for every tracked task', async () => {
  const wg = new WaitGroup();
  const done = [];
  for (const id of [1, 2, 3]) wg.go(async () => { await sleep(10); done.push(id); });
  await wg.wait();
  assert.equal(done.length, 3);
  assert.equal(wg.count, 0);
});

test('Group cancels siblings on the first error', async () => {
  const group = new Group();
  let siblingAborted = false;
  group.go(async () => { throw new Error('first failure'); });
  group.go(async (signal) => {
    try { await sleep(500, { signal }); } catch { siblingAborted = true; throw new Error('aborted'); }
  });
  await assert.rejects(() => group.wait(), /first failure/);
  assert.equal(siblingAborted, true, 'the sibling observed the cancellation');
});

test('parallel keeps input order and honours the limit', async () => {
  let live = 0;
  let peak = 0;
  const out = await parallel([5, 1, 3], async (ms) => {
    live++; peak = Math.max(peak, live);
    await sleep(ms);
    live--;
    return ms * 2;
  }, { limit: 2 });
  assert.deepEqual(out, [10, 2, 6]);
  assert.ok(peak <= 2, 'limit respected: peak=' + peak);
});

test('go() runs on the micro lane and can be cancelled', async () => {
  let ran = false;
  const handle = go(async () => { ran = true; return 1; });
  assert.equal(await handle.promise, 1);
  assert.equal(ran, true);
  assert.equal(typeof handle.cancel, 'function');
});
