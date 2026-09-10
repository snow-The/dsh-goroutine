import { test } from 'node:test';
import assert from 'node:assert/strict';
import { Channel } from '../src/channel.js';

test('buffered channel hands values over in FIFO order', async () => {
  const ch = new Channel(2);
  await ch.send('a');
  await ch.send('b');
  assert.equal(ch.size, 2);
  assert.equal(await ch.recv(), 'a');
  assert.equal(await ch.recv(), 'b');
});

test('a full channel makes the sender wait (Go blocking send)', async () => {
  const ch = new Channel(1);
  await ch.send(1);
  let sent = false;
  const pending = ch.send(2).then(() => { sent = true; });
  await new Promise((r) => setTimeout(r, 20));
  assert.equal(sent, false, 'send must not complete while the buffer is full');
  assert.equal(await ch.recv(), 1);
  await pending;
  assert.equal(sent, true);
  assert.equal(await ch.recv(), 2);
});

test('an unbuffered channel is a rendezvous', async () => {
  const ch = new Channel(0);
  const received = ch.recv();
  await ch.send('handshake');
  assert.equal(await received, 'handshake');
});

test('close ends iteration and rejects later sends', async () => {
  const ch = new Channel(2);
  await ch.send(1);
  await ch.send(2);
  ch.close();
  const seen = [];
  for await (const value of ch) seen.push(value);
  assert.deepEqual(seen, [1, 2], 'buffer drains before the channel ends');
  await assert.rejects(() => ch.send(3), /closed channel/);
});

test('a blocked receiver is released by close', async () => {
  const ch = new Channel(0);
  const pending = ch.recv();
  ch.close();
  assert.equal(await pending, undefined);
});
