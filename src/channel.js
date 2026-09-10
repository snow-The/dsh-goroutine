/**
 * channel.js - Go channels, promise-shaped.
 *
 * Mirrors the semantics of ref/go/src/runtime/chan.go (hchan: circular buffer +
 * sendq/recvq + closed flag) with one unavoidable difference: JS cannot park a
 * thread, so a blocked send/receive is an unawaited promise instead of a stopped
 * goroutine. Everything else matches:
 *   - capacity 0 is a rendezvous channel: a send completes when a receiver takes it;
 *   - capacity > 0 buffers, and a send on a full channel waits (Go blocks);
 *   - receivers are FIFO, senders are FIFO;
 *   - close() makes further sends throw, lets receivers drain the buffer, then
 *     hands them undefined - so \`for await (const v of ch)\` ends exactly like
 *     \`for v := range ch\`.
 */

export class Channel {
  #buffer;
  #capacity;
  #closed = false;
  #senders = [];    // { value, resolve, reject } waiting for room
  #receivers = [];  // { resolve, reject } waiting for a value

  constructor(capacity = 0) {
    this.#capacity = Math.max(0, Math.floor(Number(capacity) || 0));
    this.#buffer = [];
  }

  get capacity() { return this.#capacity; }
  get size() { return this.#buffer.length; }
  get closed() { return this.#closed; }
  get waitingSenders() { return this.#senders.length; }
  get waitingReceivers() { return this.#receivers.length; }

  /** Send a value; the returned promise settles when the value is accepted. */
  send(value) {
    if (this.#closed) return Promise.reject(new Error('goroutine: send on closed channel'));
    // A waiting receiver takes it directly (rendezvous handoff).
    const receiver = this.#receivers.shift();
    if (receiver !== undefined && this.#buffer.length === 0) {
      receiver.resolve(value);
      return Promise.resolve();
    }
    if (this.#buffer.length < this.#capacity) {
      this.#buffer.push(value);
      this.#flushReceivers();
      return Promise.resolve();
    }
    return new Promise((resolve, reject) => {
      this.#senders.push({ value, resolve, reject });
      this.#flushReceivers();
    });
  }

  /** Non-blocking send: false when the channel is closed or full. */
  trySend(value) {
    if (this.#closed) return false;
    const receiver = this.#receivers.shift();
    if (receiver !== undefined && this.#buffer.length === 0) { receiver.resolve(value); return true; }
    if (this.#buffer.length < this.#capacity) { this.#buffer.push(value); this.#flushReceivers(); return true; }
    return false;
  }

  /** Receive the next value; resolves undefined once a closed channel is drained. */
  recv() {
    if (this.#buffer.length > 0) {
      const value = this.#buffer.shift();
      this.#flushSenders();
      return Promise.resolve(value);
    }
    const sender = this.#senders.shift();
    if (sender !== undefined) {
      sender.resolve();
      return Promise.resolve(sender.value);
    }
    if (this.#closed) return Promise.resolve(undefined);
    return new Promise((resolve, reject) => this.#receivers.push({ resolve, reject }));
  }

  /** Non-blocking receive: { ok, value } - ok is false when empty. */
  tryRecv() {
    if (this.#buffer.length > 0) {
      const value = this.#buffer.shift();
      this.#flushSenders();
      return { ok: true, value };
    }
    const sender = this.#senders.shift();
    if (sender !== undefined) { sender.resolve(); return { ok: true, value: sender.value }; }
    return { ok: false, value: undefined };
  }

  /**
   * Close the channel: no further sends, buffered values stay receivable, and
   * every blocked receiver is released (Go's closechan wakes the whole recvq).
   */
  close() {
    if (this.#closed) return;
    this.#closed = true;
    for (const receiver of this.#receivers.splice(0)) receiver.resolve(undefined);
    for (const sender of this.#senders.splice(0)) sender.reject(new Error('goroutine: send on closed channel'));
  }

  #flushReceivers() {
    while (this.#receivers.length > 0) {
      const receiver = this.#receivers.shift();
      if (this.#buffer.length > 0) { receiver.resolve(this.#buffer.shift()); this.#flushSenders(); continue; }
      if (this.#senders.length > 0 && this.#capacity === 0) {
        const sender = this.#senders.shift();
        sender.resolve();
        receiver.resolve(sender.value);
        continue;
      }
      this.#receivers.unshift(receiver);
      return;
    }
  }

  #flushSenders() {
    while (this.#senders.length > 0 && this.#buffer.length < this.#capacity) {
      const sender = this.#senders.shift();
      this.#buffer.push(sender.value);
      sender.resolve();
    }
  }

  async *[Symbol.asyncIterator]() {
    for (;;) {
      const value = await this.recv();
      if (value === undefined && this.#closed) return;
      yield value;
    }
  }
}
