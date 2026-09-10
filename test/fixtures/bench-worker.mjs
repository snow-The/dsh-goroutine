
import { parentPort, workerData } from 'node:worker_threads';
const sab = workerData?.sab;
parentPort.on('message', (msg) => {
  if (msg && msg.kind === 'spin') {
    const i32 = new Int32Array(sab);
    i32[1] = msg.value;                    // "work done"
    Atomics.store(i32, 0, 1);              // signal
    Atomics.notify(i32, 0);
    return;
  }
  parentPort.postMessage(msg);              // echo
});
