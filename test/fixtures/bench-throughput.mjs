/**
 * Throughput probe used by RESEARCH-scheduling.md: N tiny module-lane tasks.
 * Run: node test/fixtures/bench-throughput.mjs
 */
import { Pool } from '../../src/pool.js';
const JOBS = new URL('./jobs.mjs', import.meta.url).href;

const measure = async (workers, tasks) => {
  const pool = new Pool({ size: workers, idleTimeout: 0 });
  const t0 = process.hrtime.bigint();
  await Promise.all(Array.from({ length: tasks }, (_, i) => pool.run(JOBS, 'add', [i, 1])));
  const ms = Number(process.hrtime.bigint() - t0) / 1e6;
  const { dispatches, queueSize } = pool.stats();
  await pool.shutdown({ force: true });
  return { workers, tasks, ms: +ms.toFixed(1), dispatches, perTaskUs: +((ms * 1000) / tasks).toFixed(1), queueLeft: queueSize };
};

const rows = [];
for (const workers of [1, 4, 8]) rows.push(await measure(workers, 2000));
console.log(JSON.stringify(rows));
