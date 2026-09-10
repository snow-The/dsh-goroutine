/** Fixture jobs for the module lane. */
export function add(a, b) { return a + b; }
export async function slow(ms) { await new Promise((r) => setTimeout(r, ms)); return ms; }
export function fail(message) { throw new Error(message ?? 'boom'); }
export function bigBuffer(bytes) { const b = new Uint8Array(bytes); b[0] = 7; return b; }
export function concat(a, b) { return String(a) + String(b); }
