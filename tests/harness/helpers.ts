import type { QueryWorkflowRequest } from './port.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function isRetryableConsistencyError(err: unknown): boolean {
  const msg = (err as Error)?.message ?? '';
  return (
    msg.includes('must handle at least one decision task') ||
    msg.includes('EntityNotExists') ||
    msg.includes('not found')
  );
}

interface QueryDriver {
  queryWorkflow(req: QueryWorkflowRequest): Promise<unknown>;
}

/**
 * Polls a query until it succeeds, tolerating the eventual consistency of a
 * workflow that has not yet processed its first decision task.
 */
export async function queryUntil(
  driver: QueryDriver,
  req: QueryWorkflowRequest,
  { timeoutMs = 15_000, intervalMs = 250 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<unknown> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown;
  while (Date.now() < deadline) {
    try {
      return await driver.queryWorkflow(req);
    } catch (e) {
      if (!isRetryableConsistencyError(e)) throw e;
      lastError = e;
      await sleep(intervalMs);
    }
  }
  throw lastError ?? new Error('queryUntil: deadline exceeded');
}

/** Polls an async predicate until it passes or the deadline expires. */
export async function pollUntil(
  fn: () => Promise<boolean>,
  { timeoutMs = 15_000, intervalMs = 250 }: { timeoutMs?: number; intervalMs?: number } = {},
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = new Error('pollUntil: deadline exceeded');
  while (Date.now() < deadline) {
    try {
      if (await fn()) return;
    } catch (e) {
      lastError = e;
    }
    await sleep(intervalMs);
  }
  throw lastError;
}
