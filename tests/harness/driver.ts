import { randomUUID } from 'node:crypto';
import type { CadenceTestDriver } from './port.js';

export type { CadenceTestDriver } from './port.js';
export { DriverError } from './port.js';
export type { DriverErrorCode } from './port.js';

let singleton: Promise<CadenceTestDriver> | undefined;

/**
 * Starts the driver selected by the DRIVER env var:
 *  - "go" (default): HTTP bridge to the Go harness (cadence-go-client) — validates the suite.
 *  - "ts": in-process adapter over this repo's TypeScript client — the TDD target.
 */
export function startTestDriver(): Promise<CadenceTestDriver> {
  if (!singleton) {
    const kind = process.env.DRIVER ?? 'go';
    singleton = kind === 'ts' ? startTsDriver() : startGoDriver();
  }
  return singleton;
}

export async function stopTestDriver(): Promise<void> {
  const driver = await singleton;
  await driver?.close?.();
  singleton = undefined;
}

async function startGoDriver(): Promise<CadenceTestDriver> {
  const { GoHarnessDriver } = await import('./go-harness-client.js');
  const driver = new GoHarnessDriver();
  await driver.ensureRunning();
  return driver;
}

async function startTsDriver(): Promise<CadenceTestDriver> {
  const { TsDriver } = await import('./ts-driver.js');
  return new TsDriver();
}

/** Unique domain name for test isolation. */
export function uniqueDomain(prefix = 'gwt'): string {
  return `${prefix}-${randomUUID().slice(0, 8)}`.toLowerCase();
}

/** Unique workflow id. */
export function uniqueWorkflowId(prefix: string): string {
  return `${prefix}-${randomUUID().slice(0, 8)}`;
}
