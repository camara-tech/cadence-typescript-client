/**
 * Unofficial Cadence TypeScript client.
 *
 * Core surface:
 *  - {@link Connection}: gRPC transport to the Cadence frontend (api/v1).
 *  - {@link WorkflowClient}: start/signal/query/cancel/terminate workflows,
 *    await results, describe executions, read history, list executions.
 *  - {@link Worker}: hosts workflow and activity implementations.
 *  - {@link JsonDataConverter}: default JSON payload encoding.
 *  - Interceptors: observe workflow/activity invocations.
 *
 * @example
 * ```ts
 * const connection = Connection.connect({ address: '127.0.0.1:7833' });
 * const client = new WorkflowClient(connection);
 * await client.ensureDomain('my-domain');
 * const run = await client.startWorkflow('myWorkflow', {
 *   taskList: 'my-task-list',
 *   executionStartToCloseTimeoutMs: 60_000,
 * }, 'my-domain', 'arg1');
 * const result = await client.waitForClose('my-domain', run.workflowId, run.runId);
 * ```
 */
export {
  WorkflowClient,
  durationFromMs,
  type ClientOptions,
  type StartWorkflowOptions,
  type StartedWorkflow,
  type WorkflowCloseResult,
  type WorkflowExecutionDescription,
  type WorkflowExecutionSummary,
  type HistoryEventSummary,
  type WorkflowIdReusePolicyName,
} from './client.js';
export { Connection } from './connection.js';
export type { ConnectionOptions } from './connection.js';
export { JsonDataConverter } from './data-converter.js';
export type { DataConverter } from './data-converter.js';
export { CadenceError } from './errors.js';
export type { CadenceErrorCode } from './errors.js';
export { Worker } from './worker.js';
export type { WorkerOptions } from './worker.js';
export {
  ApplicationError,
  ContinueAsNewSignal,
  type ActivityContext,
  type ActivityOptions,
  type WorkflowContext,
} from './runtime.js';
export {
  chainInterceptorFactories,
  type WorkflowInterceptor,
  type WorkflowInterceptorFactory,
} from './interceptors.js';
