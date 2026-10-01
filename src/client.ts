import { randomUUID } from 'node:crypto';
import { Payload, RetryPolicy, WorkflowExecution, WorkflowType } from './generated/uber/cadence/api/v1/common.js';
import { WorkflowIdReusePolicy } from './generated/uber/cadence/api/v1/workflow.js';
import type { Duration as ProtoDuration } from './generated/google/protobuf/duration.js';
import { EventFilterType } from './generated/uber/cadence/api/v1/history.js';
import type { HistoryEvent } from './generated/uber/cadence/api/v1/history.js';
import {
  WorkflowExecutionCloseStatus,
  WorkflowExecutionInfo,
} from './generated/uber/cadence/api/v1/workflow.js';
import {
  DescribeWorkflowExecutionResponse,
  QueryWorkflowResponse,
  StartWorkflowExecutionRequest,
  StartWorkflowExecutionResponse,
  WorkflowAPIClientImpl,
} from './generated/uber/cadence/api/v1/service_workflow.js';
import {
  DeleteDomainRequest,
  DeprecateDomainRequest,
  DomainAPIClientImpl,
  RegisterDomainRequest,
} from './generated/uber/cadence/api/v1/service_domain.js';
import {
  ListClosedWorkflowExecutionsRequest,
  ListOpenWorkflowExecutionsRequest,
  VisibilityAPIClientImpl,
} from './generated/uber/cadence/api/v1/service_visibility.js';
import { Connection } from './connection.js';
import { DataConverter, JsonDataConverter } from './data-converter.js';
import { CadenceError } from './errors.js';

export { CadenceError } from './errors.js';
export type { CadenceErrorCode } from './errors.js';
export { Connection } from './connection.js';
export type { ConnectionOptions } from './connection.js';
export { JsonDataConverter } from './data-converter.js';
export type { DataConverter } from './data-converter.js';

export type WorkflowIdReusePolicyName =
  | 'AllowDuplicateFailedOnly'
  | 'AllowDuplicate'
  | 'RejectDuplicate'
  | 'TerminateIfRunning';

export interface ClientOptions {
  connection: Connection;
  domain?: string;
  dataConverter?: DataConverter;
}

export interface StartWorkflowOptions {
  id?: string;
  taskList: string;
  executionStartToCloseTimeoutMs: number;
  decisionTaskStartToCloseTimeoutMs?: number;
  workflowIdReusePolicy?: WorkflowIdReusePolicyName;
  retryPolicy?: {
    initialIntervalMs: number;
    backoffCoefficient?: number;
    maximumIntervalMs?: number;
    expirationIntervalMs?: number;
    maximumAttempts?: number;
    nonRetryableErrorReasons?: string[];
  };
  cronSchedule?: string;
  memo?: Record<string, unknown>;
  requestId?: string;
}

export interface StartedWorkflow {
  workflowId: string;
  runId: string;
}

export type WorkflowCloseStatus =
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELED'
  | 'TERMINATED'
  | 'TIMED_OUT'
  | 'CONTINUED_AS_NEW';

export interface WorkflowCloseResult {
  status: WorkflowCloseStatus;
  result?: unknown;
  failure?: { type: string; message: string; reason?: string };
}

export interface WorkflowExecutionDescription {
  status: 'RUNNING' | WorkflowCloseStatus;
  historyLength: number;
  activityAttempts: Record<string, number>;
}

export interface HistoryEventSummary {
  eventType: string;
  id: number;
}

export interface WorkflowExecutionSummary {
  workflowId: string;
  runId: string;
  type?: string;
  status: 'RUNNING' | WorkflowCloseStatus;
  startTimeMs?: number;
  closeTimeMs?: number;
  memo?: Record<string, unknown>;
}

const CLOSE_STATUS_NAMES: Record<number, WorkflowCloseStatus> = {
  [WorkflowExecutionCloseStatus.WORKFLOW_EXECUTION_CLOSE_STATUS_COMPLETED]: 'COMPLETED',
  [WorkflowExecutionCloseStatus.WORKFLOW_EXECUTION_CLOSE_STATUS_FAILED]: 'FAILED',
  [WorkflowExecutionCloseStatus.WORKFLOW_EXECUTION_CLOSE_STATUS_CANCELED]: 'CANCELED',
  [WorkflowExecutionCloseStatus.WORKFLOW_EXECUTION_CLOSE_STATUS_TERMINATED]: 'TERMINATED',
  [WorkflowExecutionCloseStatus.WORKFLOW_EXECUTION_CLOSE_STATUS_CONTINUED_AS_NEW]: 'CONTINUED_AS_NEW',
  [WorkflowExecutionCloseStatus.WORKFLOW_EXECUTION_CLOSE_STATUS_TIMED_OUT]: 'TIMED_OUT',
};

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/**
 * Retries transient internal failures (e.g. the server-side domain cache
 * racing a freshly registered domain), mirroring the reference clients'
 * default retry behavior.
 */
async function withRetries<T>(fn: () => Promise<T>, attempts = 8): Promise<T> {
  let lastError: unknown;
  for (let attempt = 0; attempt < attempts; attempt++) {
    try {
      return await fn();
    } catch (err) {
      lastError = err;
      const retryable = err instanceof CadenceError && err.code === 'Internal';
      if (!retryable || attempt === attempts - 1) throw err;
      await sleep(Math.min(1000, 200 * 2 ** attempt));
    }
  }
  throw lastError;
}

const REUSE_POLICY: Record<WorkflowIdReusePolicyName, WorkflowIdReusePolicy> = {
  AllowDuplicateFailedOnly: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
  AllowDuplicate: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
  RejectDuplicate: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
  TerminateIfRunning: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
};

/**
 * Client for starting and getting information about workflow executions,
 * completing activities asynchronously, and managing domains.
 */
export class WorkflowClient {
  readonly identity: string;
  private readonly connection: Connection;
  private readonly converter: DataConverter;
  private readonly workflowApi: WorkflowAPIClientImpl;
  private readonly domainApi: DomainAPIClientImpl;
  private readonly visibilityApi: VisibilityAPIClientImpl;

  constructor(connection: Connection, opts: { dataConverter?: DataConverter } = {}) {
    this.connection = connection;
    this.identity = connection.identity;
    this.converter = opts.dataConverter ?? new JsonDataConverter();
    const rpc = {
      request: (service: string, method: string, data: Uint8Array) =>
        withRetries(() => connection.request(service, method, data)),
    };
    this.workflowApi = new WorkflowAPIClientImpl(rpc);
    this.domainApi = new DomainAPIClientImpl(rpc);
    this.visibilityApi = new VisibilityAPIClientImpl(rpc);
  }

  /** Registers a domain; no-op if it already exists. */
  async ensureDomain(name: string, retentionDaysInDays = 1): Promise<void> {
    const request: RegisterDomainRequest = {
      securityToken: '',
      name,
      description: 'registered by cadence-typescript-client',
      ownerEmail: '',
      workflowExecutionRetentionPeriod: durationFromMs(retentionDaysInDays * 24 * 60 * 60 * 1000),
      clusters: [],
      activeClusterName: '',
      data: {},
      isGlobalDomain: false,
      historyArchivalStatus: 0,
      historyArchivalUri: '',
      visibilityArchivalStatus: 0,
      visibilityArchivalUri: '',
      activeClustersByRegion: {},
      activeClusters: undefined,
    };
    try {
      await this.domainApi.RegisterDomain(request);
    } catch (err) {
      if (!(err instanceof CadenceError && err.code === 'DomainAlreadyExists')) {
        throw err;
      }
    }
  }

  /**
   * Deprecates a domain, preventing new workflow executions while existing
   * executions may continue. Before deprecating, confirm the domain is ready
   * to stop accepting work. If it will be deleted, first verify there are no
   * open workflows or remaining workflow history.
   */
  async deprecateDomain(name: string): Promise<void> {
    const request: DeprecateDomainRequest = { securityToken: '', name };
    await this.domainApi.DeprecateDomain(request);
  }

  /**
   * Permanently deletes a domain. Before calling, confirm it is deprecated,
   * has no open workflows, and has no remaining workflow history as supported
   * by the server. Preconditions are not checked by this client; deletion is
   * irreversible and server behavior may vary by version.
   */
  async deleteDomain(name: string): Promise<void> {
    const request: DeleteDomainRequest = { securityToken: '', name };
    await this.domainApi.DeleteDomain(request);
  }

  async startWorkflow(
    workflowType: string,
    options: StartWorkflowOptions,
    domain: string,
    ...input: unknown[]
  ): Promise<StartedWorkflow> {
    const workflowId = options.id ?? randomUUID();
    const request: StartWorkflowExecutionRequest = {
      domain,
      workflowId,
      workflowType: { name: workflowType },
      taskList: { name: options.taskList, kind: 0, baseName: '' },
      input: this.toPayloads(input),
      executionStartToCloseTimeout: durationFromMs(options.executionStartToCloseTimeoutMs),
      taskStartToCloseTimeout: durationFromMs(options.decisionTaskStartToCloseTimeoutMs ?? 10_000),
      identity: this.identity,
      requestId: options.requestId ?? randomUUID(),
      workflowIdReusePolicy: toReusePolicy(options.workflowIdReusePolicy),
      retryPolicy: options.retryPolicy ? toProtoRetryPolicy(options.retryPolicy) : undefined,
      cronSchedule: options.cronSchedule ?? '',
      memo: toMemo(options.memo, this.converter),
      searchAttributes: undefined,
      header: undefined,
      delayStart: undefined,
      jitterStart: undefined,
      firstRunAt: undefined,
      cronOverlapPolicy: 0,
      activeClusterSelectionPolicy: undefined,
    };
    const response: StartWorkflowExecutionResponse = await this.workflowApi.StartWorkflowExecution(request);
    return { workflowId, runId: response.runId };
  }

  async signalWorkflow(
    domain: string,
    workflowId: string,
    runId: string | undefined,
    signalName: string,
    input?: unknown,
  ): Promise<void> {
    await this.workflowApi.SignalWorkflowExecution({
      domain,
      workflowExecution: toExecution(workflowId, runId),
      identity: this.identity,
      requestId: randomUUID(),
      signalName,
      signalInput: this.toPayloads([input]),
      control: new Uint8Array(),
    });
  }

  async signalWithStartWorkflow(
    domain: string,
    workflowType: string,
    options: StartWorkflowOptions & { workflowId: string },
    signalName: string,
    signalInput: unknown,
    ...workflowInput: unknown[]
  ): Promise<StartedWorkflow> {
    const startRequest: StartWorkflowExecutionRequest = {
      domain,
      workflowId: options.workflowId,
      workflowType: { name: workflowType },
      taskList: { name: options.taskList, kind: 0, baseName: '' },
      input: this.toPayloads(workflowInput),
      executionStartToCloseTimeout: durationFromMs(options.executionStartToCloseTimeoutMs),
      taskStartToCloseTimeout: durationFromMs(options.decisionTaskStartToCloseTimeoutMs ?? 10_000),
      identity: this.identity,
      requestId: options.requestId ?? randomUUID(),
      workflowIdReusePolicy: WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
      retryPolicy: options.retryPolicy ? toProtoRetryPolicy(options.retryPolicy) : undefined,
      cronSchedule: options.cronSchedule ?? '',
      memo: toMemo(options.memo, this.converter),
      searchAttributes: undefined,
      header: undefined,
      delayStart: undefined,
      jitterStart: undefined,
      firstRunAt: undefined,
      cronOverlapPolicy: 0,
      activeClusterSelectionPolicy: undefined,
    };
    const response = await this.workflowApi.SignalWithStartWorkflowExecution({
      startRequest,
      signalName,
      signalInput: this.toPayloads([signalInput]),
      control: new Uint8Array(),
    });
    return { workflowId: options.workflowId, runId: response.runId };
  }

  async queryWorkflow<T = unknown>(
    domain: string,
    workflowId: string,
    runId: string | undefined,
    queryType: string,
  ): Promise<T> {
    const response: QueryWorkflowResponse = await this.workflowApi.QueryWorkflow({
      domain,
      workflowExecution: toExecution(workflowId, runId),
      query: { queryType, queryArgs: undefined },
      queryRejectCondition: 0,
      queryConsistencyLevel: 0,
    });
    if (response.queryRejected) {
      throw new CadenceError('QueryFailed', 'query rejected');
    }
    const value = this.fromPayload<T>(response.queryResult);
    return (Array.isArray(value) && value.length === 1 ? value[0] : value) as T;
  }

  async cancelWorkflow(domain: string, workflowId: string, runId?: string): Promise<void> {
    await this.workflowApi.RequestCancelWorkflowExecution({
      domain,
      workflowExecution: toExecution(workflowId, runId),
      identity: this.identity,
      requestId: randomUUID(),
      cause: '',
      firstExecutionRunId: '',
    });
  }

  async terminateWorkflow(
    domain: string,
    workflowId: string,
    runId: string | undefined,
    reason: string,
  ): Promise<void> {
    await this.workflowApi.TerminateWorkflowExecution({
      domain,
      workflowExecution: toExecution(workflowId, runId),
      reason,
      details: undefined,
      identity: this.identity,
      firstExecutionRunId: '',
    });
  }

  /**
   * Waits until the run closes and resolves with its close status, result, and
   * failure (if any). Rejects with CadenceError('EntityNotExists') when the
   * execution never existed.
   */
  async waitForClose(
    domain: string,
    workflowId: string,
    runId: string | undefined,
    timeoutMs = 60_000,
  ): Promise<WorkflowCloseResult> {
    const deadline = Date.now() + timeoutMs;
    for (;;) {
      const response = await this.getHistoryPage(domain, workflowId, runId, {
        waitForNewEvent: true,
        timeoutMs: Math.max(1000, deadline - Date.now()),
      });
      const events = response.history?.events ?? [];
      const close = events.length > 0 ? events[events.length - 1] : undefined;
      const result = closeResultFromEvent(close, this.converter);
      if (result) return result;
      if (Date.now() >= deadline) {
        throw new CadenceError('Internal', 'timed out waiting for workflow to finish');
      }
    }
  }

  async describeWorkflowExecution(
    domain: string,
    workflowId: string,
    runId: string | undefined,
  ): Promise<WorkflowExecutionDescription> {
    const response: DescribeWorkflowExecutionResponse =
      await this.workflowApi.DescribeWorkflowExecution({
        domain,
        workflowExecution: toExecution(workflowId, runId),
        queryConsistencyLevel: 0,
      });
    const info = response.workflowExecutionInfo;
    const activityAttempts: Record<string, number> = {};
    for (const pending of response.pendingActivities) {
      if (pending.activityId) {
        activityAttempts[pending.activityId] = pending.attempt;
      }
    }
    return {
      status: info?.closeTime
        ? (CLOSE_STATUS_NAMES[info.closeStatus] ?? 'FAILED')
        : 'RUNNING',
      historyLength: Number(info?.historyLength ?? 0),
      activityAttempts,
    };
  }

  async getHistory(domain: string, workflowId: string, runId?: string): Promise<HistoryEventSummary[]> {
    const events: HistoryEventSummary[] = [];
    let nextPageToken: Uint8Array | undefined;
    for (;;) {
      const response = await this.getHistoryPage(domain, workflowId, runId, { nextPageToken });
      for (const event of response.history?.events ?? []) {
        events.push({ eventType: eventTypeOf(event), id: Number(event.eventId) });
      }
      if (!response.nextPageToken || response.nextPageToken.length === 0) break;
      nextPageToken = response.nextPageToken;
    }
    return events;
  }

  async listOpenWorkflowExecutions(
    domain: string,
    opts: { maxResults?: number } = {},
  ): Promise<WorkflowExecutionSummary[]> {
    const request: ListOpenWorkflowExecutionsRequest = {
      domain,
      pageSize: opts.maxResults ?? 100,
      nextPageToken: new Uint8Array(),
      startTimeFilter: {
        earliestTime: timestampFromMs(Date.now() - 24 * 60 * 60 * 1000),
        latestTime: timestampFromMs(Date.now() + 60 * 60 * 1000),
      },
    };
    const response = await this.visibilityApi.ListOpenWorkflowExecutions(request);
    return response.executions.map(toSummary);
  }

  async listClosedWorkflowExecutions(
    domain: string,
    opts: { maxResults?: number; workflowId?: string } = {},
  ): Promise<WorkflowExecutionSummary[]> {
    const request: ListClosedWorkflowExecutionsRequest = {
      domain,
      pageSize: opts.maxResults ?? 100,
      nextPageToken: new Uint8Array(),
      startTimeFilter: {
        earliestTime: timestampFromMs(Date.now() - 24 * 60 * 60 * 1000),
        latestTime: timestampFromMs(Date.now() + 60 * 60 * 1000),
      },
      executionFilter: opts.workflowId ? { workflowId: opts.workflowId, runId: '' } : undefined,
    };
    const response = await this.visibilityApi.ListClosedWorkflowExecutions(request);
    return response.executions.map((info) => toSummary(info));
  }

  private async getHistoryPage(
    domain: string,
    workflowId: string,
    runId: string | undefined,
    opts: { waitForNewEvent?: boolean; timeoutMs?: number; nextPageToken?: Uint8Array } = {},
  ) {
    return this.workflowApi.GetWorkflowExecutionHistory({
      domain,
      workflowExecution: toExecution(workflowId, runId),
      pageSize: 1000,
      nextPageToken: opts.nextPageToken ?? new Uint8Array(),
      waitForNewEvent: opts.waitForNewEvent ?? false,
      historyEventFilterType: EventFilterType.EVENT_FILTER_TYPE_ALL_EVENT,
      skipArchival: true,
      queryConsistencyLevel: 0,
    });
  }

  private toPayloads(values: unknown[]): Payload | undefined {
    if (values.length === 0) return undefined;
    return this.converter.toPayload(values);
  }

  private fromPayload<T>(payload: Payload | undefined): T {
    if (!payload || payload.data.length === 0) return undefined as T;
    return this.converter.fromPayload<T>(payload);
  }
}

function toExecution(workflowId: string, runId: string | undefined): WorkflowExecution {
  return { workflowId, runId: runId ?? '' };
}

function toReusePolicy(name: WorkflowIdReusePolicyName | undefined): WorkflowIdReusePolicy {
  switch (name) {
    case 'AllowDuplicate':
      return WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE;
    case 'RejectDuplicate':
      return WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE;
    case 'TerminateIfRunning':
      return WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING;
    default:
      return WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY;
  }
}

function toProtoRetryPolicy(policy: {
  initialIntervalMs: number;
  backoffCoefficient?: number;
  maximumIntervalMs?: number;
  expirationIntervalMs?: number;
  maximumAttempts?: number;
  nonRetriableErrorReasons?: string[];
}): RetryPolicy {
  // The server validates retry intervals in whole seconds; round up so
  // sub-second intervals stay valid.
  const ceilSeconds = (ms: number) => durationFromMs(Math.max(1000, Math.ceil(ms / 1000) * 1000));
  return {
    initialInterval: ceilSeconds(policy.initialIntervalMs),
    backoffCoefficient: policy.backoffCoefficient ?? 2.0,
    maximumInterval: policy.maximumIntervalMs ? ceilSeconds(policy.maximumIntervalMs) : undefined,
    maximumAttempts: policy.maximumAttempts ?? 0,
    nonRetryableErrorReasons: policy.nonRetriableErrorReasons ?? [],
    expirationInterval: policy.expirationIntervalMs ? ceilSeconds(policy.expirationIntervalMs) : undefined,
  };
}

function toMemo(memo: Record<string, unknown> | undefined, converter: DataConverter) {
  if (!memo) return undefined;
  const fields: Record<string, Payload> = {};
  for (const [key, value] of Object.entries(memo)) {
    fields[key] = converter.toPayload(value);
  }
  return { fields };
}

export function durationFromMs(ms: number): ProtoDuration {
  return { seconds: String(Math.floor(ms / 1000)), nanos: Math.floor((ms % 1000) * 1_000_000) };
}

function timestampFromMs(ms: number): { seconds: string; nanos: number } {
  return { seconds: String(Math.floor(ms / 1000)), nanos: Math.floor((ms % 1000) * 1_000_000) };
}

function timestampToMs(ts: { seconds: string; nanos: number } | undefined): number | undefined {
  if (!ts) return undefined;
  return Number(ts.seconds) * 1000 + Math.floor(ts.nanos / 1_000_000);
}

function eventTypeOf(event: HistoryEvent): string {
  for (const key of Object.keys(event)) {
    if (key.endsWith('EventAttributes') && event[key as keyof HistoryEvent] != null) {
      const stem = key.slice(0, -'EventAttributes'.length);
      return stem.charAt(0).toUpperCase() + stem.slice(1);
    }
  }
  return 'Unknown';
}

function closeResultFromEvent(event: HistoryEvent | undefined, converter: DataConverter): WorkflowCloseResult | undefined {
  if (!event) return undefined;
  if (event.workflowExecutionCompletedEventAttributes) {
    const result = event.workflowExecutionCompletedEventAttributes.result;
    return {
      status: 'COMPLETED',
      result: result && result.data.length > 0 ? decodePayloads(result) : undefined,
    };
  }
  if (event.workflowExecutionFailedEventAttributes) {
    const failure = event.workflowExecutionFailedEventAttributes.failure;
    const reason = failure?.reason ?? '';
    return {
      status: 'FAILED',
      failure: /timeout/i.test(reason)
        ? { type: 'Timeout', message: reason }
        : { type: 'ApplicationError', message: reason, reason },
    };
  }
  if (event.workflowExecutionTimedOutEventAttributes) {
    return { status: 'TIMED_OUT', failure: { type: 'Timeout', message: 'execution timed out' } };
  }
  if (event.workflowExecutionCanceledEventAttributes) {
    return { status: 'CANCELED', failure: { type: 'CanceledError', message: 'workflow canceled' } };
  }
  if (event.workflowExecutionTerminatedEventAttributes) {
    return { status: 'TERMINATED', failure: { type: 'TerminatedError', message: 'workflow terminated' } };
  }
  if (event.workflowExecutionContinuedAsNewEventAttributes) {
    return { status: 'CONTINUED_AS_NEW' };
  }
  return undefined;
}

function decodePayloads(payload: Payload): unknown {
  const values = JSON.parse(new TextDecoder().decode(payload.data));
  return Array.isArray(values) && values.length === 1 ? values[0] : values;
}

function toSummary(info: WorkflowExecutionInfo): WorkflowExecutionSummary {
  const status: WorkflowCloseStatus | 'RUNNING' = info.closeTime
    ? (CLOSE_STATUS_NAMES[info.closeStatus] ?? 'FAILED')
    : 'RUNNING';
  return {
    workflowId: info.workflowExecution?.workflowId ?? '',
    runId: info.workflowExecution?.runId ?? '',
    type: info.type?.name,
    status,
    startTimeMs: timestampToMs(info.startTime),
    closeTimeMs: timestampToMs(info.closeTime),
    memo: memoToPlain(info.memo),
  };
}

function memoToPlain(memo: { fields: Record<string, Payload> } | undefined): Record<string, unknown> | undefined {
  if (!memo?.fields) return undefined;
  const out: Record<string, unknown> = {};
  for (const [key, payload] of Object.entries(memo.fields)) {
    try {
      out[key] = JSON.parse(new TextDecoder().decode(payload.data));
    } catch {
      out[key] = null;
    }
  }
  return out;
}
