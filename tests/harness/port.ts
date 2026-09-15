/**
 * CadenceTestDriver — the port the GWT black-box suite tests against.
 *
 * Implementations:
 *  - GoDriver (tests/harness/go-driver): HTTP bridge → cadence-go-client → real server.
 *    Used to validate that the suite encodes the reference client's contract.
 *  - TsDriver (tests/harness/ts-driver): in-process adapter over this repo's
 *    TypeScript client. The red-green TDD target.
 */

// ---- Value objects ----------------------------------------------------------

export type WorkflowIdReusePolicy =
  | 'AllowDuplicateFailedOnly'
  | 'AllowDuplicate'
  | 'RejectDuplicate'
  | 'TerminateIfRunning';

export interface RetryPolicy {
  initialIntervalMs: number;
  backoffCoefficient?: number;
  maximumIntervalMs?: number;
  expirationIntervalMs?: number;
  maximumAttempts?: number;
  nonRetriableErrorReasons?: string[];
}

export interface StartWorkflowRequest {
  domain: string;
  workflowType: string;
  taskList: string;
  input?: unknown[];
  executionStartToCloseTimeoutMs: number;
  decisionTaskStartToCloseTimeoutMs?: number;
  workflowId?: string;
  workflowIdReusePolicy?: WorkflowIdReusePolicy;
  retryPolicy?: RetryPolicy;
  cronSchedule?: string;
  memo?: Record<string, unknown>;
  requestId?: string;
}

export interface StartedWorkflow {
  workflowId: string;
  runId: string;
}

export interface SignalWorkflowRequest {
  domain: string;
  workflowId: string;
  runId?: string;
  signalName: string;
  input?: unknown;
}

export interface SignalWithStartWorkflowRequest {
  domain: string;
  workflowId: string;
  signalName: string;
  signalInput?: unknown;
  workflowType: string;
  taskList: string;
  input?: unknown[];
  executionStartToCloseTimeoutMs: number;
}

export interface QueryWorkflowRequest {
  domain: string;
  workflowId: string;
  runId?: string;
  queryType: string;
}

export interface CancelWorkflowRequest {
  domain: string;
  workflowId: string;
  runId?: string;
}

export interface TerminateWorkflowRequest {
  domain: string;
  workflowId: string;
  runId?: string;
  reason?: string;
}

export interface WaitForCloseRequest {
  domain: string;
  workflowId: string;
  runId?: string;
  timeoutMs?: number;
}

export interface DescribeWorkflowRequest {
  domain: string;
  workflowId: string;
  runId?: string;
}

export interface HistoryRequest {
  domain: string;
  workflowId: string;
  runId?: string;
}

export interface ListRequest {
  domain: string;
  maxResults?: number;
  workflowId?: string;
}

// ---- Results ----------------------------------------------------------------

export type WorkflowCloseStatus =
  | 'COMPLETED'
  | 'FAILED'
  | 'CANCELED'
  | 'TERMINATED'
  | 'TIMED_OUT'
  | 'CONTINUED_AS_NEW';

export interface WorkflowFailure {
  type: string; // 'ApplicationError' | 'CanceledError' | 'Timeout' | ...
  message: string;
  reason?: string;
}

export interface WorkflowResult {
  status: WorkflowCloseStatus;
  result?: unknown;
  failure?: WorkflowFailure;
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

export interface HistoryEventSummary {
  eventType: string;
  id: number;
  details?: Record<string, unknown>;
}

export interface WorkflowExecutionDescription {
  status: 'RUNNING' | WorkflowCloseStatus;
  historyLength: number;
  activityAttempts: Record<string, number>;
}

export interface InterceptorStats {
  workflowStarts: number;
  activityStarts: number;
}

// ---- Errors -----------------------------------------------------------------

export type DriverErrorCode =
  | 'WorkflowExecutionAlreadyStarted'
  | 'EntityNotExists'
  | 'DomainAlreadyExists'
  | 'BadRequest'
  | 'QueryFailed'
  | 'Internal';

/** Structured error so tests can assert on failure reasons, not message text. */
export class DriverError extends Error {
  constructor(
    public readonly code: DriverErrorCode,
    message: string,
    public readonly details?: Record<string, unknown>,
  ) {
    super(message);
    this.name = 'DriverError';
  }
}

// ---- Activity context ---------------------------------------------------------

export interface ActivityInfo {
  activityId: string;
  workflowId: string;
  runId: string;
  attempt: number;
}

export interface ActivityContext {
  info: ActivityInfo;
  heartbeat(details?: unknown): void;
}

// ---- Behavior catalog ---------------------------------------------------------

/**
 * Named workflow/activity behaviors each adapter implements natively.
 * Tests reference behaviors by name; inputs/outputs are JSON values.
 */
export interface BehaviorCatalog {
  workflows: Record<string, (input: unknown) => Promise<unknown>>;
  activities: Record<string, (input: unknown, ctx: ActivityContext) => Promise<unknown>>;
}

// ---- The port -----------------------------------------------------------------

export interface CadenceTestDriver {
  /** Idempotent: register domain (no-op if it already exists). */
  ensureDomain(name: string, retentionDays?: number): Promise<void>;

  startWorkflow(req: StartWorkflowRequest): Promise<StartedWorkflow>;

  signalWorkflow(req: SignalWorkflowRequest): Promise<void>;

  signalWithStartWorkflow(req: SignalWithStartWorkflowRequest): Promise<StartedWorkflow>;

  queryWorkflow(req: QueryWorkflowRequest): Promise<unknown>;

  cancelWorkflow(req: CancelWorkflowRequest): Promise<void>;

  terminateWorkflow(req: TerminateWorkflowRequest): Promise<void>;

  /** Resolves once the run closes; rejects DriverError('EntityNotExists') if unknown. */
  waitForClose(req: WaitForCloseRequest): Promise<WorkflowResult>;

  describeWorkflowExecution(req: DescribeWorkflowRequest): Promise<WorkflowExecutionDescription>;

  getHistory(req: HistoryRequest): Promise<HistoryEventSummary[]>;

  listOpenWorkflowExecutions(req: ListRequest): Promise<WorkflowExecutionSummary[]>;

  listClosedWorkflowExecutions(req: ListRequest): Promise<WorkflowExecutionSummary[]>;

  /** Counters from adapter-level interceptors (workflow/activity interception). */
  interceptorStats(): Promise<InterceptorStats>;

  /** Optional lifecycle hooks (driver-specific). */
  init?(): Promise<void>;
  close?(): Promise<void>;
}

export interface StartWorkflowRequest {
  domain: string;
  workflowType: string;
  taskList: string;
  input?: unknown[];
  executionStartToCloseTimeoutMs: number;
  decisionTaskStartToCloseTimeoutMs?: number;
  workflowId?: string;
  workflowIdReusePolicy?: WorkflowIdReusePolicy;
  retryPolicy?: RetryPolicy;
  cronSchedule?: string;
  memo?: Record<string, unknown>;
  requestId?: string;
}
