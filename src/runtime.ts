import type { Payload } from './generated/uber/cadence/api/v1/common.js';
import type {
  Decision,
  ScheduleActivityTaskDecisionAttributes,
  StartChildWorkflowExecutionDecisionAttributes,
} from './generated/uber/cadence/api/v1/decision.js';
import type { HistoryEvent } from './generated/uber/cadence/api/v1/history.js';
import type { WorkflowQuery, WorkflowQueryResult } from './generated/uber/cadence/api/v1/query.js';
import { QueryResultType } from './generated/uber/cadence/api/v1/query.js';
import { ContinueAsNewInitiator, ParentClosePolicy } from './generated/uber/cadence/api/v1/workflow.js';
import type { DataConverter } from './data-converter.js';
import { durationFromMs } from './client.js';
import type { WorkflowInterceptor } from './interceptors.js';

/** Error thrown by workflow/activity behaviors with a retryable reason. */
export class ApplicationError extends Error {
  constructor(
    public readonly reason: string,
    message?: string,
  ) {
    super(message ?? reason);
    this.name = 'ApplicationError';
  }
}

/** Thrown to continue a workflow as new. */
export class ContinueAsNewSignal extends Error {
  constructor(public readonly input: unknown[]) {
    super('continue as new');
    this.name = 'ContinueAsNewSignal';
  }
}

/** Behavior surface handed to workflow functions. Deterministic: every call
 * either resolves from replayed history or suspends until a future decision
 * task observes the completing event. */
export interface WorkflowContext {
  sleep(ms: number): Promise<void>;
  runActivity<T = unknown>(name: string, input?: unknown, opts?: ActivityOptions): Promise<T>;
  runChildWorkflow<T = unknown>(name: string, input?: unknown): Promise<T>;
  waitForAnySignal(...names: string[]): Promise<{ name: string; input: unknown }>;
  setQueryHandler(name: string, handler: (input: unknown) => unknown): void;
  isReplaying(): boolean;
}

export interface ActivityOptions {
  taskList?: string;
  scheduleToCloseTimeoutMs?: number;
  startToCloseTimeoutMs?: number;
  heartbeatTimeoutMs?: number;
  retryPolicy?: {
    initialIntervalMs: number;
    backoffCoefficient?: number;
    maximumIntervalMs?: number;
    expirationIntervalMs?: number;
    maximumAttempts?: number;
    nonRetriableErrorReasons?: string[];
  };
}

export interface ActivityContext {
  info: { activityId: string; workflowId: string; runId: string; attempt: number };
  heartbeat(details?: unknown): void;
}

export type WorkflowFunc = (ctx: WorkflowContext, ...args: unknown[]) => Promise<unknown>;
export type ActivityFunc = (input: unknown, ctx: ActivityContext) => Promise<unknown>;

export interface Registries {
  workflows: Map<string, WorkflowFunc>;
  activities: Map<string, ActivityFunc>;
}

interface ActivityRecord {
  scheduledEventId: string;
  activityId: string;
  activityType: string;
  input: Payload | undefined;
  completed?: { result: Payload | undefined };
  failed?: { reason: string };
  timedOut?: boolean;
}

interface ChildRecord {
  initiatedEventId: string;
  workflowId: string;
  workflowType: string;
  input: Payload | undefined;
  completed?: { result: Payload | undefined };
  failed?: { reason: string };
}

export interface ReplayOutcome {
  decisions: Decision[];
  queryResults: Record<string, WorkflowQueryResult>;
  finished: boolean;
}

const never = (): Promise<never> => new Promise(() => undefined);

/**
 * Replays a workflow's event history to drive its coroutine deterministically,
 * collecting the decisions needed to make forward progress.
 */
export class WorkflowReplay {
  private readonly decisions: Decision[] = [];
  private activitySeq = 0;
  private timerSeq = 0;
  private childSeq = 0;
  private signalCursor = 0;
  private resolvedCount = 0;
  private readonly queryHandlers = new Map<string, (input: unknown) => unknown>();
  private readonly activities = new Map<string, ActivityRecord>();
  private readonly children = new Map<string, ChildRecord>();
  private readonly timers = new Map<string, { fired: boolean }>();
  private readonly signals: Array<{ name: string; input: unknown }> = [];
  private completed = false;
  private failed = false;
  private continuedAsNew = false;
  private canceledRequested = false;

  constructor(
    private readonly events: HistoryEvent[],
    private readonly converter: DataConverter,
    private readonly registries: Registries,
    private readonly taskList: string,
    private readonly domain: string,
    private readonly interceptor?: WorkflowInterceptor,
    private readonly execution?: { workflowId: string; runId: string },
  ) {
    this.indexHistory();
  }

  private indexHistory(): void {
    for (const event of this.events) {
      const scheduled = event.activityTaskScheduledEventAttributes;
      if (scheduled) {
        this.activities.set(scheduled.activityId, {
          scheduledEventId: event.eventId,
          activityId: scheduled.activityId,
          activityType: scheduled.activityType?.name ?? '',
          input: scheduled.input,
        });
        continue;
      }
      const completed = event.activityTaskCompletedEventAttributes;
      if (completed) {
        const record = this.findByScheduledEventId(this.activities, completed.scheduledEventId);
        if (record) record.completed = { result: completed.result };
        continue;
      }
      const failed = event.activityTaskFailedEventAttributes;
      if (failed) {
        const record = this.findByScheduledEventId(this.activities, failed.scheduledEventId);
        if (record) record.failed = { reason: failed.failure?.reason ?? '' };
        continue;
      }
      const timedOut = event.activityTaskTimedOutEventAttributes;
      if (timedOut) {
        const record = this.findByScheduledEventId(this.activities, timedOut.scheduledEventId);
        if (record) record.timedOut = true;
        continue;
      }
      const timerStarted = event.timerStartedEventAttributes;
      if (timerStarted) {
        this.timers.set(timerStarted.timerId, { fired: false });
        continue;
      }
      const timerFired = event.timerFiredEventAttributes;
      if (timerFired) {
        const timer = this.timers.get(timerFired.timerId);
        if (timer) timer.fired = true;
        continue;
      }
      const signaled = event.workflowExecutionSignaledEventAttributes;
      if (signaled) {
        this.signals.push({
          name: signaled.signalName,
          input: signaled.input ? this.converter.fromPayload(signaled.input) : undefined,
        });
        continue;
      }
      const childInitiated = event.startChildWorkflowExecutionInitiatedEventAttributes;
      if (childInitiated) {
        this.children.set(childInitiated.workflowId, {
          initiatedEventId: event.eventId,
          workflowId: childInitiated.workflowId,
          workflowType: childInitiated.workflowType?.name ?? '',
          input: childInitiated.input,
        });
        continue;
      }
      const childCompleted = event.childWorkflowExecutionCompletedEventAttributes;
      if (childCompleted) {
        const record = this.children.get(childCompleted.workflowExecution?.workflowId ?? '');
        if (record) record.completed = { result: childCompleted.result };
        continue;
      }
      const childFailed = event.childWorkflowExecutionFailedEventAttributes;
      if (childFailed) {
        const record = this.children.get(childFailed.workflowExecution?.workflowId ?? '');
        if (record) record.failed = { reason: childFailed.failure?.reason ?? '' };
        continue;
      }
      if (event.workflowExecutionCompletedEventAttributes) this.completed = true;
      if (event.workflowExecutionFailedEventAttributes) this.failed = true;
      if (event.workflowExecutionContinuedAsNewEventAttributes) this.continuedAsNew = true;
      if (event.workflowExecutionCancelRequestedEventAttributes) this.canceledRequested = true;
    }
  }

  private findByScheduledEventId(
    map: Map<string, ActivityRecord>,
    scheduledEventId: string,
  ): ActivityRecord | undefined {
    for (const record of map.values()) {
      if (record.scheduledEventId === scheduledEventId) return record;
    }
    return undefined;
  }

  /** Drives the workflow coroutine until it finishes or stalls. */
  async run(workflowType: string, input: unknown[]): Promise<void> {
    const fn = this.registries.workflows.get(workflowType);
    if (!fn) {
      this.decisions.push({
        failWorkflowExecutionDecisionAttributes: {
          failure: { reason: `unknown workflow type: ${workflowType}`, details: new Uint8Array(), options: undefined },
        },
      });
      return;
    }
    const ctx = this.makeContext();
    let settled = false;
    let returned: unknown;
    let thrown: unknown;
    // A fresh (non-replayed) workflow invocation: the history contains no
    // completed decisions yet.
    const replaying = this.hasPriorDecisionCompletion();
    if (this.interceptor && !replaying) {
      this.interceptor.onWorkflowStarted({
        workflowType,
        workflowId: this.execution?.workflowId ?? '',
        runId: this.execution?.runId ?? '',
      });
    }
    fn(ctx, ...input).then(
      (value) => {
        settled = true;
        returned = value;
      },
      (err) => {
        settled = true;
        thrown = err;
      },
    );
    await this.settle();
    if (process.env.CADENCE_WORKER_DEBUG) {
      console.log(
        `[runtime] settled=${settled} thrown=${thrown ? (thrown as Error).message : undefined} decisions=${this.decisions.length} failedFlag=${this.failed}`,
      );
    }
    if (!settled) return;
    if (thrown !== undefined) {
      if (thrown instanceof ContinueAsNewSignal) {
        if (!this.continuedAsNew) {
          this.decisions.push({
            continueAsNewWorkflowExecutionDecisionAttributes: {
              workflowType: { name: workflowType },
              taskList: { name: this.taskList, kind: 0, baseName: '' },
              input: this.toPayloads(thrown.input),
              executionStartToCloseTimeout: durationFromMs(60_000),
              taskStartToCloseTimeout: durationFromMs(10_000),
              backoffStartInterval: durationFromMs(0),
              initiator: ContinueAsNewInitiator.CONTINUE_AS_NEW_INITIATOR_DECIDER,
              cronSchedule: '',
              jitterStart: undefined,
              cronOverlapPolicy: 0,
              activeClusterSelectionPolicy: undefined,
              header: undefined,
              memo: undefined,
              searchAttributes: undefined,
              retryPolicy: undefined,
              failure: undefined,
              lastCompletionResult: undefined,
            },
          });
        }
        return;
      }
      if (!this.failed) {
        this.decisions.push({
          failWorkflowExecutionDecisionAttributes: {
            failure: { reason: failureReasonOf(thrown), details: new Uint8Array(), options: undefined },
          },
        });
      }
      return;
    }
    if (!this.completed) {
      this.decisions.push({
        completeWorkflowExecutionDecisionAttributes: { result: this.toPayloads([returned]) },
      });
    }
  }

  /** True when the history shows a prior decision task completing, i.e. the
   * current pass is a replay rather than a fresh execution. */
  private hasPriorDecisionCompletion(): boolean {
    return this.events.some((event) => event.decisionTaskCompletedEventAttributes != null);
  }

  /** Yields until the coroutine stops making progress. */
  private async settle(): Promise<void> {
    for (;;) {
      const before = this.resolvedCount;
      await new Promise((resolve) => setImmediate(resolve));
      if (this.resolvedCount === before) break;
    }
  }

  private makeContext(): WorkflowContext {
    const replay = this;
    return {
      sleep: (ms: number) => replay.sleep(ms),
      runActivity: <T,>(name: string, input?: unknown, opts?: ActivityOptions) =>
        replay.runActivity<T>(name, input, opts),
      runChildWorkflow: <T,>(name: string, input?: unknown) => replay.runChildWorkflow<T>(name, input),
      waitForAnySignal: async (...names: string[]) => replay.waitForAnySignal(names),
      setQueryHandler: (name: string, handler: (input: unknown) => unknown) => {
        replay.queryHandlers.set(name, handler);
      },
      isReplaying: () => true,
    };
  }

  private sleep(ms: number): Promise<void> {
    const timerId = String(this.timerSeq++);
    const timer = this.timers.get(timerId);
    if (timer?.fired) {
      this.resolvedCount++;
      return Promise.resolve();
    }
    if (!timer) {
      this.decisions.push({
        startTimerDecisionAttributes: {
          timerId,
          startToFireTimeout: durationFromMs(ms),
        },
      });
    }
    return never();
  }

  private runActivity<T>(name: string, input: unknown, opts: ActivityOptions | undefined): Promise<T> {
    const activityId = String(this.activitySeq++);
    const record = this.activities.get(activityId);
    if (process.env.CADENCE_WORKER_DEBUG) {
      console.log(`[runtime] runActivity ${name} id=${activityId} found=${record != null} failed=${record?.failed?.reason} completed=${record?.completed != null}`);
    }
    if (!record) {
      if (this.interceptor) {
        this.interceptor.onActivityScheduled({
          workflowType: '',
          workflowId: this.execution?.workflowId ?? '',
          runId: this.execution?.runId ?? '',
          activityType: name,
          activityId,
        });
      }
      this.decisions.push({
        scheduleActivityTaskDecisionAttributes: {
          activityId,
          activityType: { name },
          domain: this.domain,
          taskList: { name: opts?.taskList ?? this.taskList, kind: 0, baseName: '' },
          input: this.toPayloads([input]),
          scheduleToCloseTimeout: durationFromMs(opts?.scheduleToCloseTimeoutMs ?? 60_000),
          scheduleToStartTimeout: durationFromMs(60_000),
          startToCloseTimeout: durationFromMs(opts?.startToCloseTimeoutMs ?? 60_000),
          heartbeatTimeout: opts?.heartbeatTimeoutMs ? durationFromMs(opts.heartbeatTimeoutMs) : undefined,
          retryPolicy: toRetryPolicy(opts?.retryPolicy),
          header: undefined,
          requestLocalDispatch: false,
        },
      });
      return never();
    }
    if (record.completed) {
      this.resolvedCount++;
      const values = record.completed.result
        ? this.converter.fromPayload<unknown[]>(record.completed.result)
        : [];
      const result = Array.isArray(values) && values.length === 1 ? values[0] : values;
      return Promise.resolve(result as T);
    }
    if (record.failed) {
      this.resolvedCount++;
      return Promise.reject(new ApplicationError(record.failed.reason));
    }
    if (record.timedOut) {
      this.resolvedCount++;
      return Promise.reject(
        new ApplicationError('cadenceInternal:Timeout SCHEDULE_TO_CLOSE', 'activity timed out'),
      );
    }
    return never();
  }

  private runChildWorkflow<T>(name: string, input: unknown): Promise<T> {
    const childWorkflowId = String(this.childSeq++);
    const record = this.children.get(childWorkflowId);
    if (!record) {
      this.decisions.push({
        startChildWorkflowExecutionDecisionAttributes: {
          domain: this.domain,
          workflowId: childWorkflowId,
          workflowType: { name },
          taskList: { name: this.taskList, kind: 0, baseName: '' },
          input: this.toPayloads([input]),
          executionStartToCloseTimeout: durationFromMs(60_000),
          taskStartToCloseTimeout: durationFromMs(10_000),
          parentClosePolicy: ParentClosePolicy.PARENT_CLOSE_POLICY_TERMINATE,
          control: new Uint8Array(),
          workflowIdReusePolicy: 1,
          retryPolicy: undefined,
          cronSchedule: '',
          header: undefined,
          memo: undefined,
          searchAttributes: undefined,
          cronOverlapPolicy: 0,
          activeClusterSelectionPolicy: undefined,
        },
      });
      return never();
    }
    if (record.completed) {
      this.resolvedCount++;
      const values = record.completed.result
        ? this.converter.fromPayload<unknown[]>(record.completed.result)
        : [];
      const result = Array.isArray(values) && values.length === 1 ? values[0] : values;
      return Promise.resolve(result as T);
    }
    if (record.failed) {
      this.resolvedCount++;
      return Promise.reject(new ApplicationError(record.failed.reason));
    }
    return never();
  }

  private async waitForAnySignal(names: string[]): Promise<{ name: string; input: unknown }> {
    while (this.signalCursor < this.signals.length) {
      const signal = this.signals[this.signalCursor++];
      if (names.includes(signal.name)) {
        this.resolvedCount++;
        return signal;
      }
    }
    return never();
  }

  private toPayloads(values: unknown[]) {
    return this.converter.toPayload(values);
  }

  answerQueries(queries: Record<string, WorkflowQuery>): Record<string, WorkflowQueryResult> {
    const results: Record<string, WorkflowQueryResult> = {};
    for (const [key, query] of Object.entries(queries)) {
      const handler = this.queryHandlers.get(query.queryType);
      if (!handler) {
        results[key] = {
          resultType: QueryResultType.QUERY_RESULT_TYPE_FAILED,
          answer: undefined,
          errorMessage: `unknown query type: ${query.queryType}`,
        };
        continue;
      }
      try {
        const value = handler(query.queryArgs ? this.converter.fromPayload(query.queryArgs) : undefined);
        results[key] = {
          resultType: QueryResultType.QUERY_RESULT_TYPE_ANSWERED,
          answer: this.toPayloads([value]),
          errorMessage: '',
        };
      } catch (err) {
        results[key] = {
          resultType: QueryResultType.QUERY_RESULT_TYPE_FAILED,
          answer: undefined,
          errorMessage: String((err as Error)?.message ?? err),
        };
      }
    }
    return results;
  }

  get decisionsSoFar(): Decision[] {
    return this.decisions;
  }

  get isCompleted(): boolean {
    return this.completed;
  }

  get isCancelRequested(): boolean {
    return this.canceledRequested;
  }

  /** Issues a cancel decision when the workflow was canceled but is stalled. */
  cancelIfRequested(): void {
    if (this.canceledRequested && !this.completed && !this.failed && !this.continuedAsNew) {
      this.decisions.push({ cancelWorkflowExecutionDecisionAttributes: { details: undefined } });
    }
  }
}

type StartActivityAttrs = StartActivityAttrsAlias;
interface StartActivityAttrsAlias {
  activityId: string;
  activityType: { name: string } | undefined;
  domain: string;
  taskList: { name: string; kind: number; baseName: string };
  input: Payload | undefined;
  scheduleToCloseTimeout: { seconds: string; nanos: number } | undefined;
  scheduleToStartTimeout: { seconds: string; nanos: number } | undefined;
  startToCloseTimeout: { seconds: string; nanos: number } | undefined;
  heartbeatTimeout: { seconds: string; nanos: number } | undefined;
  retryPolicy: unknown;
  header: undefined;
  decisionTaskCompletedEventId: string;
}

type StartChildAttrs = StartChildAttrsAlias;
interface StartChildAttrsAlias {
  domain: string;
  workflowId: string;
  workflowType: { name: string } | undefined;
  taskList: { name: string; kind: number; baseName: string };
  input: Payload | undefined;
  executionStartToCloseTimeout: { seconds: string; nanos: number } | undefined;
  taskStartToCloseTimeout: { seconds: string; nanos: number } | undefined;
  parentClosePolicy: ParentClosePolicy;
  control: Uint8Array;
  workflowIdReusePolicy: number;
  retryPolicy: unknown;
  cronSchedule: string;
  header: undefined;
  memo: undefined;
  searchAttributes: undefined;
}

function failureReasonOf(err: unknown): string {
  if (err instanceof ApplicationError) return err.reason;
  if (err instanceof ContinueAsNewSignal) return 'cadenceInternal:Generic';
  return 'cadenceInternal:Generic';
}

function toRetryPolicy(policy?: {
  initialIntervalMs: number;
  backoffCoefficient?: number;
  maximumIntervalMs?: number;
  expirationIntervalMs?: number;
  maximumAttempts?: number;
  nonRetriableErrorReasons?: string[];
}): import('./generated/uber/cadence/api/v1/common.js').RetryPolicy | undefined {
  if (!policy) return undefined;
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
