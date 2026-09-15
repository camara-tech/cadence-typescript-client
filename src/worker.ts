import type { HistoryEvent } from './generated/uber/cadence/api/v1/history.js';
import type { Decision } from './generated/uber/cadence/api/v1/decision.js';
import {
  PollForActivityTaskRequest,
  PollForDecisionTaskRequest,
  RespondActivityTaskCompletedRequest,
  RespondActivityTaskFailedRequest,
  RespondDecisionTaskCompletedRequest,
  RespondQueryTaskCompletedRequest,
  WorkerAPIClientImpl,
} from './generated/uber/cadence/api/v1/service_worker.js';
import type {
  PollForActivityTaskResponse,
  PollForDecisionTaskResponse,
} from './generated/uber/cadence/api/v1/service_worker.js';
import { QueryResultType } from './generated/uber/cadence/api/v1/query.js';
import { WorkflowAPIClientImpl } from './generated/uber/cadence/api/v1/service_workflow.js';
import { Connection } from './connection.js';
import type { DataConverter } from './data-converter.js';
import {
  chainInterceptorFactories,
  type WorkflowInterceptor,
  type WorkflowInterceptorFactory,
} from './interceptors.js';
import {
  ApplicationError,
  WorkflowReplay,
  type ActivityContext,
  type ActivityFunc,
  type WorkflowFunc,
} from './runtime.js';

export interface WorkerOptions {
  identity?: string;
  interceptors?: WorkflowInterceptorFactory[];
}

/**
 * Hosts workflow and activity implementations, polling the Cadence task list
 * for decision and activity tasks.
 */
export class Worker {
  private readonly api: WorkerAPIClientImpl;
  private readonly identity: string;
  private readonly interceptorChain: WorkflowInterceptor | undefined;
  private readonly workflows = new Map<string, WorkflowFunc>();
  private readonly activities = new Map<string, ActivityFunc>();
  private running = false;
  private readonly loops: Array<Promise<void>> = [];

  constructor(
    private readonly connection: Connection,
    private readonly domain: string,
    private readonly taskList: string,
    private readonly converter: DataConverter,
    opts: WorkerOptions = {},
  ) {
    this.identity = opts.identity ?? connection.identity;
    this.interceptorChain = chainInterceptorFactories(opts.interceptors ?? [])?.create({ workflowId: '', runId: '', workflowType: '' });
    const rpc = {
      request: (service: string, method: string, data: Uint8Array) => connection.request(service, method, data),
    };
    this.api = new WorkerAPIClientImpl(rpc);
  }

  registerWorkflow(name: string, fn: WorkflowFunc): void {
    this.workflows.set(name, fn);
  }

  registerActivity(name: string, fn: ActivityFunc): void {
    this.activities.set(name, fn);
  }

  start(): void {
    if (this.running) return;
    this.running = true;
    this.loops.push(this.decisionLoop());
    this.loops.push(this.activityLoop());
  }

  async stop(): Promise<void> {
    this.running = false;
    await Promise.allSettled(this.loops);
  }

  private async decisionLoop(): Promise<void> {
    while (this.running) {
      let task: PollForDecisionTaskResponse;
      try {
        task = await this.api.PollForDecisionTask({
          domain: this.domain,
          taskList: { name: this.taskList, kind: 0, baseName: '' },
          identity: this.identity,
          binaryChecksum: '',
        } satisfies PollForDecisionTaskRequest);
      } catch {
        continue;
      }
      if (!task.taskToken || task.taskToken.length === 0) continue;
      if (process.env.CADENCE_WORKER_DEBUG) {
        console.log(
          `[worker] decision task: query=${task.query != null} historyEvents=${task.history?.events?.length ?? 0} wfType=${task.workflowType?.name}`,
        );
      }
      try {
        if (task.query) {
          await this.processQueryOnlyTask(task);
        } else {
          await this.processDecisionTask(task);
        }
      } catch (err) {
        if (process.env.CADENCE_WORKER_DEBUG) {
          console.log('[worker] task failed:', (err as Error).message);
        }
        await this.failDecisionTask(task.taskToken, err);
      }
    }
  }

  private async activityLoop(): Promise<void> {
    while (this.running) {
      let task: PollForActivityTaskResponse;
      try {
        task = await this.api.PollForActivityTask({
          domain: this.domain,
          taskList: { name: this.taskList, kind: 0, baseName: '' },
          identity: this.identity,
          taskListMetadata: undefined,
        } satisfies PollForActivityTaskRequest);
      } catch {
        continue;
      }
      if (!task.taskToken || task.taskToken.length === 0) continue;
      void this.processActivityTask(task);
    }
  }

  /**
   * Answers a query task by replaying the execution's history and evaluating
   * the query against the replayed state. Query tasks never carry decisions.
   */
  private async processQueryOnlyTask(task: PollForDecisionTaskResponse): Promise<void> {
    if (process.env.CADENCE_WORKER_DEBUG) console.log('[worker] query task: start');
    const execution = task.workflowExecution!;
    const events = task.history?.events?.length
      ? await this.fetchFullHistory(task)
      : await this.getHistoryForExecution(execution);
    if (process.env.CADENCE_WORKER_DEBUG) console.log('[worker] query task: history fetched', events.length);
    const started = events.find((e) => e.workflowExecutionStartedEventAttributes != null);
    const workflowType =
      task.workflowType?.name ??
      started?.workflowExecutionStartedEventAttributes?.workflowType?.name ??
      '';
    const input = started?.workflowExecutionStartedEventAttributes?.input
      ? this.converter.fromPayload<unknown[]>(started.workflowExecutionStartedEventAttributes.input)
      : [];
    const replay = new WorkflowReplay(
      events,
      this.converter,
      { workflows: this.workflows, activities: this.activities },
      this.taskList,
      this.domain,
      // Queries must not double-count interceptor invocations.
      undefined,
      { workflowId: execution.workflowId, runId: execution.runId },
    );
    await replay.run(workflowType, input ?? []);
    if (process.env.CADENCE_WORKER_DEBUG) console.log('[worker] query task: replay done');
    const queryResult = replay.answerQueries({ q: task.query! }).q;
    if (process.env.CADENCE_WORKER_DEBUG) console.log('[worker] query task: answering', JSON.stringify(queryResult));
    await this.api.RespondQueryTaskCompleted({
      taskToken: task.taskToken,
      result: queryResult,
      workerVersionInfo: undefined,
    } satisfies RespondQueryTaskCompletedRequest);
    if (process.env.CADENCE_WORKER_DEBUG) console.log('[worker] query task: responded');
  }

  private async processDecisionTask(task: PollForDecisionTaskResponse): Promise<void> {
    const events = await this.fetchFullHistory(task);
    const workflowType = task.workflowType?.name ?? '';
    const execution = task.workflowExecution!;
    const replay = new WorkflowReplay(
      events,
      this.converter,
      { workflows: this.workflows, activities: this.activities },
      task.workflowExecutionTaskList?.name ?? this.taskList,
      this.domain,
      this.interceptorChain,
      { workflowId: execution.workflowId, runId: execution.runId },
    );
    const started = events.find((e) => e.workflowExecutionStartedEventAttributes != null);
    const input = started?.workflowExecutionStartedEventAttributes?.input
      ? this.converter.fromPayload<unknown[]>(started.workflowExecutionStartedEventAttributes.input)
      : [];
    await replay.run(workflowType, input ?? []);
    replay.cancelIfRequested();
    const queryResults = replay.answerQueries(task.queries ?? {});
    await this.api.RespondDecisionTaskCompleted({
      taskToken: task.taskToken,
      decisions: replay.decisionsSoFar,
      executionContext: new Uint8Array(),
      identity: this.identity,
      stickyAttributes: undefined,
      returnNewDecisionTask: false,
      forceCreateNewDecisionTask: false,
      binaryChecksum: '',
      queryResults,
    } satisfies RespondDecisionTaskCompletedRequest);
  }

  private async fetchFullHistory(task: PollForDecisionTaskResponse): Promise<HistoryEvent[]> {
    const events = [...(task.history?.events ?? [])];
    let nextPageToken = task.nextPageToken;
    while (nextPageToken && nextPageToken.length > 0) {
      const response = await this.getHistoryPage(task.workflowExecution!, nextPageToken);
      events.push(...(response.history?.events ?? []));
      nextPageToken = response.nextPageToken;
    }
    return events;
  }

  /** Fetches the complete history of an execution, following all pages. */
  private async getHistoryForExecution(execution: NonNullable<PollForDecisionTaskResponse['workflowExecution']>): Promise<HistoryEvent[]> {
    const events: HistoryEvent[] = [];
    let nextPageToken: Uint8Array | undefined;
    for (;;) {
      const response = await this.getHistoryPage(execution, nextPageToken ?? new Uint8Array());
      events.push(...(response.history?.events ?? []));
      if (!response.nextPageToken || response.nextPageToken.length === 0) break;
      nextPageToken = response.nextPageToken;
    }
    return events;
  }

  private async getHistoryPage(
    execution: NonNullable<PollForDecisionTaskResponse['workflowExecution']>,
    nextPageToken: Uint8Array,
  ) {
    const rpc = {
      request: (service: string, method: string, data: Uint8Array) =>
        this.connection.request(service, method, data),
    };
    const workflowApi = new (await import('./generated/uber/cadence/api/v1/service_workflow.js')).WorkflowAPIClientImpl(rpc);
    return workflowApi.GetWorkflowExecutionHistory({
      domain: this.domain,
      workflowExecution: execution,
      pageSize: 1000,
      nextPageToken,
      waitForNewEvent: false,
      historyEventFilterType: 1,
      skipArchival: true,
      queryConsistencyLevel: 0,
    });
  }

  private async failDecisionTask(taskToken: Uint8Array, err: unknown): Promise<void> {
    try {
      await this.api.RespondDecisionTaskFailed({
        taskToken,
        cause: 0,
        details: undefined,
        identity: this.identity,
        binaryChecksum: '',
      });
    } catch {
      // the task may have already timed out; the server will retry
    }
    void err;
  }

  private async processActivityTask(task: PollForActivityTaskResponse): Promise<void> {
    const activityType = task.activityType?.name ?? '';
    const fn = this.activities.get(activityType);
    const input = task.input ? this.converter.fromPayload<unknown[]>(task.input) : [];
    const ctx: ActivityContext = {
      info: {
        activityId: task.activityId,
        workflowId: task.workflowExecution?.workflowId ?? '',
        runId: task.workflowExecution?.runId ?? '',
        attempt: task.attempt,
      },
      heartbeat: async (details?: unknown) => {
        await this.api.RecordActivityTaskHeartbeat({
          taskToken: task.taskToken,
          details: details === undefined ? undefined : this.converter.toPayload([details]),
          identity: this.identity,
        });
      },
    };
    try {
      if (!fn) {
        throw new Error(`unknown activity type: ${activityType}`);
      }
      const result = await fn(input[0], ctx);
      await this.api.RespondActivityTaskCompleted({
        taskToken: task.taskToken,
        result: this.converter.toPayload([result]),
        identity: this.identity,
      } satisfies RespondActivityTaskCompletedRequest);
    } catch (err) {
      await this.api.RespondActivityTaskFailed({
        taskToken: task.taskToken,
        failure: { reason: activityFailureReason(err), details: new Uint8Array(), options: undefined },
        identity: this.identity,
        heartbeatDetails: undefined,
      } satisfies RespondActivityTaskFailedRequest);
    }
  }
}

function activityFailureReason(err: unknown): string {
  if (err instanceof Error && 'reason' in err && typeof err.reason === 'string') {
    return err.reason;
  }
  return 'cadenceInternal:Generic';
}
