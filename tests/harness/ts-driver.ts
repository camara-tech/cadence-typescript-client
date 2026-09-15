import { WorkflowClient } from '../../src/client.js';
import { Connection } from '../../src/connection.js';
import { JsonDataConverter } from '../../src/data-converter.js';
import { Worker } from '../../src/worker.js';
import type { WorkflowInterceptorFactory } from '../../src/interceptors.js';
import { ApplicationError, ContinueAsNewSignal } from '../../src/runtime.js';
import type {
  ActivityContext,
  ActivityOptions,
  WorkflowContext,
} from '../../src/runtime.js';
import type {
  CadenceTestDriver,
  CancelWorkflowRequest,
  DescribeWorkflowRequest,
  HistoryEventSummary,
  HistoryRequest,
  InterceptorStats,
  ListRequest,
  QueryWorkflowRequest,
  SignalWithStartWorkflowRequest,
  SignalWorkflowRequest,
  StartWorkflowRequest,
  StartedWorkflow,
  TerminateWorkflowRequest,
  WaitForCloseRequest,
  WorkflowCloseStatus,
  WorkflowExecutionDescription,
  WorkflowResult,
  WorkflowExecutionSummary,
} from './port.js';

/**
 * In-process adapter over this repo's TypeScript client: the red-green TDD
 * target for the GWT suite.
 */
export class TsDriver implements CadenceTestDriver {
  private readonly connection = Connection.connect({ address: '127.0.0.1:7833' });
  private readonly client = new WorkflowClient(this.connection, {
    dataConverter: new JsonDataConverter(),
  });
  private readonly workers = new Map<string, Worker>();
  private readonly stats = { workflowStarts: 0, activityStarts: 0 };
  private readonly interceptorFactory: WorkflowInterceptorFactory = {
    create: () => ({
      onWorkflowStarted: () => {
        this.stats.workflowStarts++;
      },
      onActivityScheduled: () => {
        this.stats.activityStarts++;
      },
    }),
  };

  async ensureDomain(name: string, retentionDays = 1): Promise<void> {
    await this.client.ensureDomain(name, retentionDays);
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartedWorkflow> {
    this.ensureWorker(req.domain, req.taskList);
    const result = await this.client.startWorkflow(
      req.workflowType,
      {
        id: req.workflowId,
        taskList: req.taskList,
        executionStartToCloseTimeoutMs: req.executionStartToCloseTimeoutMs,
        decisionTaskStartToCloseTimeoutMs: req.decisionTaskStartToCloseTimeoutMs,
        workflowIdReusePolicy: req.workflowIdReusePolicy,
        retryPolicy: req.retryPolicy,
        cronSchedule: req.cronSchedule,
        memo: req.memo,
        requestId: req.requestId,
      },
      req.domain,
      ...(req.input ?? []),
    );
    return result;
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<void> {
    await this.client.signalWorkflow(req.domain, req.workflowId, req.runId, req.signalName, req.input);
  }

  async signalWithStartWorkflow(req: SignalWithStartWorkflowRequest): Promise<StartedWorkflow> {
    this.ensureWorker(req.domain, req.taskList);
    const result = await this.client.signalWithStartWorkflow(
      req.domain,
      req.workflowType,
      {
        workflowId: req.workflowId,
        taskList: req.taskList,
        executionStartToCloseTimeoutMs: req.executionStartToCloseTimeoutMs,
      },
      req.signalName,
      req.signalInput,
      ...(req.input ?? []),
    );
    return result;
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<unknown> {
    return this.client.queryWorkflow(req.domain, req.workflowId, req.runId, req.queryType);
  }

  async cancelWorkflow(req: CancelWorkflowRequest): Promise<void> {
    await this.client.cancelWorkflow(req.domain, req.workflowId, req.runId);
  }

  async terminateWorkflow(req: TerminateWorkflowRequest): Promise<void> {
    await this.client.terminateWorkflow(req.domain, req.workflowId, req.runId, req.reason ?? '');
  }

  async waitForClose(req: WaitForCloseRequest): Promise<WorkflowResult> {
    try {
      return await this.client.waitForClose(req.domain, req.workflowId, req.runId, req.timeoutMs ?? 60_000);
    } catch (err) {
      if (err instanceof Error && 'code' in err && (err as { code?: string }).code === 'EntityNotExists') {
        const notExists = new Error(err.message) as Error & { code?: string };
        notExists.code = 'EntityNotExists';
        throw notExists;
      }
      throw err;
    }
  }

  async describeWorkflowExecution(req: DescribeWorkflowRequest): Promise<WorkflowExecutionDescription> {
    return this.client.describeWorkflowExecution(req.domain, req.workflowId, req.runId);
  }

  async getHistory(req: HistoryRequest): Promise<HistoryEventSummary[]> {
    return this.client.getHistory(req.domain, req.workflowId, req.runId);
  }

  async listOpenWorkflowExecutions(req: ListRequest): Promise<WorkflowExecutionSummary[]> {
    return this.client.listOpenWorkflowExecutions(req.domain, { maxResults: req.maxResults });
  }

  async listClosedWorkflowExecutions(req: ListRequest): Promise<WorkflowExecutionSummary[]> {
    return this.client.listClosedWorkflowExecutions(req.domain, {
      maxResults: req.maxResults,
      workflowId: req.workflowId,
    });
  }

  async interceptorStats(): Promise<InterceptorStats> {
    return { ...this.stats };
  }

  private ensureWorker(domain: string, taskList: string): Worker {
    const key = `${domain}/${taskList}`;
    let worker = this.workers.get(key);
    if (!worker) {
      worker = new Worker(this.connection, domain, taskList, new JsonDataConverter(), {
        interceptors: [this.interceptorFactory],
      });
      registerBehaviors(worker);
      worker.start();
      this.workers.set(key, worker);
    }
    return worker;
  }
}

function registerBehaviors(worker: Worker): void {
  worker.registerWorkflow('echo', async (_ctx: WorkflowContext, value: unknown) => value);

  worker.registerWorkflow('echo-via-activity', async (ctx: WorkflowContext, value: unknown) =>
    ctx.runActivity('echo-activity', value),
  );

  worker.registerWorkflow('signal-counter', async (ctx: WorkflowContext, finishSignal: unknown) => {
    let count = 0;
    ctx.setQueryHandler('count', () => count);
    const finish = typeof finishSignal === 'string' ? finishSignal : 'done';
    for (;;) {
      const signal = await ctx.waitForAnySignal('increment', finish);
      if (signal.name === finish) return count;
      count += Number(signal.input ?? 0);
    }
  });

  worker.registerWorkflow('sleep', async (ctx: WorkflowContext, ms: unknown) => {
    await ctx.sleep(Number(ms ?? 0));
    return 'slept';
  });

  worker.registerWorkflow('fail-until-attempt', async (ctx: WorkflowContext, failures: unknown) =>
    ctx.runActivity('fail-until-attempt-activity', failures, {
      retryPolicy: { initialIntervalMs: 200, backoffCoefficient: 1.5, maximumAttempts: 10 },
    }),
  );

  worker.registerWorkflow('fail-non-retryable', async (ctx: WorkflowContext) =>
    ctx.runActivity('fail-non-retryable-activity', undefined, {
      retryPolicy: {
        initialIntervalMs: 100,
        maximumAttempts: 5,
        nonRetriableErrorReasons: ['DoNotRetry'],
      },
    }),
  );

  worker.registerWorkflow('heartbeat-activity', async (ctx: WorkflowContext, beats: unknown) =>
    ctx.runActivity('heartbeat-activity-impl', beats, { heartbeatTimeoutMs: 5000 }),
  );

  worker.registerWorkflow('child-echo', async (ctx: WorkflowContext, value: unknown) =>
    ctx.runChildWorkflow('echo', value),
  );

  worker.registerWorkflow('continue-as-new', async (ctx: WorkflowContext, count: unknown) => {
    const remaining = Number(count ?? 0);
    if (remaining > 0) {
      throw new ContinueAsNewSignal([remaining - 1]);
    }
    return 'done';
  });

  worker.registerWorkflow('parallel-echo', async (ctx: WorkflowContext, a: unknown, b: unknown) =>
    Promise.all([ctx.runActivity('echo-activity', a), ctx.runActivity('echo-activity', b)]),
  );

  worker.registerWorkflow('activity-timeout', async (ctx: WorkflowContext) =>
    ctx.runActivity('sleep-activity', 5000, { scheduleToCloseTimeoutMs: 1000 }),
  );

  worker.registerActivity('echo-activity', async (input: unknown) => input);

  worker.registerActivity(
    'fail-until-attempt-activity',
    async (failures: unknown, ctx: ActivityContext) => {
      if (ctx.info.attempt <= Number(failures ?? 0)) {
        throw new ApplicationError('transient');
      }
      return ctx.info.attempt;
    },
  );

  worker.registerActivity('fail-non-retryable-activity', async () => {
    throw new ApplicationError('DoNotRetry');
  });

  worker.registerActivity('heartbeat-activity-impl', async (beats: unknown, ctx: ActivityContext) => {
    const total = Number(beats ?? 0);
    for (let i = 0; i < total; i++) {
      await ctx.heartbeat(i);
      await new Promise((r) => setTimeout(r, 50));
    }
    return total;
  });

  worker.registerActivity('sleep-activity', async (ms: unknown) => {
    await new Promise((r) => setTimeout(r, Number(ms ?? 0)));
    return 'slept';
  });
}

export type { WorkflowCloseStatus, WorkflowExecutionSummary, HistoryEventSummary };
