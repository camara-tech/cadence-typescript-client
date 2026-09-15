import type {
  CadenceTestDriver,
  CancelWorkflowRequest,
  DescribeWorkflowRequest,
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
  WorkflowResult,
  WorkflowExecutionDescription,
  WorkflowExecutionSummary,
  HistoryEventSummary,
} from './port.js';

const BASE = 'http://127.0.0.1:7877';

interface ErrorResponse {
  code: string;
  message: string;
}

async function post<T>(path: string, body: unknown): Promise<{ status: number; json: T | ErrorResponse }> {
  const res = await fetch(`${BASE}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body ?? {}),
  });
  if (res.status === 204) return { status: 204, json: undefined as T };
  return { status: res.status, json: (await res.json()) as T | ErrorResponse };
}

function isError(json: unknown): json is ErrorResponse {
  return typeof json === 'object' && json !== null && 'code' in json && 'message' in json;
}

/**
 * Adapter speaking HTTP to the Go harness process, which drives the real
 * cadence-go-client against the running Cadence server.
 */
export class GoHarnessDriver implements CadenceTestDriver {
  close?(): Promise<void>;

  async ensureRunning(): Promise<void> {
    const res = await fetch(`${BASE}/health`).catch(() => undefined);
    if (!res || res.status !== 200) {
      throw new Error(
        'Go harness not running. Start it with: SERVICE_ADDR=127.0.0.1:7833 tests/harness/go-driver/run.sh',
      );
    }
  }

  async ensureDomain(name: string, retentionDays = 1): Promise<void> {
    const { status, json } = await post('/ensureDomain', { name, retentionDays });
    if (status !== 204) throw toDriverError(status, json);
  }

  async startWorkflow(req: StartWorkflowRequest): Promise<StartedWorkflow> {
    const { status, json } = await post<StartedWorkflow>('/startWorkflow', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as StartedWorkflow;
  }

  async signalWorkflow(req: SignalWorkflowRequest): Promise<void> {
    const { status, json } = await post('/signalWorkflow', req);
    if (status !== 204) throw toDriverError(status, json);
  }

  async signalWithStartWorkflow(req: SignalWithStartWorkflowRequest): Promise<StartedWorkflow> {
    const { status, json } = await post<StartedWorkflow>('/signalWithStartWorkflow', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as StartedWorkflow;
  }

  async queryWorkflow(req: QueryWorkflowRequest): Promise<unknown> {
    const { status, json } = await post<{ value: unknown }>('/queryWorkflow', req);
    if (status !== 200) throw toDriverError(status, json);
    return (json as { value: unknown }).value;
  }

  async cancelWorkflow(req: CancelWorkflowRequest): Promise<void> {
    const { status, json } = await post('/cancelWorkflow', req);
    if (status !== 204) throw toDriverError(status, json);
  }

  async terminateWorkflow(req: TerminateWorkflowRequest): Promise<void> {
    const { status, json } = await post('/terminateWorkflow', req);
    if (status !== 204) throw toDriverError(status, json);
  }

  async waitForClose(req: WaitForCloseRequest): Promise<WorkflowResult> {
    const { status, json } = await post<WorkflowResult>('/waitForClose', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as WorkflowResult;
  }

  async describeWorkflowExecution(req: DescribeWorkflowRequest): Promise<WorkflowExecutionDescription> {
    const { status, json } = await post<WorkflowExecutionDescription>('/describeWorkflowExecution', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as WorkflowExecutionDescription;
  }

  async getHistory(req: HistoryRequest): Promise<HistoryEventSummary[]> {
    const { status, json } = await post<HistoryEventSummary[]>('/getHistory', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as HistoryEventSummary[];
  }

  async listOpenWorkflowExecutions(req: ListRequest): Promise<WorkflowExecutionSummary[]> {
    const { status, json } = await post<WorkflowExecutionSummary[]>('/listOpenWorkflowExecutions', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as WorkflowExecutionSummary[];
  }

  async listClosedWorkflowExecutions(req: ListRequest): Promise<WorkflowExecutionSummary[]> {
    const { status, json } = await post<WorkflowExecutionSummary[]>('/listClosedWorkflowExecutions', req);
    if (status !== 200) throw toDriverError(status, json);
    return json as WorkflowExecutionSummary[];
  }

  async interceptorStats(): Promise<InterceptorStats> {
    const res = await fetch(`${BASE}/interceptorStats`);
    return (await res.json()) as InterceptorStats;
  }
}

function toDriverError(status: number, json: unknown): Error {
  if (isError(json)) {
    const err = new Error(json.message) as Error & { code?: string };
    err.code = json.code;
    return err;
  }
  return new Error(`harness returned ${status}`);
}
