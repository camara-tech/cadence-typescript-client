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
  WorkflowExecutionDescription,
  WorkflowExecutionSummary,
  WorkflowResult,
} from './port.js';

/**
 * In-process adapter over this repo's TypeScript client — the red-green TDD
 * target. Intentionally unimplemented until the client exists (T03/T06).
 */
export class TsDriver implements CadenceTestDriver {
  async init(): Promise<void> {
    throw new Error('TsDriver: not implemented yet (TDD red phase)');
  }

  async ensureDomain(_name: string, _retentionDays?: number): Promise<void> {
    throw new Error('TsDriver.ensureDomain: not implemented');
  }

  async startWorkflow(_req: StartWorkflowRequest): Promise<StartedWorkflow> {
    throw new Error('TsDriver.startWorkflow: not implemented');
  }

  async signalWorkflow(_req: SignalWorkflowRequest): Promise<void> {
    throw new Error('TsDriver.signalWorkflow: not implemented');
  }

  async signalWithStartWorkflow(_req: SignalWithStartWorkflowRequest): Promise<StartedWorkflow> {
    throw new Error('TsDriver.signalWithStartWorkflow: not implemented');
  }

  async queryWorkflow(_req: QueryWorkflowRequest): Promise<unknown> {
    throw new Error('TsDriver.queryWorkflow: not implemented');
  }

  async cancelWorkflow(_req: CancelWorkflowRequest): Promise<void> {
    throw new Error('TsDriver.cancelWorkflow: not implemented');
  }

  async terminateWorkflow(_req: TerminateWorkflowRequest): Promise<void> {
    throw new Error('TsDriver.terminateWorkflow: not implemented');
  }

  async waitForClose(_req: WaitForCloseRequest): Promise<WorkflowResult> {
    throw new Error('TsDriver.waitForClose: not implemented');
  }

  async describeWorkflowExecution(_req: DescribeWorkflowRequest): Promise<WorkflowExecutionDescription> {
    throw new Error('TsDriver.describeWorkflowExecution: not implemented');
  }

  async getHistory(_req: HistoryRequest): Promise<HistoryEventSummary[]> {
    throw new Error('TsDriver.getHistory: not implemented');
  }

  async listOpenWorkflowExecutions(_req: ListRequest): Promise<WorkflowExecutionSummary[]> {
    throw new Error('TsDriver.listOpenWorkflowExecutions: not implemented');
  }

  async listClosedWorkflowExecutions(_req: ListRequest): Promise<WorkflowExecutionSummary[]> {
    throw new Error('TsDriver.listClosedWorkflowExecutions: not implemented');
  }

  async interceptorStats(): Promise<InterceptorStats> {
    throw new Error('TsDriver.interceptorStats: not implemented');
  }
}
