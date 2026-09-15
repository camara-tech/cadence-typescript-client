/**
 * Interception surface for workflow executions. A factory creates one
 * interceptor per workflow replay/execution; hooks fire only for fresh
 * (non-replayed) invocations, mirroring the reference clients'
 * WorkflowInterceptor semantics.
 */
export interface WorkflowInvocationInfo {
  workflowType: string;
  workflowId: string;
  runId: string;
}

export interface WorkflowInterceptor {
  /** Called when the workflow function starts a fresh (non-replayed) run. */
  onWorkflowStarted(info: WorkflowInvocationInfo): void;
  /** Called when the workflow schedules a fresh (non-replayed) activity. */
  onActivityScheduled(info: WorkflowInvocationInfo & { activityType: string; activityId: string }): void;
}

export interface WorkflowInterceptorFactory {
  create(info: WorkflowInvocationInfo): WorkflowInterceptor;
}

/** Composes factories into a chain that fans each hook out to all of them. */
export function chainInterceptorFactories(
  factories: WorkflowInterceptorFactory[],
): WorkflowInterceptorFactory | undefined {
  if (factories.length === 0) return undefined;
  return {
    create(info: WorkflowInvocationInfo): WorkflowInterceptor {
      const interceptors = factories.map((factory) => factory.create(info));
      return {
        onWorkflowStarted(info: WorkflowInvocationInfo): void {
          for (const interceptor of interceptors) interceptor.onWorkflowStarted(info);
        },
        onActivityScheduled(info: WorkflowInvocationInfo & { activityType: string; activityId: string }): void {
          for (const interceptor of interceptors) {
            interceptor.onActivityScheduled(info);
          }
        },
      };
    },
  };
}
