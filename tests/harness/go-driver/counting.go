package main

import (
	"sync/atomic"

	"go.uber.org/cadence/interceptors"
	"go.uber.org/cadence/workflow"
)

// InterceptorStats counts workflow/activity invocations observed through the
// workflow interceptor chain (deduplicated during replay).
type InterceptorStats struct {
	WorkflowStarts int32
	ActivityStarts int32
}

type countingFactory struct {
	stats *InterceptorStats
}

func (f *countingFactory) NewInterceptor(info *workflow.Info, next interceptors.WorkflowInterceptor) interceptors.WorkflowInterceptor {
	return &countingInterceptor{
		WorkflowInterceptorBase: interceptors.WorkflowInterceptorBase{Next: next},
		stats:                   f.stats,
	}
}

type countingInterceptor struct {
	interceptors.WorkflowInterceptorBase
	stats *InterceptorStats
}

func (t *countingInterceptor) ExecuteWorkflow(ctx workflow.Context, workflowType string, args ...interface{}) []interface{} {
	if !workflow.IsReplaying(ctx) {
		atomic.AddInt32(&t.stats.WorkflowStarts, 1)
	}
	return t.Next.ExecuteWorkflow(ctx, workflowType, args...)
}

func (t *countingInterceptor) ExecuteActivity(ctx workflow.Context, activityType string, args ...interface{}) workflow.Future {
	if !workflow.IsReplaying(ctx) {
		atomic.AddInt32(&t.stats.ActivityStarts, 1)
	}
	return t.Next.ExecuteActivity(ctx, activityType, args...)
}
