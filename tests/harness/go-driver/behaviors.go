package main

import (
	"context"
	"time"

	"go.uber.org/cadence"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

// ---- Activities ----

func echoActivity(ctx context.Context, value string) (string, error) {
	return value, nil
}

func failUntilAttemptActivity(ctx context.Context, failures int) (int, error) {
	info := activity.GetInfo(ctx)
	if int(info.Attempt) <= failures {
		return 0, cadence.NewCustomError("transient")
	}
	return int(info.Attempt), nil
}

func failNonRetryableActivity(ctx context.Context) (string, error) {
	return "", cadence.NewCustomError("DoNotRetry")
}

func heartbeatActivityImpl(ctx context.Context, beats int) (int, error) {
	for i := 0; i < beats; i++ {
		activity.RecordHeartbeat(ctx, i)
		time.Sleep(50 * time.Millisecond)
	}
	return beats, nil
}

func sleepActivity(ctx context.Context, ms int) (string, error) {
	time.Sleep(time.Duration(ms) * time.Millisecond)
	return "slept", nil
}

// ---- Workflows ----

func echoWorkflow(ctx workflow.Context, value string) (string, error) {
	return value, nil
}

func echoViaActivityWorkflow(ctx workflow.Context, value string) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
	var out string
	if err := workflow.ExecuteActivity(ctx, echoActivity, value).Get(ctx, &out); err != nil {
		return "", err
	}
	return out, nil
}

func signalCounterWorkflow(ctx workflow.Context, finishSignal string) (int, error) {
	count := 0
	if err := workflow.SetQueryHandler(ctx, "count", func() (int, error) {
		return count, nil
	}); err != nil {
		return 0, err
	}
	inc := workflow.GetSignalChannel(ctx, "increment")
	done := false
	fin := workflow.GetSignalChannel(ctx, finishSignal)
	s := workflow.NewSelector(ctx)
	s.AddReceive(inc, func(c workflow.Channel, more bool) {
		var n int
		c.Receive(ctx, &n)
		count += n
	})
	s.AddReceive(fin, func(c workflow.Channel, more bool) {
		done = true
	})
	for !done {
		s.Select(ctx)
	}
	return count, nil
}

func sleepWorkflow(ctx workflow.Context, ms int) (string, error) {
	if err := workflow.Sleep(ctx, time.Duration(ms)*time.Millisecond); err != nil {
		return "", err
	}
	return "slept", nil
}

func failUntilAttemptWorkflow(ctx workflow.Context, failures int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		ScheduleToCloseTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy: &workflow.RetryPolicy{
			InitialInterval:    200 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumAttempts:    10,
		},
	})
	var attempt int
	if err := workflow.ExecuteActivity(ctx, failUntilAttemptActivity, failures).Get(ctx, &attempt); err != nil {
		return 0, err
	}
	return attempt, nil
}

func failNonRetryableWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		ScheduleToCloseTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		RetryPolicy: &workflow.RetryPolicy{
			InitialInterval:          100 * time.Millisecond,
			MaximumAttempts:          5,
			NonRetriableErrorReasons: []string{"DoNotRetry"},
		},
	})
	var out string
	err := workflow.ExecuteActivity(ctx, failNonRetryableActivity).Get(ctx, &out)
	return "", err
}

func heartbeatWorkflow(ctx workflow.Context, beats int) (int, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
		HeartbeatTimeout:       5 * time.Second,
	})
	var out int
	if err := workflow.ExecuteActivity(ctx, heartbeatActivityImpl, beats).Get(ctx, &out); err != nil {
		return 0, err
	}
	return out, nil
}

func childEchoWorkflow(ctx workflow.Context, value string) (string, error) {
	ctx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ExecutionStartToCloseTimeout: time.Minute,
	})
	var out string
	if err := workflow.ExecuteChildWorkflow(ctx, echoWorkflow, value).Get(ctx, &out); err != nil {
		return "", err
	}
	return out, nil
}

func continueAsNewWorkflow(ctx workflow.Context, count int) (string, error) {
	if count > 0 {
		return "", workflow.NewContinueAsNewError(ctx, continueAsNewWorkflow, count-1)
	}
	return "done", nil
}

func parallelEchoWorkflow(ctx workflow.Context, a string, b string) ([]string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		StartToCloseTimeout:    time.Minute,
	})
	f1 := workflow.ExecuteActivity(ctx, echoActivity, a)
	f2 := workflow.ExecuteActivity(ctx, echoActivity, b)
	var ra, rb string
	if err := f1.Get(ctx, &ra); err != nil {
		return nil, err
	}
	if err := f2.Get(ctx, &rb); err != nil {
		return nil, err
	}
	return []string{ra, rb}, nil
}

func activityTimeoutWorkflow(ctx workflow.Context) (string, error) {
	ctx = workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		ScheduleToStartTimeout: time.Minute,
		ScheduleToCloseTimeout: time.Second,
		StartToCloseTimeout:    time.Second,
	})
	var out string
	err := workflow.ExecuteActivity(ctx, sleepActivity, 5000).Get(ctx, &out)
	return "", err
}

// ---- Registration ----

func registerBehaviors(w worker.Worker) {
	w.RegisterWorkflowWithOptions(echoWorkflow, workflow.RegisterOptions{Name: "echo"})
	w.RegisterWorkflowWithOptions(echoViaActivityWorkflow, workflow.RegisterOptions{Name: "echo-via-activity"})
	w.RegisterWorkflowWithOptions(signalCounterWorkflow, workflow.RegisterOptions{Name: "signal-counter"})
	w.RegisterWorkflowWithOptions(sleepWorkflow, workflow.RegisterOptions{Name: "sleep"})
	w.RegisterWorkflowWithOptions(failUntilAttemptWorkflow, workflow.RegisterOptions{Name: "fail-until-attempt"})
	w.RegisterWorkflowWithOptions(failNonRetryableWorkflow, workflow.RegisterOptions{Name: "fail-non-retryable"})
	w.RegisterWorkflowWithOptions(heartbeatWorkflow, workflow.RegisterOptions{Name: "heartbeat-activity"})
	w.RegisterWorkflowWithOptions(childEchoWorkflow, workflow.RegisterOptions{Name: "child-echo"})
	w.RegisterWorkflowWithOptions(continueAsNewWorkflow, workflow.RegisterOptions{Name: "continue-as-new"})
	w.RegisterWorkflowWithOptions(parallelEchoWorkflow, workflow.RegisterOptions{Name: "parallel-echo"})
	w.RegisterWorkflowWithOptions(activityTimeoutWorkflow, workflow.RegisterOptions{Name: "activity-timeout"})

	w.RegisterActivityWithOptions(echoActivity, activity.RegisterOptions{Name: "echo-activity"})
	w.RegisterActivityWithOptions(failUntilAttemptActivity, activity.RegisterOptions{Name: "fail-until-attempt-activity"})
	w.RegisterActivityWithOptions(failNonRetryableActivity, activity.RegisterOptions{Name: "fail-non-retryable-activity"})
	w.RegisterActivityWithOptions(heartbeatActivityImpl, activity.RegisterOptions{Name: "heartbeat-activity-impl"})
	w.RegisterActivityWithOptions(sleepActivity, activity.RegisterOptions{Name: "sleep-activity"})
}
