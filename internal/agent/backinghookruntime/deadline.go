package backinghookruntime

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func boundedBackingHookTimeout(ctx context.Context, requested time.Duration) (time.Duration, error) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, errs.New(errs.KindInternal, "agent: backing hook Task deadline is missing")
	}
	available := time.Until(deadline) - backingHookKillAfter - backingHookRunGrace - 2*backingHookHelperTimeout
	if available <= 0 {
		return 0, errs.New(errs.KindStateConflict, "agent: backing hook Task budget is exhausted")
	}
	if requested < available {
		return requested, nil
	}
	return available, nil
}

// backingHookExecutionContext deliberately preserves an in-flight Docker exec
// across Task cancellation. Canceling the Docker client is not evidence that
// the process created by docker exec stopped; the in-container timeout owns
// termination, and the Agent remains joined until it observes completion. The
// absolute deadline retains one helper budget for reading and one for cleanup.
func backingHookExecutionContext(
	ctx context.Context,
	hookTimeout time.Duration,
) (context.Context, context.CancelFunc, time.Duration, error) {
	taskDeadline, ok := ctx.Deadline()
	if !ok {
		return nil, nil, 0, errs.New(errs.KindInternal, "agent: backing hook Task deadline is missing")
	}
	joinDeadline := time.Now().Add(hookTimeout + backingHookKillAfter + backingHookRunGrace)
	latestJoinDeadline := taskDeadline.Add(-2 * backingHookHelperTimeout)
	if latestJoinDeadline.Before(joinDeadline) {
		joinDeadline = latestJoinDeadline
	}
	joinTimeout := time.Until(joinDeadline)
	if joinTimeout <= 0 {
		return nil, nil, 0, errs.New(errs.KindStateConflict, "agent: backing hook Task budget is exhausted")
	}
	executionCtx, cancel := context.WithDeadline(context.WithoutCancel(ctx), joinDeadline)
	return executionCtx, cancel, joinTimeout, nil
}

func backingHookReadContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(backingHookHelperTimeout)
	if taskDeadline, ok := ctx.Deadline(); ok {
		reserved := taskDeadline.Add(-backingHookHelperTimeout)
		if reserved.Before(deadline) {
			deadline = reserved
		}
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}

func backingHookCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(backingHookHelperTimeout)
	if taskDeadline, ok := ctx.Deadline(); ok && taskDeadline.Before(deadline) {
		deadline = taskDeadline
	}
	return context.WithDeadline(context.WithoutCancel(ctx), deadline)
}
