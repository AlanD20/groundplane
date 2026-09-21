package etcd

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	initialTaskCASDelay = 2 * time.Millisecond
	maximumTaskCASDelay = 128 * time.Millisecond
)

type taskCASRetryPolicy struct {
	initialDelay time.Duration
	maximumDelay time.Duration
	jitter       func(time.Duration) time.Duration
	wait         func(context.Context, time.Duration) error
}

func defaultTaskCASRetryPolicy() taskCASRetryPolicy {
	return taskCASRetryPolicy{
		initialDelay: initialTaskCASDelay,
		maximumDelay: maximumTaskCASDelay,
		jitter: func(bound time.Duration) time.Duration {
			if bound <= 0 {
				return 0
			}
			return time.Duration(rand.Int64N(int64(bound) + 1))
		},
		wait: waitForTaskCASRetry,
	}
}

func validateTaskCASRetryPolicy(policy taskCASRetryPolicy) error {
	if policy.initialDelay <= 0 || policy.maximumDelay < policy.initialDelay ||
		policy.jitter == nil || policy.wait == nil {
		return errs.New(errs.KindInternal, "task CAS retry policy is invalid")
	}
	return nil
}

func (policy taskCASRetryPolicy) waitAfterConflict(ctx context.Context, conflicts int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := policy.initialDelay
	for exponent := 1; exponent < conflicts && delay < policy.maximumDelay; exponent++ {
		if delay > policy.maximumDelay/2 {
			delay = policy.maximumDelay
			break
		}
		delay *= 2
	}
	jitterBound := delay / 2
	jitter := policy.jitter(jitterBound)
	if jitter < 0 || jitter > jitterBound {
		return errs.New(errs.KindInternal, "task CAS retry jitter is outside its bound")
	}
	if delay > policy.maximumDelay-jitter {
		delay = policy.maximumDelay
	} else {
		delay += jitter
	}
	return policy.wait(ctx, delay)
}

func waitForTaskCASRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
