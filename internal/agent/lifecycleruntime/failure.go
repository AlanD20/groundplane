package lifecycleruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Failure delivery may outlive a cancelled effect only to persist and report
// its boundary. This context cannot start another container operation or hook.
func (executor *Executor) fail(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	reason := lifecycleexecution.ExecutionFailed
	if !time.Now().Before(input.attempt.Deadline) {
		reason = lifecycleexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = lifecycleexecution.ExecutionInterrupted
	}
	deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	failure, err := executor.journal.PrepareLifecycleFailure(deliveryCtx, input.attempt, input.procedure, reason)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.deliverFailure(deliveryCtx, input, exchange, failure)
}

func (executor *Executor) deliverFailure(
	ctx context.Context,
	input Input,
	exchange Exchange,
	failure *agentworkjournal.LifecycleFailureDelivery,
) (workexecution.Attempt, error) {
	if failure.Receipt != nil {
		return *failure.Receipt, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailLifecycle(
		ctx,
		&agentpb.ServiceLifecycleFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeLifecycleFailure(ctx, input.attempt, input.procedure, ack)
}
