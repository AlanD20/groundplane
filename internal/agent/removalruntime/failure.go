package removalruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
)

func (executor *Executor) fail(ctx context.Context, input Input, exchange Exchange) (workexecution.Attempt, error) {
	reason := lifecycleexecution.ExecutionFailed
	if !time.Now().Before(input.attempt.Deadline) {
		reason = lifecycleexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = lifecycleexecution.ExecutionInterrupted
	}
	// This context only persists/reports failure; it cannot execute host effects.
	deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	failure, err := executor.journal.PrepareRemovalRecoveryFailure(
		deliveryCtx,
		input.attempt,
		input.procedure,
		input.capture,
		reason,
	)
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
	request, err := input.FailureRequest(failure.Reason)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailRemovalRecovery(ctx, request)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := input.AcknowledgeFailure(failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeRemovalRecoveryFailure(ctx, input.attempt, input.procedure, input.capture, terminal, ack.Failure); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
