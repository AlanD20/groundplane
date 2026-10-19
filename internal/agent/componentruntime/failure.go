package componentruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Failure delivery has a bounded cleanup context so a cancelled effect can save
// its failure. It never executes another host action or claims effects undone.
func (executor *Executor) fail(
	ctx context.Context,
	input Input,
	exchange Exchange,
	_ error,
) (workexecution.Attempt, error) {
	reason := componentexecution.ExecutionFailed
	if !time.Now().Before(input.attempt.Deadline) {
		reason = componentexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = componentexecution.ExecutionInterrupted
	}
	deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	var failure *agentworkjournal.ComponentFailureDelivery
	var err error
	if input.recovery != nil {
		failure, err = executor.journal.PrepareComponentRecoveryFailure(
			deliveryCtx,
			input.attempt,
			input.procedure,
			reason,
		)
	} else {
		failure, err = executor.journal.PrepareComponentFailure(deliveryCtx, input.attempt, input.procedure, reason)
	}
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, err := executor.deliverFailure(deliveryCtx, input, exchange, failure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

func (executor *Executor) deliverFailure(
	ctx context.Context,
	input Input,
	exchange Exchange,
	failure *agentworkjournal.ComponentFailureDelivery,
) (workexecution.Attempt, error) {
	if failure.Receipt != nil {
		return *failure.Receipt, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailComponent(
		ctx,
		&agentpb.ComponentWorkFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if input.recovery != nil {
		return executor.journal.AcknowledgeComponentRecoveryFailure(ctx, input.attempt, input.procedure, ack)
	}
	return executor.journal.AcknowledgeComponentFailure(ctx, input.attempt, input.procedure, ack)
}
