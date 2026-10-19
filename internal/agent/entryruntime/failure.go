package entryruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Failure delivery never performs host effects or treats cancellation as proof
// that an earlier command stopped. The Controller retains the accepted boundary.
func (executor *Executor) fail(
	ctx context.Context,
	input Input,
	exchange Exchange,
	_ error,
) (workexecution.Attempt, error) {
	reason := entryexecution.ExecutionFailed
	if !time.Now().Before(input.attempt.Deadline) {
		reason = entryexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = entryexecution.ExecutionInterrupted
	}
	deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	failure, err := executor.journal.PrepareEntryConfigurationFailure(
		deliveryCtx,
		input.attempt,
		input.procedure,
		reason,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.deliverFailure(deliveryCtx, input, exchange, failure)
}

func (executor *Executor) deliverFailure(ctx context.Context, input Input, exchange Exchange,
	failure *agentworkjournal.EntryConfigurationFailureDelivery,
) (workexecution.Attempt, error) {
	if failure.Receipt != nil {
		return *failure.Receipt, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailEntryConfiguration(ctx, &agentpb.EntryConfigurationWorkFailureRequest{
		Attempt: identity, Reason: string(failure.Reason),
	})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	terminal, _, err := entryexecution.DecodeFailureAck(input.attempt, input.procedure, failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if err := executor.journal.AcknowledgeEntryConfigurationFailure(ctx, input.attempt, input.procedure, terminal, ack.Failure); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
