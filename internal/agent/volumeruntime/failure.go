package volumeruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
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
	reason := volumeexecution.ExecutionFailed
	if !time.Now().Before(input.attempt.Deadline) {
		reason = volumeexecution.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = volumeexecution.ExecutionInterrupted
	}
	deliveryCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	var failure *agentworkjournal.VolumeFailureDelivery
	var err error
	if input.recovery != nil {
		failure, err = executor.journal.PrepareVolumeRecoveryFailure(deliveryCtx, input.attempt, reason)
	} else {
		failure, err = executor.journal.PrepareVolumeFailure(deliveryCtx, input.attempt, input.procedure.Action, input.procedure.VolumeId, reason)
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
	failure *agentworkjournal.VolumeFailureDelivery,
) (workexecution.Attempt, error) {
	if failure.Receipt != nil {
		return *failure.Receipt, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailVolume(
		ctx,
		&agentpb.VolumeWorkFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if input.recovery != nil {
		return executor.journal.AcknowledgeVolumeRecoveryFailure(ctx, input.attempt, ack)
	}
	return executor.journal.AcknowledgeVolumeFailure(
		ctx,
		input.attempt,
		input.procedure.Action,
		input.procedure.VolumeId,
		ack,
	)
}
