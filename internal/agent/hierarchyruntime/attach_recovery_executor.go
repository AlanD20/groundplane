package hierarchyruntime

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (executor *AttachExecutor) Recover(
	ctx context.Context,
	input attachexecution.RecoveryInput,
	exchange AttachExchange,
) (*workexecution.Attempt, error) {
	if err := input.Validate(); err != nil {
		return nil, err
	}
	savedAttempt, saved, err := executor.journal.LoadAttachInspection(ctx, input)
	if err != nil {
		return nil, err
	}
	var report attachexecution.RemovalInspection
	if saved != nil {
		input.Attempt = *savedAttempt
		if saved.Receipt != nil {
			return saved.Receipt, nil
		}
		report = saved.Report
	} else {
		input, report, err = executor.inspectRecovery(ctx, input, exchange)
		if err != nil {
			return nil, err
		}
		storage, cancelStorage := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err = executor.journal.SaveAttachInspection(storage, input, report)
		cancelStorage()
		if err != nil {
			return nil, err
		}
	}
	request, err := attachexecution.EncodeInspectionRequest(input, report)
	if err != nil {
		return nil, err
	}
	bounded, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	ack, err := exchange.CompleteAttachRecovery(bounded, request)
	if err != nil {
		return nil, err
	}
	terminal, err := attachexecution.DecodeInspectionAck(input, report, ack)
	if err != nil {
		return nil, err
	}
	if err := executor.journal.AcknowledgeAttachInspection(bounded, input, terminal); err != nil {
		return nil, err
	}
	return &terminal, nil
}

func (executor *AttachExecutor) inspectRecovery(
	ctx context.Context,
	input attachexecution.RecoveryInput,
	exchange AttachExchange,
) (attachexecution.RecoveryInput, attachexecution.RemovalInspection, error) {
	interrupted := func() attachexecution.RemovalInspection {
		return attachexecution.RemovalInspection{AttemptID: input.Attempt.ID, ForwardID: input.Source.Forward.ID,
			Failure: input.Source.Evidence, CommandOrdinal: uint32(input.Source.Failure.Checkpoint.Sequence / 2),
			State: attachexecution.InspectionInterrupted, ObservedAt: time.Now().UTC()}
	}
	if err := ctx.Err(); err != nil {
		return input, attachexecution.RemovalInspection{}, err
	}
	if !time.Now().UTC().Before(input.Attempt.Deadline) {
		return input, interrupted(), nil
	}
	identity, err := workexecution.EncodeAgentAttempt(input.Attempt)
	if err != nil {
		return input, attachexecution.RemovalInspection{}, err
	}
	ack, err := exchange.Start(ctx, &agentpb.WorkStartRequest{Attempt: identity})
	if err != nil {
		if ctx.Err() == nil && !time.Now().UTC().Before(input.Attempt.Deadline) {
			// Start may have reached the Controller. The completion response
			// supplies its saved Start time; no command permission is inferred.
			return input, interrupted(), nil
		}
		return input, attachexecution.RemovalInspection{}, err
	}
	started, _, err := workexecution.DecodeAgentStartAck(ack, input.Attempt)
	if err != nil {
		return input, attachexecution.RemovalInspection{}, err
	}
	input.Attempt.StartedAt = &started
	inspection, cancel := context.WithDeadline(ctx, input.Attempt.Deadline)
	report, err := executor.host.InspectRemoval(inspection, input)
	cancel()
	if err != nil && ctx.Err() == nil {
		// Inspection failure/expiry does not settle the prior command's
		// effects. Persist an unresolved result and release this worker slot.
		return input, interrupted(), nil
	}
	return input, report, err
}
