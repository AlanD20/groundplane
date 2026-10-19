package backingruntime

import (
	"context"
	"encoding/hex"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/agentworkjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func checkpointRequest(
	assignment backingprovision.Assignment,
	progress backingprovision.Progress,
) (*agentpb.BackingProvisionCheckpointRequest, error) {
	layout, err := backingprovision.CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil {
		return nil, err
	}
	raw, err := backingprovision.EncodeProgress(assignment.Attempt, layout, progress)
	if err != nil {
		return nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return nil, err
	}
	return &agentpb.BackingProvisionCheckpointRequest{Attempt: identity, Progress: raw}, nil
}

func (executor *Executor) save(
	ctx context.Context,
	assignment backingprovision.Assignment,
	exchange Exchange,
	progress backingprovision.Progress,
) error {
	if _, err := executor.journal.PrepareBacking(ctx, assignment.Attempt, assignment.Procedure, progress); err != nil {
		return err
	}
	return executor.acknowledge(ctx, assignment, exchange, progress)
}

func (executor *Executor) acknowledge(
	ctx context.Context,
	assignment backingprovision.Assignment,
	exchange Exchange,
	progress backingprovision.Progress,
) error {
	request, err := checkpointRequest(assignment, progress)
	if err != nil {
		return err
	}
	ack, err := exchange.BackingProvisionCheckpoint(ctx, request)
	if err != nil {
		return err
	}
	return executor.journal.AcknowledgeBacking(ctx, assignment.Attempt, assignment.Procedure, ack)
}

func (executor *Executor) complete(
	ctx context.Context,
	assignment backingprovision.Assignment,
	exchange Exchange,
) (workexecution.Attempt, error) {
	saved, err := executor.journal.Backing(ctx, assignment.Attempt, assignment.Procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if saved == nil || saved.Progress == nil || saved.Progress.Stage != backingprovision.Complete ||
		!saved.Acknowledged || saved.Runtime == nil || saved.Failure != nil {
		return workexecution.Attempt{}, invalidInput()
	}
	checkpoint, err := saved.Progress.Checkpoint(assignment.Attempt, saved.Layout)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	evidence, err := backingprovision.CompletionEvidence(assignment.Attempt, checkpoint, *saved.Runtime)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	digest, err := hex.DecodeString(evidence.SHA256)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	runtime, err := backingprovision.EncodeRuntimeObservation(*saved.Runtime)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.CompleteBackingProvision(ctx, &agentpb.BackingProvisionCompletionRequest{Attempt: identity,
		Evidence: &agentpb.WorkInputReference{InputId: evidence.ID, Sha256: digest}, RuntimeObservation: runtime})
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.CompleteBacking(ctx, assignment.Attempt, assignment.Procedure, ack)
}

func (executor *Executor) fail(
	ctx context.Context,
	assignment backingprovision.Assignment,
	exchange Exchange,
	cause error,
) (workexecution.Attempt, error) {
	reason := backingprovision.ExecutionFailed
	if !time.Now().Before(assignment.Attempt.Deadline) {
		reason = backingprovision.DeadlineExpired
	} else if ctx.Err() != nil {
		reason = backingprovision.Interrupted
	}
	delivery, cancel := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer cancel()
	failure, err := executor.journal.PrepareBackingFailure(
		delivery,
		assignment.Attempt,
		assignment.Procedure,
		reason,
		time.Now().UTC(),
	)
	if err != nil {
		return workexecution.Attempt{}, errors.Join(cause, err)
	}
	terminal, err := executor.deliverFailure(delivery, assignment, exchange, failure)
	if err != nil {
		return workexecution.Attempt{}, errors.Join(cause, err)
	}
	return terminal, nil
}

func (executor *Executor) deliverFailure(ctx context.Context, assignment backingprovision.Assignment, exchange Exchange,
	failure *agentworkjournal.BackingFailureDelivery,
) (workexecution.Attempt, error) {
	if failure.Receipt != nil {
		return *failure.Receipt, nil
	}
	identity, err := workexecution.EncodeAgentAttempt(assignment.Attempt)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	ack, err := exchange.FailBackingProvision(
		ctx,
		&agentpb.BackingProvisionFailureRequest{Attempt: identity, Reason: string(failure.Reason)},
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	return executor.journal.AcknowledgeBackingFailure(ctx, assignment.Attempt, assignment.Procedure, ack)
}
