package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareRemovalRecoveryFailure(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput, capture removalexecution.RecoveryCapture, reason lifecycleexecution.FailureReason,
) (*LifecycleFailureDelivery, error) {
	if capture.ValidateFor(attempt, input) != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil || !reason.Valid() {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		progress, err := removalexecution.NewProgress(attempt, input, capture, *attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		digest, err := removalCaptureDigest(input, capture)
		if err != nil {
			return nil, err
		}
		stages, err := lifecycleexecution.Stages(input)
		if err != nil {
			return nil, err
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: progress.UpdatedAt,
			RemovalRecovery: &RemovalRecoveryRecord{Stages: stages, Source: capture.Source, CaptureSHA256: digest, Progress: progress}}
	}
	if !matchesRemovalRecovery(state, attempt, input, capture) || state.RemovalRecovery.Completion != nil || state.RemovalRecovery.Progress.Phase == removalexecution.Complete {
		return nil, conflict()
	}
	if state.RemovalRecovery.Failure != nil {
		return state.RemovalRecovery.Failure, nil
	}
	state.RemovalRecovery.Failure = &LifecycleFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.RemovalRecovery.Failure, nil
}

func (journal *Journal) AcknowledgeRemovalRecoveryFailure(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput, capture removalexecution.RecoveryCapture, terminal workexecution.Attempt, evidence []byte,
) error {
	if terminal.Validate() != nil || terminal.Result == nil || terminal.FinishedAt == nil || terminal.Outcome != workexecution.Failed {
		return invalid()
	}
	failure, err := removalexecution.DecodeFailure(terminal, input, capture, evidence, *terminal.Result)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !matchesRemovalRecovery(state, attempt, input, capture) || state.RemovalRecovery.Failure == nil ||
		state.RemovalRecovery.Failure.Reason != failure.Reason || terminal.FinishedAt.Before(state.UpdatedAt) {
		return conflict()
	}
	stored := state.RemovalRecovery.Failure
	if previous := stored.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || *previous.Result != *terminal.Result || !previous.FinishedAt.Equal(*terminal.FinishedAt) {
			return conflict()
		}
		return nil
	}
	stored.Receipt, stored.Evidence = &terminal, append([]byte(nil), evidence...)
	return journal.writeRecord(ctx, *state)
}
