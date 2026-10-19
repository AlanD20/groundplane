package agentworkjournal

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func removalCaptureDigest(
	input *agentpb.ServiceLifecycleWorkInput,
	capture removalexecution.RecoveryCapture,
) (string, error) {
	raw, err := removalexecution.EncodeCapture(input, capture)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func matchesRemovalRecovery(
	state *attemptRecord,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture removalexecution.RecoveryCapture,
) bool {
	digest, err := removalCaptureDigest(input, capture)
	if err != nil || state == nil || state.RemovalRecovery == nil ||
		!workexecution.SameAssignment(state.Attempt, attempt) ||
		state.Attempt.StartedAt == nil ||
		attempt.StartedAt == nil ||
		!state.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
		state.RemovalRecovery.CaptureSHA256 != digest ||
		state.RemovalRecovery.Progress.Validate(attempt, input, capture) != nil {
		return false
	}
	stages, err := lifecycleexecution.Stages(input)
	return err == nil && slices.Equal(stages, state.RemovalRecovery.Stages)
}

func (journal *Journal) RemovalRecovery(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture removalexecution.RecoveryCapture,
) (*RemovalRecoveryRecord, error) {
	if capture.ValidateFor(attempt, input) != nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !matchesRemovalRecovery(state, attempt, input, capture) {
		return nil, conflict()
	}
	return state.RemovalRecovery, nil
}

func (journal *Journal) PrepareRemovalRecovery(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput, capture removalexecution.RecoveryCapture, next removalexecution.Progress,
) (*RemovalRecoveryRecord, error) {
	if next.Validate(attempt, input, capture) != nil || next.Sequence == 0 {
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
	var previous *removalexecution.Progress
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: attempt}
	} else {
		if !matchesRemovalRecovery(state, attempt, input, capture) || state.RemovalRecovery.Completion != nil || state.RemovalRecovery.Failure != nil {
			return nil, conflict()
		}
		previous = &state.RemovalRecovery.Progress
		if removalexecution.EqualProgress(*previous, next) {
			return state.RemovalRecovery, nil
		}
		if !state.RemovalRecovery.Acknowledged {
			return nil, conflict()
		}
	}
	if err := removalexecution.ValidateTransition(attempt, input, capture, previous, next); err != nil {
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
	state.RemovalRecovery = &RemovalRecoveryRecord{
		Source:        capture.Source,
		Stages:        stages,
		CaptureSHA256: digest,
		Progress:      next,
	}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.RemovalRecovery, nil
}

func (journal *Journal) AcknowledgeRemovalRecovery(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	capture removalexecution.RecoveryCapture,
	checkpoint workexecution.Checkpoint,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !matchesRemovalRecovery(state, attempt, input, capture) {
		return conflict()
	}
	expected, err := state.RemovalRecovery.Progress.Checkpoint(attempt, input, capture)
	if err != nil || checkpoint != expected {
		return conflict()
	}
	if state.RemovalRecovery.Acknowledged {
		return nil
	}
	state.RemovalRecovery.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) CompleteRemovalRecovery(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput, capture removalexecution.RecoveryCapture, terminal workexecution.Attempt,
) error {
	if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil {
		return invalid()
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
	if !matchesRemovalRecovery(state, attempt, input, capture) {
		return conflict()
	}
	if previous := state.RemovalRecovery.Completion; previous != nil {
		if !workexecution.SameAssignment(*previous, terminal) || previous.Outcome != terminal.Outcome ||
			!previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result ||
			terminal.StartedAt == nil || !previous.StartedAt.Equal(*terminal.StartedAt) {
			return conflict()
		}
		return nil
	}
	state.RemovalRecovery.Completion = &terminal
	return journal.writeRecord(ctx, *state)
}
