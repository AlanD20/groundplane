package agentworkjournal

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareEnvironmentRemoval(ctx context.Context, input hierarchyexecution.EnvironmentInput,
	next hierarchyexecution.EnvironmentProgress,
) (*EnvironmentRemovalRecord, error) {
	if next.Validate(input) != nil || next.Sequence == 0 {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[input.Attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: input.Attempt}
	}
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) || state.Attempt.StartedAt == nil ||
		!state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) {
		return nil, conflict()
	}
	if state.EnvironmentFailure != nil {
		return nil, conflict()
	}
	var previous *hierarchyexecution.EnvironmentProgress
	if saved := state.EnvironmentRemoval; saved != nil {
		if !saved.matches(input) {
			return nil, conflict()
		}
		previous = &saved.Report.Progress
		if hierarchyexecution.EqualEnvironmentProgress(*previous, next) {
			return saved, nil
		}
		if !saved.Acknowledged {
			return nil, conflict()
		}
	} else if state.Sequence != 0 {
		return nil, conflict()
	}
	if err := hierarchyexecution.ValidateEnvironmentTransition(input, previous, next); err != nil {
		return nil, err
	}
	state.EnvironmentRemoval = &EnvironmentRemovalRecord{
		Recovery:      input.Recovery,
		EnvironmentID: input.Removal.EnvironmentId, Directory: input.Removal.ExpectedVolumeDir,
		Source: workexecution.Input{
			ID:     input.Removal.CapturedSource.InputId,
			SHA256: hex.EncodeToString(input.Removal.CapturedSource.Sha256),
		},
		Report: hierarchyexecution.EnvironmentReport{Progress: next},
	}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.EnvironmentRemoval, nil
}

func (journal *Journal) AcknowledgeEnvironmentRemoval(ctx context.Context, input hierarchyexecution.EnvironmentInput,
	ack *agentpb.EnvironmentRemovalCheckpointAck,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.EnvironmentRemoval == nil || !workexecution.SameAssignment(state.Attempt, input.Attempt) ||
		state.Attempt.StartedAt == nil || input.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) ||
		!state.EnvironmentRemoval.matches(input) {
		return conflict()
	}
	if err := hierarchyexecution.ValidateEnvironmentCheckpointAck(input, state.EnvironmentRemoval.Report, ack); err != nil {
		return err
	}
	state.EnvironmentRemoval.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) EnvironmentRemoval(
	ctx context.Context,
	input hierarchyexecution.EnvironmentInput,
) (*EnvironmentRemovalRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if state.EnvironmentRemoval == nil || !workexecution.SameAssignment(state.Attempt, input.Attempt) ||
		state.Attempt.StartedAt == nil || input.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) ||
		!state.EnvironmentRemoval.matches(input) || state.EnvironmentRemoval.Report.Progress.Validate(input) != nil {
		return nil, conflict()
	}
	return state.EnvironmentRemoval, nil
}
