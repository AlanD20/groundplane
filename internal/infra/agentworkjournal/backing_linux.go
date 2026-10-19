package agentworkjournal

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareBacking(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, next backingprovision.Progress,
) (*BackingRecord, error) {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil || next.Validate(attempt, layout) != nil || next.Sequence == 0 {
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
	var previous *backingprovision.Progress
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: attempt, Backing: &BackingRecord{Layout: layout}}
	} else {
		if !matchesBacking(state, attempt, layout) || state.Backing.Completion != nil || state.Backing.Failure != nil {
			return nil, conflict()
		}
		previous = state.Backing.Progress
		if previous != nil && backingprovision.EqualProgress(*previous, next) {
			return state.Backing, nil
		}
		if previous != nil && !state.Backing.Acknowledged {
			return nil, conflict()
		}
	}
	if err := backingprovision.ValidateTransition(attempt, layout, previous, next); err != nil {
		return nil, err
	}
	if previous != nil && previous.InFlight && previous.Stage == backingprovision.AfterStart {
		hook := state.Backing.Hook
		if hook == nil || hook.Result == nil || next.Last == nil || *hook.Result != *next.Last {
			return nil, conflict()
		}
	}
	if next.Stage == backingprovision.Complete && (state.Backing.Runtime == nil || next.Last == nil ||
		state.Backing.Runtime.ContainerID != next.Last.ContainerID || !state.Backing.Runtime.ObservedAt.Equal(next.Last.At)) {
		return nil, conflict()
	}
	state.Backing.Progress, state.Backing.Acknowledged = &next, false
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Backing, nil
}

func (journal *Journal) Backing(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput,
) (*BackingRecord, error) {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil {
		return nil, err
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
	if !matchesBacking(state, attempt, layout) {
		return nil, conflict()
	}
	return state.Backing, nil
}

func (journal *Journal) AcknowledgeBacking(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, ack *agentpb.BackingProvisionCheckpointAck,
) error {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil || ack == nil || ack.Evidence == nil {
		return invalid()
	}
	claimed, err := workexecution.DecodeAgentAttempt(ack.Attempt, attempt.Executor)
	if err != nil || !workexecution.SameAssignment(claimed, attempt) {
		return invalid()
	}
	progress, err := backingprovision.DecodeProgress(attempt, layout, ack.Progress)
	if err != nil {
		return err
	}
	checkpoint, err := progress.Checkpoint(attempt, layout)
	if err != nil || checkpoint.Evidence.ID != ack.Evidence.InputId ||
		checkpoint.Evidence.SHA256 != hex.EncodeToString(ack.Evidence.Sha256) {
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
	if !matchesBacking(state, attempt, layout) || state.Backing.Progress == nil ||
		!backingprovision.EqualProgress(
			*state.Backing.Progress,
			progress,
		) || state.Backing.Completion != nil || state.Backing.Failure != nil {
		return conflict()
	}
	if state.Backing.Acknowledged {
		return nil
	}
	state.Backing.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func matchesBacking(state *attemptRecord, attempt workexecution.Attempt, layout backingprovision.Layout) bool {
	if state == nil || state.Backing == nil || state.Backing.Layout != layout ||
		!workexecution.SameAssignment(state.Attempt, attempt) ||
		(state.Attempt.StartedAt == nil) != (attempt.StartedAt == nil) {
		return false
	}
	return attempt.StartedAt == nil || state.Attempt.StartedAt.Equal(*attempt.StartedAt)
}
