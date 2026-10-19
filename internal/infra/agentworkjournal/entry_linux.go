package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareEntryConfiguration(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
	next entryexecution.Progress,
) (*EntryConfigurationRecord, error) {
	layout, err := entryexecution.CaptureLayout(attempt, input)
	if err != nil || next.ValidateLayout(attempt, layout) != nil || next.Sequence == 0 {
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
	var previous *entryexecution.Progress
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
		if !matchesEntryConfiguration(state, attempt, layout) || state.EntryConfiguration.Completion != nil || state.EntryConfiguration.Failure != nil {
			return nil, conflict()
		}
		previous = &state.EntryConfiguration.Progress
		if entryexecution.EqualProgress(*previous, next) {
			return state.EntryConfiguration, nil
		}
		if !state.EntryConfiguration.Acknowledged {
			return nil, conflict()
		}
	}
	if err := entryexecution.ValidateTransition(attempt, input, previous, next); err != nil {
		return nil, err
	}
	state.EntryConfiguration = &EntryConfigurationRecord{Layout: layout, Progress: next}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.EntryConfiguration, nil
}

func (journal *Journal) EntryConfiguration(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
) (*EntryConfigurationRecord, error) {
	layout, err := entryexecution.CaptureLayout(attempt, input)
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
	if !matchesEntryConfiguration(state, attempt, layout) {
		return nil, conflict()
	}
	return state.EntryConfiguration, nil
}

func matchesEntryConfiguration(state *attemptRecord, attempt workexecution.Attempt, layout entryexecution.Layout) bool {
	return state != nil && state.EntryConfiguration != nil &&
		sameEntryLayout(state.EntryConfiguration.Layout, layout) &&
		workexecution.SameAssignment(state.Attempt, attempt) &&
		state.Attempt.StartedAt != nil &&
		attempt.StartedAt != nil &&
		state.Attempt.StartedAt.Equal(*attempt.StartedAt)
}
