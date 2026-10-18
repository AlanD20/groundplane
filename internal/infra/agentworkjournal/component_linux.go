package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareComponent(ctx context.Context, attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, next componentexecution.Progress) (*ComponentRecord, error) {
	layout, err := componentexecution.CaptureLayout(attempt, input)
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
	var previous *componentexecution.Progress
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
		if !matchesComponent(state, attempt, layout) || state.Component.Completion != nil || state.Component.Failure != nil {
			return nil, conflict()
		}
		previous = &state.Component.Progress
		if componentexecution.EqualProgress(*previous, next) {
			return state.Component, nil
		}
		if !state.Component.Acknowledged {
			return nil, conflict()
		}
	}
	if err := componentexecution.ValidateTransition(attempt, input, previous, next); err != nil {
		return nil, err
	}
	state.Component = &ComponentRecord{Layout: layout, Progress: next}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Component, nil
}

func (journal *Journal) Component(ctx context.Context, attempt workexecution.Attempt, input *agentpb.ComponentWorkInput) (*ComponentRecord, error) {
	layout, err := componentexecution.CaptureLayout(attempt, input)
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
	if !matchesComponent(state, attempt, layout) {
		return nil, conflict()
	}
	return state.Component, nil
}

func matchesComponent(state *attemptRecord, attempt workexecution.Attempt, layout componentexecution.Layout) bool {
	return state != nil && state.Component != nil && state.Component.Layout == layout &&
		workexecution.SameAssignment(state.Attempt, attempt) && state.Attempt.StartedAt != nil &&
		attempt.StartedAt != nil && state.Attempt.StartedAt.Equal(*attempt.StartedAt)
}
