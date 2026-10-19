package agentworkjournal

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareLifecycle(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	next lifecycleexecution.Progress,
) (*LifecycleRecord, error) {
	if next.Validate(attempt, input) != nil || next.Sequence == 0 {
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
	var previous *lifecycleexecution.Progress
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
		if !matchesLifecycle(state, attempt, input) {
			return nil, conflict()
		}
		if state.Lifecycle.Completion != nil || state.Lifecycle.Failure != nil {
			return nil, conflict()
		}
		previous = &state.Lifecycle.Progress
		if lifecycleexecution.EqualProgress(*previous, next) {
			return state.Lifecycle, nil
		}
		if !state.Lifecycle.Acknowledged {
			return nil, conflict()
		}
	}
	if err := lifecycleexecution.ValidateTransition(attempt, input, previous, next); err != nil {
		return nil, err
	}
	stages, err := lifecycleexecution.Stages(input)
	if err != nil {
		return nil, err
	}
	var hook *LifecycleHookRecord
	if state.Lifecycle != nil {
		hook = state.Lifecycle.Hook
	}
	state.Lifecycle = &LifecycleRecord{Stages: stages, Progress: next, Hook: hook}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Lifecycle, nil
}

func (journal *Journal) Lifecycle(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) (*LifecycleRecord, error) {
	if attempt.Validate() != nil {
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
	if !matchesLifecycle(state, attempt, input) {
		return nil, conflict()
	}
	return state.Lifecycle, nil
}

func (journal *Journal) AcknowledgeLifecycle(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	checkpoint workexecution.Checkpoint,
) error {
	if attempt.Validate() != nil {
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
	if !matchesLifecycle(state, attempt, input) {
		return conflict()
	}
	expected, err := state.Lifecycle.Progress.Checkpoint(attempt, input)
	if err != nil || checkpoint.AttemptID != expected.AttemptID || checkpoint.Sequence != expected.Sequence ||
		checkpoint.StageID != expected.StageID || checkpoint.Evidence != expected.Evidence || !checkpoint.RecordedAt.Equal(expected.RecordedAt) {
		return conflict()
	}
	if state.Lifecycle.Acknowledged {
		return nil
	}
	state.Lifecycle.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func matchesLifecycle(
	state *attemptRecord,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
) bool {
	stages, err := lifecycleexecution.Stages(input)
	return err == nil && state != nil && state.Lifecycle != nil &&
		state.Lifecycle.Progress.Validate(attempt, input) == nil &&
		workexecution.SameAssignment(state.Attempt, attempt) &&
		state.Attempt.StartedAt != nil &&
		attempt.StartedAt != nil &&
		state.Attempt.StartedAt.Equal(*attempt.StartedAt) &&
		slices.Equal(state.Lifecycle.Stages, stages)
}

func (journal *Journal) AcknowledgeLifecycleCompletion(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	terminal workexecution.Attempt,
) error {
	if attempt.Validate() != nil || terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil {
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
	if !matchesLifecycle(state, attempt, input) {
		return conflict()
	}
	if previous := state.Lifecycle.Completion; previous != nil {
		if !workexecution.SameAssignment(*previous, terminal) || previous.Outcome != terminal.Outcome ||
			!previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result ||
			terminal.StartedAt == nil || !previous.StartedAt.Equal(*terminal.StartedAt) {
			return conflict()
		}
		return nil
	}
	state.Lifecycle.Completion = &terminal
	return journal.writeRecord(ctx, *state)
}
