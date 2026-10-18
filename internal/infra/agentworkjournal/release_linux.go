package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// PrepareRelease persists the report before transport. A replay retains its
// sequence; a different report cannot overtake an unacknowledged Script hook.
func (journal *Journal) PrepareRelease(ctx context.Context, input releaseexecution.Input, next releaseexecution.Progress) (*releaseexecution.CheckpointReport, error) {
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
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) || input.Attempt.StartedAt == nil || state.Attempt.StartedAt == nil ||
		!state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) {
		return nil, conflict()
	}
	var prior *releaseexecution.Progress
	if state.Release != nil {
		prior = &state.Release.Progress
	}
	if err := releaseexecution.ValidateTransition(input, prior, next); err != nil {
		return nil, err
	}
	if prior != nil && releaseexecution.EqualProgress(*prior, next) {
		if state.Release.Sequence != state.Sequence {
			return nil, conflict()
		}
		return state.Release, nil
	}
	pending, err := state.pending()
	if err != nil {
		return nil, err
	}
	if pending || state.Sequence == ^uint64(0) || next.UpdatedAt.Before(state.UpdatedAt) || prior == nil && state.Sequence != 0 {
		return nil, conflict()
	}
	if state.Effect != nil && state.Effect.Boundary.Sequence == state.Sequence && !next.InFlight &&
		(next.Evidence == nil || *next.Evidence != state.Effect.Reference) {
		return nil, conflict()
	}
	stage := next
	if prior != nil && !next.InFlight {
		stage = *prior
	}
	stageID, err := stage.StageID(input)
	if err != nil {
		return nil, err
	}
	state.Sequence++
	state.UpdatedAt = next.UpdatedAt
	state.Release = &releaseexecution.CheckpointReport{Sequence: state.Sequence, StageID: stageID, Progress: next}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Release, nil
}

func (journal *Journal) AcknowledgeRelease(ctx context.Context, ack workexecution.Checkpoint) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, ack.AttemptID)
	if err != nil {
		return err
	}
	if state == nil || state.Release == nil {
		return conflict()
	}
	expected, err := state.Release.Checkpoint(state.Attempt)
	if err != nil {
		return err
	}
	if !releaseexecution.EqualCheckpoint(expected, ack) {
		return conflict()
	}
	state.Release.Ack = &ack
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) Release(ctx context.Context, input releaseexecution.Input) (*releaseexecution.CheckpointReport, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) {
		return nil, conflict()
	}
	if state.Release != nil {
		if err := state.Release.Progress.Validate(input); err != nil {
			return nil, err
		}
	}
	return state.Release, nil
}
