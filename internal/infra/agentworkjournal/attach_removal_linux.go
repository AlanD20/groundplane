package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareAttachRemoval(ctx context.Context, input attachexecution.RemovalInput,
	next attachexecution.RemovalProgress,
) (*AttachRemovalRecord, error) {
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
	if state.AttachFailure != nil {
		return nil, conflict()
	}
	var previous *attachexecution.RemovalProgress
	if saved := state.AttachRemoval; saved != nil {
		if !saved.matches(input) || saved.Completion != nil {
			return nil, conflict()
		}
		previous = &saved.Report.Progress
		requested, err := attachexecution.EncodeRemovalReport(input, attachexecution.RemovalReport{Progress: next})
		if err != nil {
			return nil, err
		}
		current, err := attachexecution.EncodeRemovalReport(input, saved.Report)
		if err != nil {
			return nil, err
		}
		if bytes.Equal(current, requested) {
			return saved, nil
		}
		if !saved.Acknowledged {
			return nil, conflict()
		}
		if saved.Report.Progress.InFlight {
			// Do not discard process identities until the successful command result
			// has been saved. A caller-supplied Confirm cannot erase unknown effects.
			result := saved.CommandResult
			if result == nil || next.LastResult == nil ||
				!attachexecution.SameRemovalCommandResult(*result, *next.LastResult) {
				return nil, conflict()
			}
		}
	} else if state.Sequence != 0 {
		return nil, conflict()
	}
	if err := attachexecution.ValidateRemovalTransition(input, previous, next); err != nil {
		return nil, err
	}
	authority, err := input.Authority()
	if err != nil {
		return nil, err
	}
	state.AttachRemoval = &AttachRemovalRecord{
		Authority: authority,
		Report:    attachexecution.RemovalReport{Progress: next},
	}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.AttachRemoval, nil
}

func (journal *Journal) AcknowledgeAttachRemoval(ctx context.Context, input attachexecution.RemovalInput,
	ack *agentpb.AttachRemovalCheckpointAck,
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
	if !matchesAttachState(state, input) {
		return conflict()
	}
	if err := attachexecution.ValidateRemovalCheckpointAck(input, state.AttachRemoval.Report, ack); err != nil {
		return err
	}
	state.AttachRemoval.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AttachRemoval(
	ctx context.Context,
	input attachexecution.RemovalInput,
) (*AttachRemovalRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !matchesAttachState(state, input) || state.AttachRemoval.Report.Progress.Validate(input) != nil {
		return nil, conflict()
	}
	return state.AttachRemoval, nil
}

func matchesAttachState(state *attemptRecord, input attachexecution.RemovalInput) bool {
	return state != nil && state.AttachRemoval != nil && workexecution.SameAssignment(state.Attempt, input.Attempt) &&
		state.Attempt.StartedAt != nil && input.Attempt.StartedAt != nil && state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) &&
		state.AttachRemoval.matches(input)
}
