package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareAttachProvision(ctx context.Context, input attachexecution.ProvisionInput,
	next attachexecution.ProvisionProgress,
) (*AttachProvisionRecord, error) {
	authority, err := input.Authority()
	if err != nil || next.Validate(input.Attempt, authority) != nil || next.Sequence == 0 {
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
	if state.AttachProvisionFailure != nil {
		return nil, conflict()
	}
	var previous *attachexecution.ProvisionProgress
	if saved := state.AttachProvision; saved != nil {
		if saved.Authority != authority || saved.Completion != nil {
			return nil, conflict()
		}
		previous = &saved.Report.Progress
		requested, err := attachexecution.EncodeProvisionReport(input, attachexecution.ProvisionReport{Progress: next})
		if err != nil {
			return nil, err
		}
		current, err := attachexecution.EncodeProvisionReport(input, saved.Report)
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
			// A caller-supplied Confirm cannot erase unknown effects. Require
			// the successful command result to have been saved first.
			result := saved.CommandResult
			if result == nil || next.LastResult == nil ||
				!attachexecution.SameProvisionCommandResult(*result, *next.LastResult) {
				return nil, conflict()
			}
		}
	} else if state.Sequence != 0 {
		return nil, conflict()
	}
	if err := attachexecution.ValidateProvisionTransition(input.Attempt, authority, previous, next); err != nil {
		return nil, err
	}
	state.AttachProvision = &AttachProvisionRecord{
		Authority: authority,
		Report:    attachexecution.ProvisionReport{Progress: next},
	}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.AttachProvision, nil
}

func (journal *Journal) AcknowledgeAttachProvision(ctx context.Context, input attachexecution.ProvisionInput,
	ack *agentpb.AttachProvisionCheckpointAck,
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
	if !matchesAttachProvisionState(state, input) {
		return conflict()
	}
	if err := attachexecution.ValidateProvisionCheckpointAck(input, state.AttachProvision.Report, ack); err != nil {
		return err
	}
	state.AttachProvision.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AttachProvision(
	ctx context.Context,
	input attachexecution.ProvisionInput,
) (*AttachProvisionRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !matchesAttachProvisionState(state, input) ||
		state.AttachProvision.Report.Progress.Validate(input.Attempt, state.AttachProvision.Authority) != nil {
		return nil, conflict()
	}
	return state.AttachProvision, nil
}

func matchesAttachProvisionState(state *attemptRecord, input attachexecution.ProvisionInput) bool {
	authority, err := input.Authority()
	return err == nil && state != nil && state.AttachProvision != nil &&
		workexecution.SameAssignment(state.Attempt, input.Attempt) &&
		state.Attempt.StartedAt != nil &&
		input.Attempt.StartedAt != nil &&
		state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) &&
		state.AttachProvision.Authority == authority
}

// LoadAttachProvision restores the original start time before report replay.
// Redelivery does not replace the saved authority or create a new execution.
func (journal *Journal) LoadAttachProvision(
	ctx context.Context,
	input attachexecution.ProvisionInput,
) (*workexecution.Attempt, *AttachProvisionRecord, error) {
	authority, err := input.Authority()
	if err != nil {
		return nil, nil, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) || state.AttachProvision == nil ||
		state.Attempt.StartedAt == nil || state.AttachProvision.Authority != authority {
		return nil, nil, conflict()
	}
	return &state.Attempt, state.AttachProvision, nil
}
