package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) SaveProvisionInspection(
	ctx context.Context,
	input attachexecution.ProvisionRecoveryInput,
	report attachexecution.ProvisionInspection,
) error {
	if report.Validate(input) != nil {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return err
	}
	if state != nil {
		if !matchesProvisionRecovery(state, input) {
			return conflict()
		}
		expected, _, err := attachexecution.EncodeProvisionInspection(input, state.AttachProvisionRecovery.Report)
		provided, _, encodeErr := attachexecution.EncodeProvisionInspection(input, report)
		if err != nil || encodeErr != nil || !bytes.Equal(expected, provided) {
			return conflict()
		}
		return nil
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return err
	}
	if _, reserved := attempts[input.Attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
		return conflict()
	}
	return journal.writeRecord(ctx, attemptRecord{Attempt: input.Attempt, Sequence: 1, UpdatedAt: report.ObservedAt,
		AttachProvisionRecovery: &AttachProvisionRecoveryRecord{Source: input.Source, Report: report}})
}

func (journal *Journal) LoadProvisionInspection(
	ctx context.Context,
	input attachexecution.ProvisionRecoveryInput,
) (*workexecution.Attempt, *AttachProvisionRecoveryRecord, error) {
	if input.Validate() != nil {
		return nil, nil, invalid()
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
	if !matchesProvisionRecovery(state, input) {
		return nil, nil, conflict()
	}
	return &state.Attempt, state.AttachProvisionRecovery, nil
}

func (journal *Journal) AcknowledgeProvisionInspection(
	ctx context.Context,
	input attachexecution.ProvisionRecoveryInput,
	terminal workexecution.Attempt,
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
	if !matchesProvisionRecovery(state, input) {
		return conflict()
	}
	if previous := state.AttachProvisionRecovery.Receipt; previous != nil {
		if terminal.Result == nil || terminal.FinishedAt == nil || previous.Outcome != terminal.Outcome ||
			*previous.Result != *terminal.Result || !previous.FinishedAt.Equal(*terminal.FinishedAt) ||
			!workexecution.SameAssignment(
				*previous,
				terminal,
			) || (previous.StartedAt == nil) != (terminal.StartedAt == nil) ||
			previous.StartedAt != nil && !previous.StartedAt.Equal(*terminal.StartedAt) {
			return conflict()
		}
		return nil
	}
	state.AttachProvisionRecovery.Receipt = &terminal
	state.Attempt.StartedAt = terminal.StartedAt
	return journal.writeRecord(ctx, *state)
}

func matchesProvisionRecovery(state *attemptRecord, input attachexecution.ProvisionRecoveryInput) bool {
	if state == nil || state.AttachProvisionRecovery == nil ||
		!workexecution.SameAssignment(state.Attempt, input.Attempt) {
		return false
	}
	left, err := attachexecution.EncodeProvisionRecovery(state.AttachProvisionRecovery.Source)
	right, encodeErr := attachexecution.EncodeProvisionRecovery(input.Source)
	return err == nil && encodeErr == nil && bytes.Equal(left, right)
}
