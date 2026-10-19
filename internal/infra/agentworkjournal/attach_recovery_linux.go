package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) SaveAttachInspection(
	ctx context.Context,
	input attachexecution.RecoveryInput,
	report attachexecution.RemovalInspection,
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
		if !matchesAttachRecovery(state, input) {
			return conflict()
		}
		expected, _, err := attachexecution.EncodeRemovalInspection(input, state.AttachRecovery.Report)
		provided, _, encodeErr := attachexecution.EncodeRemovalInspection(input, report)
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
		AttachRecovery: &AttachRecoveryRecord{Source: input.Source, Report: report}})
}

func (journal *Journal) LoadAttachInspection(
	ctx context.Context,
	input attachexecution.RecoveryInput,
) (*workexecution.Attempt, *AttachRecoveryRecord, error) {
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
	if !matchesAttachRecovery(state, input) {
		return nil, nil, conflict()
	}
	return &state.Attempt, state.AttachRecovery, nil
}

func (journal *Journal) AcknowledgeAttachInspection(
	ctx context.Context,
	input attachexecution.RecoveryInput,
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
	if !matchesAttachRecovery(state, input) {
		return conflict()
	}
	if previous := state.AttachRecovery.Receipt; previous != nil {
		if terminal.Result == nil || terminal.FinishedAt == nil || previous.Outcome != terminal.Outcome ||
			*previous.Result != *terminal.Result || !previous.FinishedAt.Equal(*terminal.FinishedAt) ||
			!workexecution.SameAssignment(*previous, terminal) {
			return conflict()
		}
		return nil
	}
	state.AttachRecovery.Receipt = &terminal
	state.Attempt.StartedAt = terminal.StartedAt
	return journal.writeRecord(ctx, *state)
}

func matchesAttachRecovery(state *attemptRecord, input attachexecution.RecoveryInput) bool {
	if state == nil || state.AttachRecovery == nil || !workexecution.SameAssignment(state.Attempt, input.Attempt) {
		return false
	}
	left, err := attachexecution.EncodeRemovalRecovery(state.AttachRecovery.Source)
	right, encodeErr := attachexecution.EncodeRemovalRecovery(input.Source)
	return err == nil && encodeErr == nil && bytes.Equal(left, right)
}
