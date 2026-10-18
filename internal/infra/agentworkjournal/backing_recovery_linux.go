package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareBackingRecovery(ctx context.Context, assignment backingprovision.Assignment) (*BackingRecoveryRecord, error) {
	if assignment.Validate() != nil || assignment.Recovery == nil || assignment.Attempt.StartedAt == nil {
		return nil, invalid()
	}
	layout, err := backingprovision.CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil {
		return nil, err
	}
	captured := BackingRecoveryRecord{Layout: layout, Source: *assignment.Recovery}
	state := attemptRecord{Attempt: assignment.Attempt, UpdatedAt: *assignment.Attempt.StartedAt, BackingRecovery: &captured}
	if err := state.validate(); err != nil {
		return nil, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	previous, err := journal.readRecord(ctx, assignment.Attempt.ID)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		if !matchesBackingRecovery(previous, assignment, layout) {
			return nil, conflict()
		}
		return previous.BackingRecovery, nil
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return nil, err
	}
	if _, reserved := attempts[assignment.Attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
		return nil, conflict()
	}
	if err := journal.writeRecord(ctx, state); err != nil {
		return nil, err
	}
	return state.BackingRecovery, nil
}

func matchesBackingRecovery(state *attemptRecord, assignment backingprovision.Assignment, layout backingprovision.Layout) bool {
	if state == nil || state.BackingRecovery == nil || assignment.Recovery == nil || assignment.Attempt.StartedAt == nil ||
		state.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*assignment.Attempt.StartedAt) ||
		!workexecution.SameAssignment(state.Attempt, assignment.Attempt) || state.BackingRecovery.Layout != layout {
		return false
	}
	left, err := backingprovision.EncodeRecovery(state.BackingRecovery.Source, layout)
	if err != nil {
		return false
	}
	right, err := backingprovision.EncodeRecovery(*assignment.Recovery, layout)
	return err == nil && bytes.Equal(left, right)
}

func (journal *Journal) SaveBackingRecoveryReport(ctx context.Context, assignment backingprovision.Assignment, report backingprovision.RecoveryReport) error {
	layout, err := backingprovision.CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil || assignment.Recovery == nil {
		return invalid()
	}
	raw, _, err := backingprovision.EncodeRecoveryReport(assignment.Attempt, *assignment.Recovery, layout, report)
	if err != nil {
		return err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, assignment.Attempt.ID)
	if err != nil {
		return err
	}
	if !matchesBackingRecovery(state, assignment, layout) {
		return conflict()
	}
	recovery := state.BackingRecovery
	if previous := recovery.Report; previous != nil {
		saved, _, err := backingprovision.EncodeRecoveryReport(assignment.Attempt, recovery.Source, layout, *previous)
		if err != nil || !bytes.Equal(raw, saved) {
			return conflict()
		}
		return nil
	}
	if recovery.Receipt != nil || report.ObservedAt.Before(state.UpdatedAt) {
		return conflict()
	}
	recovery.Report, state.UpdatedAt = &report, report.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeBackingRecovery(ctx context.Context, assignment backingprovision.Assignment,
	ack *agentpb.BackingProvisionRecoveryAck,
) (workexecution.Attempt, error) {
	layout, err := backingprovision.CaptureLayout(assignment.Attempt, assignment.Procedure)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, assignment.Attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if !matchesBackingRecovery(state, assignment, layout) || state.BackingRecovery.Report == nil {
		return workexecution.Attempt{}, conflict()
	}
	recovery := state.BackingRecovery
	terminal, err := backingprovision.DecodeRecoveryAck(assignment.Attempt, recovery.Source, layout, *recovery.Report, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := recovery.Receipt; previous != nil {
		if !sameBackingReceipt(previous, terminal) {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	recovery.Receipt = &terminal
	return terminal, journal.writeRecord(ctx, *state)
}
