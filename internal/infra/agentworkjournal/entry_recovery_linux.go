package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareEntryRecovery(ctx context.Context, attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput, source entryexecution.Recovery, boundary entryexecution.Progress) (*EntryRecoveryRecord, error) {
	restoration, err := entryexecution.CaptureRestoration(attempt, input)
	if err != nil {
		return nil, err
	}
	captured := EntryRecoveryRecord{Restoration: restoration, Source: source, Boundary: boundary}
	if attempt.StartedAt == nil || captured.Report != nil || captured.Receipt != nil {
		return nil, invalid()
	}
	state := attemptRecord{Attempt: attempt, UpdatedAt: *attempt.StartedAt, EntryRecovery: &captured}
	if err := state.validate(); err != nil {
		return nil, err
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	previous, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if previous != nil {
		old := previous.EntryRecovery
		if old == nil || !workexecution.SameAssignment(previous.Attempt, attempt) || previous.Attempt.StartedAt == nil || !previous.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
			!sameEntryRestoration(old.Restoration, captured.Restoration) || !entryexecution.EqualProgress(old.Boundary, captured.Boundary) {
			return nil, conflict()
		}
		left, err := entryexecution.EncodeRecovery(old.Source, input)
		if err != nil {
			return nil, err
		}
		right, err := entryexecution.EncodeRecovery(captured.Source, input)
		if err != nil || !bytes.Equal(left, right) {
			return nil, conflict()
		}
		return old, nil
	}
	attempts, err := journal.attempts(ctx)
	if err != nil {
		return nil, err
	}
	if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
		return nil, conflict()
	}
	if err := journal.writeRecord(ctx, state); err != nil {
		return nil, err
	}
	return state.EntryRecovery, nil
}

func (journal *Journal) SaveEntryRecoveryReport(ctx context.Context, attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput, report entryexecution.RecoveryReport) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.EntryRecovery == nil || !matchesEntryRecovery(state, attempt, input) {
		return conflict()
	}
	recovery := state.EntryRecovery
	raw, _, err := entryexecution.EncodeRestorationReport(state.Attempt, recovery.Source, recovery.Restoration, recovery.Boundary, report)
	if err != nil {
		return err
	}
	if recovery.Report != nil {
		previous, _, err := entryexecution.EncodeRestorationReport(state.Attempt, recovery.Source, recovery.Restoration, recovery.Boundary, *recovery.Report)
		if err != nil || !bytes.Equal(previous, raw) {
			return conflict()
		}
		return nil
	}
	recovery.Report, state.UpdatedAt = &report, report.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeEntryRecovery(ctx context.Context, attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput, terminal workexecution.Attempt) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.EntryRecovery == nil || state.EntryRecovery.Report == nil || !matchesEntryRecovery(state, attempt, input) || terminal.FinishedAt == nil || terminal.Result == nil {
		return conflict()
	}
	if previous := state.EntryRecovery.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result {
			return conflict()
		}
		return nil
	}
	state.EntryRecovery.Receipt = &terminal
	return journal.writeRecord(ctx, *state)
}

func matchesEntryRecovery(state *attemptRecord, attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput) bool {
	if state == nil || state.EntryRecovery == nil || !workexecution.SameAssignment(state.Attempt, attempt) || state.Attempt.StartedAt == nil || attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return false
	}
	restoration, err := entryexecution.CaptureRestoration(attempt, input)
	return err == nil && sameEntryRestoration(restoration, state.EntryRecovery.Restoration)
}
