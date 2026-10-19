package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareComponentRecovery(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	source componentexecution.Recovery,
	boundary componentexecution.Progress,
) (*ComponentRecoveryRecord, error) {
	restoration, err := componentexecution.CaptureRestoration(attempt, input)
	if err != nil {
		return nil, err
	}
	captured := ComponentRecoveryRecord{Restoration: restoration, Source: source, Boundary: boundary}
	if attempt.StartedAt == nil || captured.Report != nil || captured.Receipt != nil || captured.Failure != nil {
		return nil, invalid()
	}
	state := attemptRecord{Attempt: attempt, UpdatedAt: *attempt.StartedAt, ComponentRecovery: &captured}
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
		old := previous.ComponentRecovery
		if old == nil || !workexecution.SameAssignment(previous.Attempt, attempt) ||
			previous.Attempt.StartedAt == nil ||
			!previous.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
			old.Restoration != captured.Restoration ||
			!componentexecution.EqualProgress(old.Boundary, captured.Boundary) {
			return nil, conflict()
		}
		left, err := componentexecution.EncodeRecovery(old.Source, input)
		if err != nil {
			return nil, err
		}
		right, err := componentexecution.EncodeRecovery(captured.Source, input)
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
	return state.ComponentRecovery, nil
}

func (journal *Journal) SaveComponentRecoveryReport(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	report componentexecution.RecoveryReport,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.ComponentRecovery == nil || !matchesComponentRecovery(state, attempt, input) {
		return conflict()
	}
	recovery := state.ComponentRecovery
	if recovery.Failure != nil {
		return conflict()
	}
	raw, _, err := componentexecution.EncodeRestorationReport(
		state.Attempt,
		recovery.Source,
		recovery.Restoration,
		recovery.Boundary,
		report,
	)
	if err != nil {
		return err
	}
	if recovery.Report != nil {
		previous, _, err := componentexecution.EncodeRestorationReport(
			state.Attempt,
			recovery.Source,
			recovery.Restoration,
			recovery.Boundary,
			*recovery.Report,
		)
		if err != nil || !bytes.Equal(previous, raw) {
			return conflict()
		}
		return nil
	}
	recovery.Report, state.UpdatedAt = &report, report.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeComponentRecovery(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	terminal workexecution.Attempt,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.ComponentRecovery == nil || state.ComponentRecovery.Report == nil ||
		!matchesComponentRecovery(state, attempt, input) ||
		terminal.FinishedAt == nil ||
		terminal.Result == nil {
		return conflict()
	}
	if previous := state.ComponentRecovery.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) ||
			*previous.Result != *terminal.Result {
			return conflict()
		}
		return nil
	}
	state.ComponentRecovery.Receipt = &terminal
	return journal.writeRecord(ctx, *state)
}

func matchesComponentRecovery(
	state *attemptRecord,
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
) bool {
	if state == nil || state.ComponentRecovery == nil || !workexecution.SameAssignment(state.Attempt, attempt) ||
		state.Attempt.StartedAt == nil ||
		attempt.StartedAt == nil ||
		!state.Attempt.StartedAt.Equal(*attempt.StartedAt) {
		return false
	}
	restoration, err := componentexecution.CaptureRestoration(attempt, input)
	return err == nil && restoration == state.ComponentRecovery.Restoration
}
