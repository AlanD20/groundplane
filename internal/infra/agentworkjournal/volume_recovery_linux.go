package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) PrepareVolumeRecovery(ctx context.Context, attempt workexecution.Attempt, captured VolumeRecoveryRecord) (*VolumeRecoveryRecord, error) {
	if attempt.StartedAt == nil || captured.Report != nil || captured.Receipt != nil || captured.Failure != nil {
		return nil, invalid()
	}
	state := attemptRecord{Attempt: attempt, UpdatedAt: *attempt.StartedAt, VolumeRecovery: &captured}
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
		old := previous.VolumeRecovery
		if old == nil || !workexecution.SameAssignment(previous.Attempt, attempt) || previous.Attempt.StartedAt == nil || !previous.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
			old.Action != captured.Action || old.VolumeID != captured.VolumeID || !volumeexecution.EqualProgress(old.Boundary, captured.Boundary) {
			return nil, conflict()
		}
		left, err := volumeexecution.EncodeRecovery(old.Source)
		if err != nil {
			return nil, err
		}
		right, err := volumeexecution.EncodeRecovery(captured.Source)
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
	return state.VolumeRecovery, nil
}

func (journal *Journal) SaveVolumeRecoveryReport(ctx context.Context, attempt workexecution.Attempt, report volumeexecution.RecoveryReport) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.VolumeRecovery == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return conflict()
	}
	recovery := state.VolumeRecovery
	if recovery.Failure != nil {
		return conflict()
	}
	raw, _, err := volumeexecution.EncodeRecoveryReport(state.Attempt, recovery.Source, recovery.Action, recovery.VolumeID, recovery.Boundary, report)
	if err != nil {
		return err
	}
	if recovery.Report != nil {
		previous, _, err := volumeexecution.EncodeRecoveryReport(state.Attempt, recovery.Source, recovery.Action, recovery.VolumeID, recovery.Boundary, *recovery.Report)
		if err != nil || !bytes.Equal(previous, raw) {
			return conflict()
		}
		return nil
	}
	recovery.Report, state.UpdatedAt = &report, report.Observation.At
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeVolumeRecovery(ctx context.Context, attempt, terminal workexecution.Attempt) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.VolumeRecovery == nil || state.VolumeRecovery.Report == nil || !workexecution.SameAssignment(state.Attempt, attempt) || terminal.FinishedAt == nil || terminal.Result == nil {
		return conflict()
	}
	if previous := state.VolumeRecovery.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result {
			return conflict()
		}
		return nil
	}
	state.VolumeRecovery.Receipt = &terminal
	return journal.writeRecord(ctx, *state)
}
