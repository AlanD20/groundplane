package agentworkjournal

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

func (journal *Journal) PrepareLifecycleRecovery(ctx context.Context, attempt workexecution.Attempt, captured LifecycleRecoveryRecord) (*LifecycleRecoveryRecord, error) {
	if attempt.StartedAt == nil || captured.Report != nil || captured.Receipt != nil {
		return nil, invalid()
	}
	state := attemptRecord{Attempt: attempt, UpdatedAt: *attempt.StartedAt, LifecycleRecovery: &captured}
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
		old := previous.LifecycleRecovery
		if old == nil || !workexecution.SameAssignment(previous.Attempt, attempt) || previous.Attempt.StartedAt == nil || !previous.Attempt.StartedAt.Equal(*attempt.StartedAt) ||
			!slices.Equal(old.Stages, captured.Stages) {
			return nil, conflict()
		}
		left, err := json.Marshal(old.Source)
		if err != nil {
			return nil, err
		}
		right, err := json.Marshal(captured.Source)
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
	return state.LifecycleRecovery, nil
}

func (journal *Journal) SaveLifecycleRecoveryReport(ctx context.Context, attempt workexecution.Attempt, report lifecycleexecution.RecoveryReport) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.LifecycleRecovery == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return conflict()
	}
	recovery := state.LifecycleRecovery
	raw, _, err := lifecycleexecution.EncodeRecoveryReportStages(state.Attempt, recovery.Stages, recovery.Source, report)
	if err != nil {
		return err
	}
	if recovery.Report != nil {
		previous, _, err := lifecycleexecution.EncodeRecoveryReportStages(state.Attempt, recovery.Stages, recovery.Source, *recovery.Report)
		if err != nil || !bytes.Equal(previous, raw) {
			return conflict()
		}
		return nil
	}
	recovery.Report, state.UpdatedAt = &report, report.ObservedAt
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) AcknowledgeLifecycleRecovery(ctx context.Context, attempt, terminal workexecution.Attempt) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.LifecycleRecovery == nil || state.LifecycleRecovery.Report == nil || !workexecution.SameAssignment(state.Attempt, attempt) || terminal.FinishedAt == nil || terminal.Result == nil {
		return conflict()
	}
	if previous := state.LifecycleRecovery.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || !previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result {
			return conflict()
		}
		return nil
	}
	state.LifecycleRecovery.Receipt = &terminal
	return journal.writeRecord(ctx, *state)
}
