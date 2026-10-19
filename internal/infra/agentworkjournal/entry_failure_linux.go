package agentworkjournal

import (
	"bytes"
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) AcknowledgeEntryConfigurationFailure(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput, terminal workexecution.Attempt, evidence []byte,
) error {
	layout, err := entryexecution.CaptureLayout(attempt, input)
	if err != nil || terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
		terminal.Outcome == workexecution.Succeeded || !workexecution.SameAssignment(attempt, terminal) ||
		attempt.StartedAt == nil || terminal.StartedAt == nil || !attempt.StartedAt.Equal(*terminal.StartedAt) {
		return invalid()
	}
	captured, err := entryexecution.DecodeLayoutFailure(terminal, layout, evidence, *terminal.Result)
	if err != nil || !captured.RecordedAt.Equal(*terminal.FinishedAt) {
		return invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return err
	}
	if !matchesEntryConfiguration(state, attempt, layout) || state.EntryConfiguration.Failure == nil ||
		terminal.FinishedAt.Before(state.UpdatedAt) {
		return conflict()
	}
	failure := state.EntryConfiguration.Failure
	if failure.Reason != captured.Reason {
		return conflict()
	}
	if previous := failure.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || *previous.Result != *terminal.Result ||
			!previous.FinishedAt.Equal(*terminal.FinishedAt) || !bytes.Equal(failure.Evidence, evidence) {
			return conflict()
		}
		return nil
	}
	failure.Receipt, failure.Evidence = &terminal, bytes.Clone(evidence)
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) PrepareEntryConfigurationFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.EntryConfigurationWorkInput,
	reason entryexecution.FailureReason,
) (*EntryConfigurationFailureDelivery, error) {
	layout, err := entryexecution.CaptureLayout(attempt, input)
	if err != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil || !reason.Valid() {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return nil, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		progress, err := entryexecution.NewProgress(attempt, input, *attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: progress.UpdatedAt,
			EntryConfiguration: &EntryConfigurationRecord{Layout: layout, Progress: progress}}
	}
	if !matchesEntryConfiguration(state, attempt, layout) || state.EntryConfiguration.Completion != nil ||
		state.EntryConfiguration.Progress.Stage == entryexecution.Complete {
		return nil, conflict()
	}
	if state.EntryConfiguration.Failure != nil {
		return state.EntryConfiguration.Failure, nil
	}
	state.EntryConfiguration.Failure = &EntryConfigurationFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.EntryConfiguration.Failure, nil
}
