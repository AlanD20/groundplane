package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) AcknowledgeEntryConfiguration(ctx context.Context, attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput, checkpoint workexecution.Checkpoint) error {
	layout, err := entryexecution.CaptureLayout(attempt, input)
	if err != nil {
		return err
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
	if !matchesEntryConfiguration(state, attempt, layout) {
		return conflict()
	}
	expected, err := state.EntryConfiguration.Progress.LayoutCheckpoint(attempt, layout)
	if err != nil || checkpoint.AttemptID != expected.AttemptID || checkpoint.Sequence != expected.Sequence ||
		checkpoint.StageID != expected.StageID || checkpoint.Evidence != expected.Evidence || !checkpoint.RecordedAt.Equal(expected.RecordedAt) {
		return conflict()
	}
	if state.EntryConfiguration.Acknowledged {
		return nil
	}
	state.EntryConfiguration.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) CompleteEntryConfiguration(ctx context.Context, attempt workexecution.Attempt, input *agentpb.EntryConfigurationWorkInput, terminal workexecution.Attempt) error {
	layout, err := entryexecution.CaptureLayout(attempt, input)
	if err != nil {
		return err
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
	if !matchesEntryConfiguration(state, attempt, layout) {
		return conflict()
	}
	if saved := state.EntryConfiguration.Completion; saved != nil {
		if terminal.FinishedAt == nil || terminal.Result == nil || !workexecution.SameAssignment(*saved, terminal) ||
			terminal.StartedAt == nil || !saved.StartedAt.Equal(*terminal.StartedAt) ||
			!saved.FinishedAt.Equal(*terminal.FinishedAt) || saved.Outcome != terminal.Outcome || *saved.Result != *terminal.Result {
			return conflict()
		}
		return nil
	}
	state.EntryConfiguration.Completion = &terminal
	return journal.writeRecord(ctx, *state)
}
