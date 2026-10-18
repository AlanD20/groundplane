package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) AcknowledgeComponent(ctx context.Context, attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, checkpoint workexecution.Checkpoint) error {
	layout, err := componentexecution.CaptureLayout(attempt, input)
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
	if !matchesComponent(state, attempt, layout) {
		return conflict()
	}
	expected, err := state.Component.Progress.LayoutCheckpoint(attempt, layout)
	if err != nil || checkpoint.AttemptID != expected.AttemptID || checkpoint.Sequence != expected.Sequence ||
		checkpoint.StageID != expected.StageID || checkpoint.Evidence != expected.Evidence || !checkpoint.RecordedAt.Equal(expected.RecordedAt) {
		return conflict()
	}
	if state.Component.Acknowledged {
		return nil
	}
	state.Component.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) CompleteComponent(ctx context.Context, attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, terminal workexecution.Attempt) error {
	layout, err := componentexecution.CaptureLayout(attempt, input)
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
	if !matchesComponent(state, attempt, layout) {
		return conflict()
	}
	if saved := state.Component.Completion; saved != nil {
		if terminal.FinishedAt == nil || terminal.Result == nil || !workexecution.SameAssignment(*saved, terminal) ||
			terminal.StartedAt == nil || !saved.StartedAt.Equal(*terminal.StartedAt) ||
			!saved.FinishedAt.Equal(*terminal.FinishedAt) || saved.Outcome != terminal.Outcome || *saved.Result != *terminal.Result {
			return conflict()
		}
		return nil
	}
	state.Component.Completion = &terminal
	return journal.writeRecord(ctx, *state)
}
