package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// LoadEnvironmentRemoval restores saved Start and progress before attempting
// another Start RPC. A settled Controller attempt need not grant Start again.
func (journal *Journal) LoadEnvironmentRemoval(
	ctx context.Context,
	input hierarchyexecution.EnvironmentInput,
) (*workexecution.Attempt, *EnvironmentRemovalRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, nil, err
	}
	if state.EnvironmentRemoval == nil || !workexecution.SameAssignment(state.Attempt, input.Attempt) ||
		!state.EnvironmentRemoval.matches(input) {
		return nil, nil, conflict()
	}
	return &state.Attempt, state.EnvironmentRemoval, nil
}

func (journal *Journal) CompleteEnvironmentRemoval(ctx context.Context, input hierarchyexecution.EnvironmentInput,
	ack *agentpb.EnvironmentRemovalCompletionAck,
) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.EnvironmentRemoval == nil || !state.EnvironmentRemoval.Acknowledged ||
		!workexecution.SameAssignment(state.Attempt, input.Attempt) || !state.EnvironmentRemoval.matches(input) ||
		state.Attempt.StartedAt == nil || input.Attempt.StartedAt == nil || !state.Attempt.StartedAt.Equal(*input.Attempt.StartedAt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal, err := hierarchyexecution.DecodeEnvironmentCompletion(
		input,
		state.EnvironmentRemoval.Report.Progress,
		ack,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if prior := state.EnvironmentRemoval.Completion; prior != nil &&
		(!prior.FinishedAt.Equal(*terminal.FinishedAt) || *prior.Result != *terminal.Result || prior.Outcome != terminal.Outcome) {
		return workexecution.Attempt{}, conflict()
	}
	state.EnvironmentRemoval.Completion = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
