package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) CompleteAttachRemoval(ctx context.Context, input attachexecution.RemovalInput,
	ack *agentpb.AttachRemovalCompletionAck,
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
	if !matchesAttachState(state, input) || !state.AttachRemoval.Acknowledged {
		return workexecution.Attempt{}, conflict()
	}
	terminal, err := attachexecution.DecodeRemovalCompletion(input, state.AttachRemoval.Report.Progress, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := state.AttachRemoval.Completion; previous != nil {
		if !previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	state.AttachRemoval.Completion = &terminal
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}

// LoadAttachRemoval reconstructs the exact saved Start time before completion
// replay; a fresh assignment does not carry started or terminal timestamps.
func (journal *Journal) LoadAttachRemoval(ctx context.Context, input attachexecution.RemovalInput) (*workexecution.Attempt, *AttachRemovalRecord, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) || state.AttachRemoval == nil || state.Attempt.StartedAt == nil ||
		!state.AttachRemoval.matches(input) {
		return nil, nil, conflict()
	}
	return &state.Attempt, state.AttachRemoval, nil
}
