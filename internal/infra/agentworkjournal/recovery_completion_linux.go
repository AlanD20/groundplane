package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) RecoveryCompletion(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
) (*releaseexecution.CompletionReceipt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Input.Attempt) || state.Recovery == nil ||
		state.Recovery.Forward.ID != input.Forward.ID || input.Forward.Result == nil ||
		*state.Recovery.Forward.Result != *input.Forward.Result {
		return nil, conflict()
	}
	return state.Recovery.Completion, nil
}

func (journal *Journal) AcknowledgeRecoveryCompletion(
	ctx context.Context,
	input releaseexecution.RecoveryInput,
	ack *agentpb.ReleaseWorkCompletionAck,
) error {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Input.Attempt.ID)
	if err != nil {
		return err
	}
	if state == nil || state.Recovery == nil || !workexecution.SameAssignment(state.Attempt, input.Input.Attempt) ||
		state.Recovery.Forward.ID != input.Forward.ID || input.Forward.Result == nil ||
		*state.Recovery.Forward.Result != *input.Forward.Result {
		return conflict()
	}
	input.Input.Attempt = state.Attempt
	receipt, err := releaseexecution.DecodeRecoveryCompletionAck(input, state.Recovery.Report, ack)
	if err != nil {
		return err
	}
	if prior := state.Recovery.Completion; prior != nil {
		if prior.Outcome != receipt.Outcome || !prior.FinishedAt.Equal(receipt.FinishedAt) {
			return conflict()
		}
		return nil
	}
	state.Recovery.Completion = &receipt
	return journal.writeRecord(ctx, *state)
}
