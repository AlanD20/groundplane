package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ReleaseCompletion retains enough identity to redeliver completion after a
// reconnect without asking Start to authorize an already-finished attempt.
type ReleaseCompletion struct {
	Attempt workexecution.Attempt
	Report  releaseexecution.CheckpointReport
	Receipt *releaseexecution.CompletionReceipt
}

func (journal *Journal) LoadReleaseCompletion(
	ctx context.Context,
	input releaseexecution.Input,
) (*ReleaseCompletion, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, input.Attempt) {
		return nil, conflict()
	}
	if state.Release == nil || state.Release.Progress.Phase != releaseexecution.Complete || state.Release.Ack == nil {
		return nil, nil
	}
	input.Attempt = state.Attempt
	if state.Release.Progress.Validate(input) != nil {
		return nil, conflict()
	}
	return &ReleaseCompletion{Attempt: state.Attempt, Report: *state.Release, Receipt: state.Completion}, nil
}

func (journal *Journal) AcknowledgeReleaseCompletion(
	ctx context.Context,
	input releaseexecution.Input,
	ack *agentpb.ReleaseWorkCompletionAck,
) (releaseexecution.CompletionReceipt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return releaseexecution.CompletionReceipt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, input.Attempt.ID)
	if err != nil {
		return releaseexecution.CompletionReceipt{}, err
	}
	if state == nil || state.Release == nil || !workexecution.SameAssignment(state.Attempt, input.Attempt) {
		return releaseexecution.CompletionReceipt{}, conflict()
	}
	input.Attempt = state.Attempt
	receipt, err := releaseexecution.DecodeCompletionAck(input, *state.Release, ack)
	if err != nil {
		return releaseexecution.CompletionReceipt{}, err
	}
	if state.Completion != nil {
		if state.Completion.Outcome != receipt.Outcome || !state.Completion.FinishedAt.Equal(receipt.FinishedAt) {
			return releaseexecution.CompletionReceipt{}, conflict()
		}
		return receipt, nil
	}
	state.Completion = &receipt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return releaseexecution.CompletionReceipt{}, err
	}
	return receipt, nil
}
