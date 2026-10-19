package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareComponentRecoveryFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	reason componentexecution.FailureReason,
) (*ComponentFailureDelivery, error) {
	if !reason.Valid() {
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
	if state == nil || state.ComponentRecovery == nil || state.ComponentRecovery.Report != nil ||
		!matchesComponentRecovery(state, attempt, input) {
		return nil, conflict()
	}
	if state.ComponentRecovery.Failure != nil {
		return state.ComponentRecovery.Failure, nil
	}
	state.ComponentRecovery.Failure = &ComponentFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.ComponentRecovery.Failure, nil
}

func (journal *Journal) AcknowledgeComponentRecoveryFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ComponentWorkInput,
	ack *agentpb.ComponentWorkFailureAck,
) (workexecution.Attempt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if state == nil || state.ComponentRecovery == nil || state.ComponentRecovery.Failure == nil ||
		!matchesComponentRecovery(state, attempt, input) {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.ComponentRecovery.Failure
	terminal, _, err := componentexecution.DecodeFailureAck(state.Attempt, input, failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := failure.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || *previous.Result != *terminal.Result ||
			!previous.FinishedAt.Equal(*terminal.FinishedAt) {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	failure.Receipt, failure.Evidence = &terminal, append([]byte(nil), ack.Failure...)
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
