package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareNetworkRecoveryFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	reason networkexecution.FailureReason,
) (*NetworkFailureDelivery, error) {
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
	if state == nil || state.NetworkRecovery == nil || state.NetworkRecovery.Report != nil ||
		!workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, conflict()
	}
	if state.NetworkRecovery.Failure != nil {
		return state.NetworkRecovery.Failure, nil
	}
	state.NetworkRecovery.Failure = &NetworkFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.NetworkRecovery.Failure, nil
}

func (journal *Journal) AcknowledgeNetworkRecoveryFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	ack *agentpb.NetworkWorkFailureAck,
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
	if state == nil || state.NetworkRecovery == nil || state.NetworkRecovery.Failure == nil ||
		!workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.NetworkRecovery.Failure
	terminal, _, err := networkexecution.DecodeFailureAck(state.Attempt, failure.Reason, ack)
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
