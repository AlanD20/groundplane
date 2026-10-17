package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareNetworkFailure(ctx context.Context, attempt workexecution.Attempt, action agentpb.NetworkWorkAction,
	networkID string, reason networkexecution.FailureReason,
) (*NetworkFailureDelivery, error) {
	if attempt.Validate() != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil || !reason.Valid() {
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
		progress, err := networkexecution.NewProgress(attempt, action, networkID, *attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: progress.UpdatedAt,
			Network: &NetworkRecord{Action: action, NetworkID: networkID, Progress: progress}}
	}
	if !matchesNetwork(state, attempt, action, networkID) || state.Network.Completion != nil || state.Network.Progress.Stage == networkexecution.Complete {
		return nil, conflict()
	}
	if state.Network.Failure != nil {
		return state.Network.Failure, nil
	}
	state.Network.Failure = &NetworkFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Network.Failure, nil
}

func (journal *Journal) AcknowledgeNetworkFailure(ctx context.Context, attempt workexecution.Attempt, action agentpb.NetworkWorkAction,
	networkID string, ack *agentpb.NetworkWorkFailureAck,
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
	if !matchesNetwork(state, attempt, action, networkID) || state.Network.Failure == nil {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.Network.Failure
	terminal, _, err := networkexecution.DecodeFailureAck(attempt, failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
	}
	if previous := failure.Receipt; previous != nil {
		if previous.Outcome != terminal.Outcome || *previous.Result != *terminal.Result || !previous.FinishedAt.Equal(*terminal.FinishedAt) {
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
