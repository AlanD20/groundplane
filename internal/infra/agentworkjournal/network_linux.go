package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareNetwork(ctx context.Context, attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string, next networkexecution.Progress) (*NetworkRecord, error) {
	if next.Validate(attempt, action, networkID) != nil || next.Sequence == 0 {
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
	var previous *networkexecution.Progress
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return nil, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return nil, conflict()
		}
		state = &attemptRecord{Attempt: attempt}
	} else {
		if !matchesNetwork(state, attempt, action, networkID) {
			return nil, conflict()
		}
		if state.Network.Failure != nil || state.Network.Completion != nil {
			return nil, conflict()
		}
		previous = &state.Network.Progress
		if networkexecution.EqualProgress(*previous, next) {
			return state.Network, nil
		}
		if !state.Network.Acknowledged {
			return nil, conflict()
		}
	}
	if err := networkexecution.ValidateTransition(attempt, action, networkID, previous, next); err != nil {
		return nil, err
	}
	state.Network = &NetworkRecord{Action: action, NetworkID: networkID, Progress: next}
	state.Sequence, state.UpdatedAt = next.Sequence, next.UpdatedAt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Network, nil
}

func (journal *Journal) Network(ctx context.Context, attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string) (*NetworkRecord, error) {
	if attempt.Validate() != nil {
		return nil, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !matchesNetwork(state, attempt, action, networkID) {
		return nil, conflict()
	}
	return state.Network, nil
}

func (journal *Journal) AcknowledgeNetwork(ctx context.Context, attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string, checkpoint workexecution.Checkpoint) error {
	if attempt.Validate() != nil {
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
	if !matchesNetwork(state, attempt, action, networkID) {
		return conflict()
	}
	expected, err := state.Network.Progress.Checkpoint(attempt, action, networkID)
	if err != nil || checkpoint.AttemptID != expected.AttemptID || checkpoint.Sequence != expected.Sequence ||
		checkpoint.StageID != expected.StageID || checkpoint.Evidence != expected.Evidence || !checkpoint.RecordedAt.Equal(expected.RecordedAt) {
		return conflict()
	}
	if state.Network.Acknowledged {
		return nil
	}
	state.Network.Acknowledged = true
	return journal.writeRecord(ctx, *state)
}

func matchesNetwork(state *attemptRecord, attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string) bool {
	return state != nil && state.Network != nil && workexecution.SameAssignment(state.Attempt, attempt) &&
		state.Attempt.StartedAt != nil && attempt.StartedAt != nil && state.Attempt.StartedAt.Equal(*attempt.StartedAt) &&
		state.Network.Action == action && state.Network.NetworkID == networkID
}

func (journal *Journal) AcknowledgeNetworkCompletion(ctx context.Context, attempt workexecution.Attempt, action agentpb.NetworkWorkAction, networkID string, terminal workexecution.Attempt) error {
	if attempt.Validate() != nil || terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil {
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
	if !matchesNetwork(state, attempt, action, networkID) {
		return conflict()
	}
	if previous := state.Network.Completion; previous != nil {
		if !workexecution.SameAssignment(*previous, terminal) || previous.Outcome != terminal.Outcome ||
			!previous.FinishedAt.Equal(*terminal.FinishedAt) || *previous.Result != *terminal.Result ||
			terminal.StartedAt == nil || !previous.StartedAt.Equal(*terminal.StartedAt) {
			return conflict()
		}
		return nil
	}
	state.Network.Completion = &terminal
	return journal.writeRecord(ctx, *state)
}
