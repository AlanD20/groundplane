package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/hierarchyexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareEnvironmentFailure(ctx context.Context, attempt workexecution.Attempt,
	reason hierarchyexecution.FailureReason,
) (EnvironmentFailureDelivery, error) {
	if attempt.Validate() != nil || attempt.FinishedAt != nil || !reason.Valid() {
		return EnvironmentFailureDelivery{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return EnvironmentFailureDelivery{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return EnvironmentFailureDelivery{}, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return EnvironmentFailureDelivery{}, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return EnvironmentFailureDelivery{}, conflict()
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: attempt.AssignedAt}
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return EnvironmentFailureDelivery{}, conflict()
	}
	if state.EnvironmentFailure != nil {
		return *state.EnvironmentFailure, nil
	}
	state.EnvironmentFailure = &EnvironmentFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return EnvironmentFailureDelivery{}, err
	}
	return *state.EnvironmentFailure, nil
}

func (journal *Journal) LoadEnvironmentFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
) (*EnvironmentFailureDelivery, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return nil, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil || state == nil {
		return nil, err
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, conflict()
	}
	return state.EnvironmentFailure, nil
}

func (journal *Journal) AcknowledgeEnvironmentFailure(ctx context.Context, attempt workexecution.Attempt,
	ack *agentpb.EnvironmentRemovalFailureAck,
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
	if state == nil || state.EnvironmentFailure == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal, _, err := hierarchyexecution.DecodeEnvironmentFailureAck(
		state.Attempt,
		state.EnvironmentFailure.Reason,
		ack,
	)
	if err != nil || terminal.FinishedAt.Before(state.UpdatedAt) {
		if err != nil {
			return workexecution.Attempt{}, err
		}
		return workexecution.Attempt{}, conflict()
	}
	if prior := state.EnvironmentFailure.Receipt; prior != nil {
		if prior.Outcome != terminal.Outcome || !prior.FinishedAt.Equal(*terminal.FinishedAt) ||
			*prior.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *prior, nil
	}
	// The Controller may have accepted Start whose reply never reached us.
	state.Attempt.StartedAt = terminal.StartedAt
	state.EnvironmentFailure.Receipt = &terminal
	state.EnvironmentFailure.Evidence = append([]byte(nil), ack.Failure...)
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
