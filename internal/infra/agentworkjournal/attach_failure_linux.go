package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareAttachFailure(ctx context.Context, attempt workexecution.Attempt,
	reason attachexecution.FailureReason,
) (AttachFailureDelivery, error) {
	if attempt.Validate() != nil || attempt.FinishedAt != nil || !reason.Valid() {
		return AttachFailureDelivery{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return AttachFailureDelivery{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return AttachFailureDelivery{}, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return AttachFailureDelivery{}, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return AttachFailureDelivery{}, conflict()
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: attempt.AssignedAt}
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return AttachFailureDelivery{}, conflict()
	}
	if state.AttachFailure != nil {
		return *state.AttachFailure, nil
	}
	state.AttachFailure = &AttachFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return AttachFailureDelivery{}, err
	}
	return *state.AttachFailure, nil
}

func (journal *Journal) LoadAttachFailure(ctx context.Context, attempt workexecution.Attempt) (*AttachFailureDelivery, error) {
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
	return state.AttachFailure, nil
}

func (journal *Journal) AcknowledgeAttachFailure(ctx context.Context, attempt workexecution.Attempt,
	ack *agentpb.AttachRemovalFailureAck,
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
	if state == nil || state.AttachFailure == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal, _, err := attachexecution.DecodeRemovalFailureAck(state.Attempt, state.AttachFailure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
	}
	if prior := state.AttachFailure.Receipt; prior != nil {
		if prior.Outcome != terminal.Outcome || !prior.FinishedAt.Equal(*terminal.FinishedAt) || *prior.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *prior, nil
	}
	state.Attempt.StartedAt = terminal.StartedAt
	state.AttachFailure.Receipt, state.AttachFailure.Evidence = &terminal, append([]byte(nil), ack.Failure...)
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
