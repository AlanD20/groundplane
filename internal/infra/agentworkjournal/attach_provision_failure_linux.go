package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareAttachProvisionFailure(ctx context.Context, attempt workexecution.Attempt,
	reason attachexecution.FailureReason,
) (AttachProvisionFailureDelivery, error) {
	if attempt.Validate() != nil || attempt.FinishedAt != nil || !reason.Valid() {
		return AttachProvisionFailureDelivery{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return AttachProvisionFailureDelivery{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return AttachProvisionFailureDelivery{}, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return AttachProvisionFailureDelivery{}, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return AttachProvisionFailureDelivery{}, conflict()
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: attempt.AssignedAt}
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return AttachProvisionFailureDelivery{}, conflict()
	}
	if state.AttachProvisionFailure != nil {
		return *state.AttachProvisionFailure, nil
	}
	state.AttachProvisionFailure = &AttachProvisionFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return AttachProvisionFailureDelivery{}, err
	}
	return *state.AttachProvisionFailure, nil
}

func (journal *Journal) LoadAttachProvisionFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
) (*AttachProvisionFailureDelivery, error) {
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
	return state.AttachProvisionFailure, nil
}

func (journal *Journal) AcknowledgeAttachProvisionFailure(ctx context.Context, attempt workexecution.Attempt,
	ack *agentpb.AttachProvisionFailureAck,
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
	if state == nil || state.AttachProvisionFailure == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	terminal, _, err := attachexecution.DecodeProvisionFailureAck(
		state.Attempt,
		state.AttachProvisionFailure.Reason,
		ack,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
	}
	if prior := state.AttachProvisionFailure.Receipt; prior != nil {
		if prior.Outcome != terminal.Outcome || !prior.FinishedAt.Equal(*terminal.FinishedAt) ||
			*prior.Result != *terminal.Result {
			return workexecution.Attempt{}, conflict()
		}
		return *prior, nil
	}
	state.Attempt.StartedAt = terminal.StartedAt
	state.AttachProvisionFailure.Receipt, state.AttachProvisionFailure.Evidence = &terminal, append(
		[]byte(nil),
		ack.Failure...)
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
