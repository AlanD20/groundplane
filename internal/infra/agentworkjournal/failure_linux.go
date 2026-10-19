package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/releaseexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type ReleaseFailureDelivery struct {
	Reason  releaseexecution.FailureReason   `json:"reason"`
	Receipt *releaseexecution.FailureReceipt `json:"receipt,omitempty"`
}

func (journal *Journal) AcknowledgeReleaseFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	ack *agentpb.ReleaseWorkFailureAck,
) (releaseexecution.FailureReceipt, error) {
	unlock, err := journal.lock(ctx)
	if err != nil {
		return releaseexecution.FailureReceipt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return releaseexecution.FailureReceipt{}, err
	}
	if state == nil || state.Failure == nil || !workexecution.SameAssignment(state.Attempt, attempt) {
		return releaseexecution.FailureReceipt{}, conflict()
	}
	receipt, err := releaseexecution.DecodeFailureAck(state.Attempt, state.Failure.Reason, ack)
	if err != nil {
		return releaseexecution.FailureReceipt{}, err
	}
	if saved := state.Failure.Receipt; saved != nil {
		if saved.Evidence != receipt.Evidence || saved.Outcome != receipt.Outcome {
			return releaseexecution.FailureReceipt{}, conflict()
		}
		return *saved, nil
	}
	state.Failure.Receipt = &receipt
	if err := journal.writeRecord(ctx, *state); err != nil {
		return releaseexecution.FailureReceipt{}, err
	}
	return receipt, nil
}

// PrepareReleaseFailure freezes the first failure reason before transport.
// A later reconnect error must not replace the original execution failure.
// Existing operation/hook evidence remains intact for Controller recovery.
func (journal *Journal) PrepareReleaseFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	reason releaseexecution.FailureReason,
) (ReleaseFailureDelivery, error) {
	if attempt.Validate() != nil || attempt.FinishedAt != nil {
		return ReleaseFailureDelivery{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return ReleaseFailureDelivery{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return ReleaseFailureDelivery{}, err
	}
	if state == nil {
		attempts, err := journal.attempts(ctx)
		if err != nil {
			return ReleaseFailureDelivery{}, err
		}
		if _, reserved := attempts[attempt.ID]; !reserved && len(attempts) >= MaximumAttempts {
			return ReleaseFailureDelivery{}, conflict()
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: attempt.AssignedAt}
	}
	if !workexecution.SameAssignment(state.Attempt, attempt) {
		return ReleaseFailureDelivery{}, conflict()
	}
	if state.Failure != nil {
		return *state.Failure, nil
	}
	state.Failure = &ReleaseFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return ReleaseFailureDelivery{}, err
	}
	return *state.Failure, nil
}

func (journal *Journal) LoadReleaseFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
) (*ReleaseFailureDelivery, error) {
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
	return state.Failure, nil
}
