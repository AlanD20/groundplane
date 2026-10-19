package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareLifecycleFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	reason lifecycleexecution.FailureReason,
) (*LifecycleFailureDelivery, error) {
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
		progress, err := lifecycleexecution.NewProgress(attempt, input, *attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		stages, err := lifecycleexecution.Stages(input)
		if err != nil {
			return nil, err
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: progress.UpdatedAt,
			Lifecycle: &LifecycleRecord{Stages: stages, Progress: progress}}
	}
	if !matchesLifecycle(state, attempt, input) || state.Lifecycle.Completion != nil ||
		len(state.Lifecycle.Progress.Completed) == len(state.Lifecycle.Stages) {
		return nil, conflict()
	}
	if state.Lifecycle.Failure != nil {
		return state.Lifecycle.Failure, nil
	}
	state.Lifecycle.Failure = &LifecycleFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Lifecycle.Failure, nil
}

func (journal *Journal) AcknowledgeLifecycleFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput,
	ack *agentpb.ServiceLifecycleFailureAck,
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
	if !matchesLifecycle(state, attempt, input) || state.Lifecycle.Failure == nil {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.Lifecycle.Failure
	terminal, _, err := lifecycleexecution.DecodeFailureAck(attempt, input, failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
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
