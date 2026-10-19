package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// AcknowledgeStoppedLifecycle records a Controller rejection of obsolete
// completion, not an Agent-invented failure. Completed host effects remain in
// progress; their restoration is a separately assigned recovery operation.
func (journal *Journal) AcknowledgeStoppedLifecycle(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.ServiceLifecycleWorkInput, ack *agentpb.ServiceLifecycleFailureAck,
) (workexecution.Attempt, error) {
	if input == nil || input.Action != agentpb.ServiceLifecycleAction_SERVICE_LIFECYCLE_ACTION_DESTROY ||
		input.Hook != nil ||
		ack == nil ||
		executionplan.RejectUnknown(ack) != nil {
		return workexecution.Attempt{}, invalid()
	}
	terminal, failure, err := lifecycleexecution.DecodeFailureAck(
		attempt,
		input,
		lifecycleexecution.ExecutionInterrupted,
		ack,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded {
		return workexecution.Attempt{}, invalid()
	}
	unlock, err := journal.lock(ctx)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	defer unlock()
	state, err := journal.readRecord(ctx, attempt.ID)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if !matchesLifecycle(state, attempt, input) || state.Lifecycle.Completion != nil || !state.Lifecycle.Acknowledged ||
		state.Lifecycle.Progress.BeganAt != nil || len(state.Lifecycle.Progress.Completed) != len(state.Lifecycle.Stages) {
		return workexecution.Attempt{}, conflict()
	}
	checkpoint, err := state.Lifecycle.Progress.Checkpoint(attempt, input)
	if err != nil || failure.Checkpoint == nil || *failure.Checkpoint != checkpoint ||
		terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
	}
	if previous := state.Lifecycle.Failure; previous != nil {
		if previous.Receipt == nil || previous.Receipt.Result == nil || previous.Receipt.FinishedAt == nil ||
			previous.Receipt.Outcome != terminal.Outcome || *previous.Receipt.Result != *terminal.Result ||
			!previous.Receipt.FinishedAt.Equal(*terminal.FinishedAt) {
			return workexecution.Attempt{}, conflict()
		}
		return *previous.Receipt, nil
	}
	state.Lifecycle.Failure = &LifecycleFailureDelivery{
		Reason:   failure.Reason,
		Receipt:  &terminal,
		Evidence: append([]byte(nil), ack.Failure...),
	}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return workexecution.Attempt{}, err
	}
	return terminal, nil
}
