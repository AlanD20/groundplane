package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareComponentFailure(ctx context.Context, attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, reason componentexecution.FailureReason) (*ComponentFailureDelivery, error) {
	layout, err := componentexecution.CaptureLayout(attempt, input)
	if err != nil || attempt.StartedAt == nil || attempt.FinishedAt != nil || !reason.Valid() {
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
		progress, err := componentexecution.NewProgress(attempt, input, *attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: progress.UpdatedAt,
			Component: &ComponentRecord{Layout: layout, Progress: progress}}
	}
	if !matchesComponent(state, attempt, layout) || state.Component.Completion != nil || state.Component.Progress.Stage == componentexecution.Complete {
		return nil, conflict()
	}
	if state.Component.Failure != nil {
		return state.Component.Failure, nil
	}
	state.Component.Failure = &ComponentFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Component.Failure, nil
}

func (journal *Journal) AcknowledgeComponentFailure(ctx context.Context, attempt workexecution.Attempt, input *agentpb.ComponentWorkInput, ack *agentpb.ComponentWorkFailureAck) (workexecution.Attempt, error) {
	layout, err := componentexecution.CaptureLayout(attempt, input)
	if err != nil {
		return workexecution.Attempt{}, err
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
	if !matchesComponent(state, attempt, layout) || state.Component.Failure == nil {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.Component.Failure
	terminal, _, err := componentexecution.DecodeFailureAck(attempt, input, failure.Reason, ack)
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
