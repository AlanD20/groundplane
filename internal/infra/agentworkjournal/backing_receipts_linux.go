package agentworkjournal

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/backingruntimefact"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// SaveBackingRuntime persists the exact final observation before its checkpoint
// is confirmed. A reconnect can resend it without inspecting a newer runtime.
func (journal *Journal) SaveBackingRuntime(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, observation backingruntimefact.Observation,
) error {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil {
		return err
	}
	workload, err := backingruntimefact.Workload(
		input.Runtime,
		input.ServiceId,
		attempt.Input.ID,
		input.RenderGeneration,
	)
	if err != nil || !backingruntimefact.MatchesObservation(observation, input.Runtime, workload) {
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
	if !matchesBacking(state, attempt, layout) || state.Backing.Failure != nil || state.Backing.Completion != nil ||
		state.Backing.Progress == nil || !state.Backing.Acknowledged || !state.Backing.Progress.InFlight ||
		state.Backing.Progress.Stage != backingprovision.VerifyRuntime || observation.ObservedAt.Before(*state.Backing.Progress.BeganAt) ||
		state.Backing.Progress.Last == nil || observation.ContainerID != state.Backing.Progress.Last.ContainerID {
		return conflict()
	}
	if previous := state.Backing.Runtime; previous != nil {
		left, err := backingprovision.EncodeRuntimeObservation(*previous)
		if err != nil {
			return err
		}
		right, err := backingprovision.EncodeRuntimeObservation(observation)
		if err != nil || string(left) != string(right) {
			return conflict()
		}
		return nil
	}
	state.Backing.Runtime = &observation
	return journal.writeRecord(ctx, *state)
}

func (journal *Journal) CompleteBacking(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, ack *agentpb.BackingProvisionCompletionAck,
) (workexecution.Attempt, error) {
	layout, err := backingprovision.CaptureLayout(attempt, input)
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
	if !matchesBacking(state, attempt, layout) || state.Backing.Failure != nil || state.Backing.Runtime == nil ||
		state.Backing.Progress == nil || !state.Backing.Acknowledged {
		return workexecution.Attempt{}, conflict()
	}
	terminal, err := backingprovision.DecodeCompletionAck(
		attempt,
		layout,
		*state.Backing.Progress,
		*state.Backing.Runtime,
		ack,
	)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if previous := state.Backing.Completion; previous != nil {
		if !sameBackingReceipt(previous, terminal) {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	state.Backing.Completion = &terminal
	return terminal, journal.writeRecord(ctx, *state)
}

func (journal *Journal) PrepareBackingFailure(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, reason backingprovision.FailureReason, at time.Time,
) (*BackingFailureDelivery, error) {
	layout, err := backingprovision.CaptureLayout(attempt, input)
	if err != nil || attempt.Direction != workexecution.Forward || attempt.FinishedAt != nil ||
		reason != backingprovision.ExecutionFailed && reason != backingprovision.DeadlineExpired &&
			reason != backingprovision.Interrupted ||
		at.Before(attempt.AssignedAt) || attempt.StartedAt != nil && at.Before(*attempt.StartedAt) ||
		reason == backingprovision.DeadlineExpired && at.Before(attempt.Deadline) {
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
		state = &attemptRecord{Attempt: attempt, UpdatedAt: at.UTC(), Backing: &BackingRecord{Layout: layout}}
	}
	if !matchesBacking(state, attempt, layout) || state.Backing.Completion != nil ||
		state.Backing.Progress != nil &&
			state.Backing.Progress.Stage == backingprovision.Complete || at.Before(state.UpdatedAt) {
		return nil, conflict()
	}
	if saved := state.Backing.Failure; saved != nil {
		return saved, nil
	}
	state.Backing.Failure = &BackingFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Backing.Failure, nil
}

func (journal *Journal) AcknowledgeBackingFailure(ctx context.Context, attempt workexecution.Attempt,
	input *agentpb.BackingProvisionInput, ack *agentpb.BackingProvisionFailureAck,
) (workexecution.Attempt, error) {
	layout, err := backingprovision.CaptureLayout(attempt, input)
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
	if !matchesBacking(state, attempt, layout) || state.Backing.Failure == nil {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.Backing.Failure
	terminal, captured, err := backingprovision.DecodeFailureAck(attempt, layout, failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
	}
	if terminal.FinishedAt.Before(state.UpdatedAt) {
		return workexecution.Attempt{}, conflict()
	}
	if cursor := captured.Checkpoint; cursor != nil {
		if state.Backing.Progress == nil || cursor.Sequence > state.Backing.Progress.Sequence {
			return workexecution.Attempt{}, conflict()
		}
		if cursor.Sequence == state.Backing.Progress.Sequence {
			local, err := state.Backing.Progress.Checkpoint(attempt, layout)
			if err != nil || local != *cursor {
				return workexecution.Attempt{}, conflict()
			}
		} else if state.Backing.Acknowledged || cursor.Sequence+1 != state.Backing.Progress.Sequence {
			return workexecution.Attempt{}, conflict()
		}
	} else if state.Backing.Progress != nil && (state.Backing.Acknowledged || state.Backing.Progress.Sequence > 1) {
		return workexecution.Attempt{}, conflict()
	}
	if previous := failure.Receipt; previous != nil {
		if !sameBackingReceipt(previous, terminal) {
			return workexecution.Attempt{}, conflict()
		}
		return *previous, nil
	}
	failure.Receipt, failure.Evidence = &terminal, append([]byte(nil), ack.Failure...)
	return terminal, journal.writeRecord(ctx, *state)
}

func sameBackingReceipt(saved *workexecution.Attempt, terminal workexecution.Attempt) bool {
	return saved != nil && sameBackingTerminal(*saved, terminal) && saved.FinishedAt != nil && saved.Result != nil &&
		saved.Outcome == terminal.Outcome && saved.FinishedAt.Equal(*terminal.FinishedAt) && *saved.Result == *terminal.Result
}
