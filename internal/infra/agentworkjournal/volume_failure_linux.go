package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareVolumeFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	action agentpb.VolumeWorkAction,
	volumeID string,
	reason volumeexecution.FailureReason,
) (*VolumeFailureDelivery, error) {
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
		progress, err := volumeexecution.NewProgress(attempt, action, volumeID, *attempt.StartedAt)
		if err != nil {
			return nil, err
		}
		state = &attemptRecord{Attempt: attempt, UpdatedAt: progress.UpdatedAt,
			Volume: &VolumeRecord{Action: action, VolumeID: volumeID, Progress: progress}}
	}
	if !matchesVolume(state, attempt, action, volumeID) || state.Volume.Completion != nil ||
		state.Volume.Progress.Stage == volumeexecution.Complete {
		return nil, conflict()
	}
	if state.Volume.Failure != nil {
		return state.Volume.Failure, nil
	}
	state.Volume.Failure = &VolumeFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.Volume.Failure, nil
}

func (journal *Journal) AcknowledgeVolumeFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	action agentpb.VolumeWorkAction,
	volumeID string,
	ack *agentpb.VolumeWorkFailureAck,
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
	if !matchesVolume(state, attempt, action, volumeID) || state.Volume.Failure == nil {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.Volume.Failure
	terminal, _, err := volumeexecution.DecodeFailureAck(attempt, failure.Reason, ack)
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
