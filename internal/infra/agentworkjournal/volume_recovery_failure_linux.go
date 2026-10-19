package agentworkjournal

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (journal *Journal) PrepareVolumeRecoveryFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
	reason volumeexecution.FailureReason,
) (*VolumeFailureDelivery, error) {
	if !reason.Valid() {
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
	if state == nil || state.VolumeRecovery == nil || state.VolumeRecovery.Report != nil ||
		!workexecution.SameAssignment(state.Attempt, attempt) {
		return nil, conflict()
	}
	if state.VolumeRecovery.Failure != nil {
		return state.VolumeRecovery.Failure, nil
	}
	state.VolumeRecovery.Failure = &VolumeFailureDelivery{Reason: reason}
	if err := journal.writeRecord(ctx, *state); err != nil {
		return nil, err
	}
	return state.VolumeRecovery.Failure, nil
}

func (journal *Journal) AcknowledgeVolumeRecoveryFailure(
	ctx context.Context,
	attempt workexecution.Attempt,
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
	if state == nil || state.VolumeRecovery == nil || state.VolumeRecovery.Failure == nil ||
		!workexecution.SameAssignment(state.Attempt, attempt) {
		return workexecution.Attempt{}, conflict()
	}
	failure := state.VolumeRecovery.Failure
	terminal, _, err := volumeexecution.DecodeFailureAck(state.Attempt, failure.Reason, ack)
	if err != nil {
		return workexecution.Attempt{}, err
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
