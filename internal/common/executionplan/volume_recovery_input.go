package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// OpenVolumeRecoveryInput binds inspection to the original failed execution.
// It does not relax the forward-only validation used by ordinary assignments.
func OpenVolumeRecoveryInput(attempt workexecution.Attempt, message *agentpb.VolumeWorkAssignment) (*agentpb.VolumeWorkInput, volumeexecution.Recovery, volumeexecution.Progress, error) {
	if message == nil || RejectUnknown(message) != nil || attempt.Direction != workexecution.Recovery {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, invalidVolumeWork()
	}
	source, err := volumeexecution.DecodeRecovery(message.RecoverySource)
	if err != nil {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, err
	}
	if err := source.ValidateFor(attempt); err != nil {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, err
	}
	input, raw, reference, err := CaptureVolumeWorkInput(attempt.Input.ID, message.Input)
	clear(raw)
	if err != nil {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, err
	}
	if reference != attempt.Input || source.Failure.Checkpoint == nil {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, invalidVolumeWork()
	}
	boundary, err := volumeexecution.DecodeProgress(source.Forward, input.Action, input.VolumeId, message.RecoveryBoundary)
	if err != nil {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, err
	}
	checkpoint, err := boundary.Checkpoint(source.Forward, input.Action, input.VolumeId)
	if err != nil || !boundary.InFlight || checkpoint != *source.Failure.Checkpoint {
		return nil, volumeexecution.Recovery{}, volumeexecution.Progress{}, invalidVolumeWork()
	}
	return input, source, boundary, nil
}
