package executionplan

import (
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// OpenNetworkRecoveryInput binds inspection to the original failed execution.
// It does not relax the forward-only validation used by ordinary assignments.
func OpenNetworkRecoveryInput(
	attempt workexecution.Attempt,
	message *agentpb.NetworkWorkAssignment,
) (*agentpb.NetworkWorkInput, networkexecution.Recovery, networkexecution.Progress, error) {
	if message == nil || RejectUnknown(message) != nil || attempt.Direction != workexecution.Recovery {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, invalidNetworkWork()
	}
	source, err := networkexecution.DecodeRecovery(message.RecoverySource)
	if err != nil {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, err
	}
	if err := source.ValidateFor(attempt); err != nil {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, err
	}
	input, raw, reference, err := CaptureNetworkWorkInput(attempt.Input.ID, message.Input)
	clear(raw)
	if err != nil {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, err
	}
	if reference != attempt.Input || source.Failure.Checkpoint == nil {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, invalidNetworkWork()
	}
	boundary, err := networkexecution.DecodeProgress(
		source.Forward,
		input.Action,
		input.NetworkId,
		message.RecoveryBoundary,
	)
	if err != nil {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, err
	}
	checkpoint, err := boundary.Checkpoint(source.Forward, input.Action, input.NetworkId)
	if err != nil || !boundary.InFlight || checkpoint != *source.Failure.Checkpoint {
		return nil, networkexecution.Recovery{}, networkexecution.Progress{}, invalidNetworkWork()
	}
	return input, source, boundary, nil
}
