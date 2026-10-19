package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// NetworkRecord retains a bounded checkpoint, never the captured Compose files.
// The attempt's input digest binds those files to this operation and Network.
type NetworkRecord struct {
	Action       agentpb.NetworkWorkAction `json:"action"`
	NetworkID    string                    `json:"network_id"`
	Progress     networkexecution.Progress `json:"progress"`
	Acknowledged bool                      `json:"acknowledged"`
	Completion   *workexecution.Attempt    `json:"completion,omitempty"`
	Failure      *NetworkFailureDelivery   `json:"failure,omitempty"`
}

type NetworkFailureDelivery struct {
	Reason   networkexecution.FailureReason `json:"reason"`
	Evidence []byte                         `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt         `json:"receipt,omitempty"`
}

func (record attemptRecord) validateNetwork() error {
	if record.Network == nil || record.Volume != nil || record.VolumeRecovery != nil || record.NetworkRecovery != nil ||
		record.AttachRecovery != nil ||
		record.AttachFailure != nil ||
		record.AttachRemoval != nil ||
		record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil ||
		record.ScriptFailure != nil ||
		record.ScriptRecovery != nil ||
		record.ScriptCompletion != nil ||
		record.Release != nil ||
		record.Effect != nil ||
		record.Completion != nil ||
		record.Failure != nil ||
		record.Recovery != nil ||
		len(record.Script) != 0 {
		return invalid()
	}
	network := record.Network
	if network.Progress.Sequence != record.Sequence || !network.Progress.UpdatedAt.Equal(record.UpdatedAt) {
		return invalid()
	}
	if err := network.Progress.Validate(record.Attempt, network.Action, network.NetworkID); err != nil {
		return err
	}
	if network.Progress.Sequence == 0 && (network.Failure == nil || network.Acknowledged || network.Completion != nil) {
		return invalid()
	}
	if failure := network.Failure; failure != nil {
		if !failure.Reason.Valid() || network.Completion != nil || network.Progress.Stage == networkexecution.Complete {
			return invalid()
		}
		if terminal := failure.Receipt; terminal != nil {
			if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
				terminal.Outcome == workexecution.Succeeded ||
				!workexecution.SameAssignment(record.Attempt, *terminal) ||
				terminal.StartedAt == nil ||
				!terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
				terminal.FinishedAt.Before(record.UpdatedAt) {
				return invalid()
			}
			captured, err := networkexecution.DecodeFailure(*terminal, failure.Evidence, *terminal.Result)
			if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) {
				return invalid()
			}
		} else if len(failure.Evidence) != 0 {
			return invalid()
		}
	}
	if terminal := network.Completion; terminal != nil {
		checkpoint, err := network.Progress.Checkpoint(record.Attempt, network.Action, network.NetworkID)
		if err != nil {
			return err
		}
		if !network.Acknowledged || network.Progress.Stage != networkexecution.Complete || terminal.Validate() != nil ||
			!workexecution.SameAssignment(record.Attempt, *terminal) || terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) || terminal.FinishedAt == nil ||
			terminal.FinishedAt.Before(
				record.UpdatedAt,
			) || (terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Cancelled && terminal.Outcome != workexecution.Superseded) ||
			terminal.Result == nil || *terminal.Result != checkpoint.Evidence {
			return invalid()
		}
	}
	return nil
}
