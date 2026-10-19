package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type NetworkRecoveryRecord struct {
	Action    agentpb.NetworkWorkAction        `json:"action"`
	NetworkID string                           `json:"network_id"`
	Source    networkexecution.Recovery        `json:"source"`
	Boundary  networkexecution.Progress        `json:"boundary"`
	Report    *networkexecution.RecoveryReport `json:"report,omitempty"`
	Receipt   *workexecution.Attempt           `json:"receipt,omitempty"`
	Failure   *NetworkFailureDelivery          `json:"failure,omitempty"`
}

func (record attemptRecord) validateNetworkRecovery() error {
	recovery := record.NetworkRecovery
	if recovery == nil || record.Network != nil || record.Volume != nil || record.VolumeRecovery != nil ||
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
		len(record.Script) != 0 ||
		record.Sequence != 0 ||
		record.Attempt.StartedAt == nil ||
		recovery.Source.ValidateFor(record.Attempt) != nil ||
		recovery.Source.Failure.Checkpoint == nil {
		return invalid()
	}
	checkpoint, err := recovery.Boundary.Checkpoint(recovery.Source.Forward, recovery.Action, recovery.NetworkID)
	if err != nil || !recovery.Boundary.InFlight || checkpoint != *recovery.Source.Failure.Checkpoint {
		return invalid()
	}
	if recovery.Report == nil {
		if recovery.Receipt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) {
			return invalid()
		}
		if recovery.Failure != nil {
			return validateNetworkRecoveryFailure(record.Attempt, recovery.Failure)
		}
		return nil
	}
	if recovery.Failure != nil {
		return invalid()
	}
	_, evidence, err := networkexecution.EncodeRecoveryReport(
		record.Attempt,
		recovery.Source,
		recovery.Action,
		recovery.NetworkID,
		recovery.Boundary,
		*recovery.Report,
	)
	if err != nil || !record.UpdatedAt.Equal(recovery.Report.Observation.At) {
		return invalid()
	}
	if terminal := recovery.Receipt; terminal != nil {
		if terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) ||
			terminal.FinishedAt == nil ||
			terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt.Before(record.UpdatedAt) ||
			terminal.Result == nil ||
			*terminal.Result != evidence ||
			terminal.Outcome != workexecution.Succeeded {
			return invalid()
		}
	}
	return nil
}

func validateNetworkRecoveryFailure(attempt workexecution.Attempt, failure *NetworkFailureDelivery) error {
	if !failure.Reason.Valid() {
		return invalid()
	}
	if terminal := failure.Receipt; terminal != nil {
		if terminal.Validate() != nil || !workexecution.SameAssignment(attempt, *terminal) ||
			terminal.FinishedAt == nil ||
			terminal.Result == nil ||
			terminal.Outcome == workexecution.Succeeded ||
			terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*attempt.StartedAt) {
			return invalid()
		}
		saved, err := networkexecution.DecodeFailure(*terminal, failure.Evidence, *terminal.Result)
		if err != nil || saved.Reason != failure.Reason || saved.Checkpoint != nil ||
			!saved.RecordedAt.Equal(*terminal.FinishedAt) {
			return invalid()
		}
	} else if len(failure.Evidence) != 0 {
		return invalid()
	}
	return nil
}
