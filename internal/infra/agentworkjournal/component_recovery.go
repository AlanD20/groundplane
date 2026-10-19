package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/componentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type ComponentRecoveryRecord struct {
	Restoration componentexecution.Restoration     `json:"restoration"`
	Source      componentexecution.Recovery        `json:"source"`
	Boundary    componentexecution.Progress        `json:"boundary"`
	Report      *componentexecution.RecoveryReport `json:"report,omitempty"`
	Receipt     *workexecution.Attempt             `json:"receipt,omitempty"`
	Failure     *ComponentFailureDelivery          `json:"failure,omitempty"`
}

func (record attemptRecord) validateComponentRecovery() error {
	recovery := record.ComponentRecovery
	if recovery == nil || record.Component != nil || record.Network != nil || record.NetworkRecovery != nil ||
		record.Volume != nil ||
		record.VolumeRecovery != nil ||
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
		record.Attempt.Validate() != nil ||
		record.Attempt.Executor.Executor != workexecution.Agent ||
		record.Attempt.FinishedAt != nil ||
		recovery.Restoration.Validate(record.Attempt) != nil ||
		record.Attempt.StartedAt == nil ||
		recovery.Source.ValidateLayoutFor(record.Attempt, recovery.Restoration.Layout) != nil ||
		recovery.Source.Failure.Checkpoint == nil {
		return invalid()
	}
	checkpoint, err := recovery.Boundary.LayoutCheckpoint(recovery.Source.Forward, recovery.Restoration.Layout)
	if err != nil || checkpoint != *recovery.Source.Failure.Checkpoint {
		return invalid()
	}
	if recovery.Report == nil {
		if recovery.Receipt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) {
			return invalid()
		}
		if recovery.Failure != nil {
			return validateComponentRecoveryFailure(record.Attempt, recovery.Restoration.Layout, recovery.Failure)
		}
		return nil
	}
	if recovery.Failure != nil {
		return invalid()
	}
	_, evidence, err := componentexecution.EncodeRestorationReport(
		record.Attempt,
		recovery.Source,
		recovery.Restoration,
		recovery.Boundary,
		*recovery.Report,
	)
	if err != nil || !record.UpdatedAt.Equal(recovery.Report.ObservedAt) {
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

func validateComponentRecoveryFailure(
	attempt workexecution.Attempt,
	layout componentexecution.Layout,
	failure *ComponentFailureDelivery,
) error {
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
		saved, err := componentexecution.DecodeLayoutFailure(*terminal, layout, failure.Evidence, *terminal.Result)
		if err != nil || saved.Reason != failure.Reason || saved.Checkpoint != nil ||
			!saved.RecordedAt.Equal(*terminal.FinishedAt) {
			return invalid()
		}
	} else if len(failure.Evidence) != 0 {
		return invalid()
	}
	return nil
}
