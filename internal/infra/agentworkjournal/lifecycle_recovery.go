package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type LifecycleRecoveryRecord struct {
	Stages  []lifecycleexecution.Stage         `json:"stages"`
	Source  lifecycleexecution.Recovery        `json:"source"`
	Report  *lifecycleexecution.RecoveryReport `json:"report,omitempty"`
	Receipt *workexecution.Attempt             `json:"receipt,omitempty"`
}

func (record attemptRecord) validateLifecycleRecovery() error {
	recovery := record.LifecycleRecovery
	if recovery == nil || record.Lifecycle != nil || record.AttachProvisionRecovery != nil ||
		record.AttachProvisionFailure != nil ||
		record.AttachProvision != nil ||
		record.EntryRecovery != nil ||
		record.EntryConfiguration != nil ||
		record.ComponentRecovery != nil ||
		record.Component != nil ||
		record.NetworkRecovery != nil ||
		record.Network != nil ||
		record.VolumeRecovery != nil ||
		record.Volume != nil ||
		record.AttachRecovery != nil ||
		record.AttachFailure != nil ||
		record.AttachRemoval != nil ||
		record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil ||
		record.ScriptFailure != nil ||
		record.ScriptRecovery != nil ||
		len(record.Script) != 0 ||
		record.Release != nil ||
		record.Effect != nil ||
		record.Completion != nil ||
		record.Failure != nil ||
		record.Recovery != nil ||
		record.ScriptCompletion != nil ||
		record.Sequence != 0 ||
		record.Attempt.StartedAt == nil ||
		record.Attempt.FinishedAt != nil ||
		recovery.Source.ValidateForStages(record.Attempt, recovery.Stages) != nil ||
		recovery.Source.Progress == nil ||
		recovery.Source.Progress.BeganAt == nil {
		return invalid()
	}
	if recovery.Report == nil {
		if recovery.Receipt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) {
			return invalid()
		}
		return nil
	}
	_, evidence, err := lifecycleexecution.EncodeRecoveryReportStages(
		record.Attempt,
		recovery.Stages,
		recovery.Source,
		*recovery.Report,
	)
	if err != nil || !record.UpdatedAt.Equal(recovery.Report.ObservedAt) {
		return invalid()
	}
	if terminal := recovery.Receipt; terminal != nil {
		if terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) ||
			terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt == nil ||
			terminal.FinishedAt.Before(record.UpdatedAt) ||
			terminal.Result == nil ||
			*terminal.Result != evidence ||
			(terminal.Outcome != workexecution.Succeeded && terminal.Outcome != workexecution.Failed) ||
			(terminal.Outcome == workexecution.Succeeded) != recovery.Report.BoundaryCompleted() {
			return invalid()
		}
	}
	return nil
}
