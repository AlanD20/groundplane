package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/entryexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type EntryRecoveryRecord struct {
	Restoration entryexecution.Restoration     `json:"restoration"`
	Source      entryexecution.Recovery        `json:"source"`
	Boundary    entryexecution.Progress        `json:"boundary"`
	Report      *entryexecution.RecoveryReport `json:"report,omitempty"`
	Receipt     *workexecution.Attempt         `json:"receipt,omitempty"`
}

func (record attemptRecord) validateEntryRecovery() error {
	recovery := record.EntryRecovery
	if recovery == nil || record.EntryConfiguration != nil || record.Component != nil ||
		record.ComponentRecovery != nil ||
		record.Network != nil ||
		record.NetworkRecovery != nil ||
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
		record.Attempt.FinishedAt != nil ||
		record.Attempt.StartedAt == nil ||
		recovery.Restoration.Validate(record.Attempt) != nil ||
		recovery.Source.ValidateLayoutFor(record.Attempt, recovery.Restoration.Layout) != nil {
		return invalid()
	}
	checkpoint, err := recovery.Boundary.LayoutCheckpoint(recovery.Source.Forward, recovery.Restoration.Layout)
	if err != nil || recovery.Source.Failure.Checkpoint == nil || checkpoint != *recovery.Source.Failure.Checkpoint {
		return invalid()
	}
	if recovery.Report == nil {
		if recovery.Receipt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) {
			return invalid()
		}
		return nil
	}
	_, evidence, err := entryexecution.EncodeRestorationReport(
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
		outcome := workexecution.Failed
		if recovery.Report.EffectsStopped && recovery.Report.FilesRestored &&
			(len(recovery.Restoration.Layout.Consumers) == 0 || recovery.Report.ConsumersRestored) {
			outcome = workexecution.Succeeded
		}
		if terminal.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *terminal) ||
			terminal.FinishedAt == nil ||
			terminal.StartedAt == nil ||
			!terminal.StartedAt.Equal(*record.Attempt.StartedAt) ||
			terminal.FinishedAt.Before(record.UpdatedAt) ||
			terminal.Result == nil ||
			*terminal.Result != evidence ||
			terminal.Outcome != outcome {
			return invalid()
		}
	}
	return nil
}

func sameEntryRestoration(left, right entryexecution.Restoration) bool {
	return sameEntryLayout(left.Layout, right.Layout) && left.BaselineSHA256 == right.BaselineSHA256 &&
		left.ConfigurationSHA256 == right.ConfigurationSHA256
}
