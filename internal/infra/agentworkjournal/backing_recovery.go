package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type BackingRecoveryRecord struct {
	Layout  backingprovision.Layout          `json:"layout"`
	Source  backingprovision.Recovery        `json:"source"`
	Report  *backingprovision.RecoveryReport `json:"report,omitempty"`
	Receipt *workexecution.Attempt           `json:"receipt,omitempty"`
}

func (record attemptRecord) validateBackingRecovery() error {
	recovery := record.BackingRecovery
	if recovery == nil || record.Backing != nil || record.Attempt.Validate() != nil || record.Attempt.FinishedAt != nil ||
		record.Attempt.StartedAt == nil || recovery.Source.ValidateFor(record.Attempt, recovery.Layout) != nil ||
		record.Sequence != 0 || record.UpdatedAt.Before(*record.Attempt.StartedAt) {
		return invalid()
	}
	for _, present := range []bool{record.EnvironmentProvision != nil, record.Backup != nil, record.RemovalRecovery != nil,
		record.LifecycleRecovery != nil, record.Lifecycle != nil, record.AttachProvisionRecovery != nil,
		record.AttachProvisionFailure != nil, record.AttachProvision != nil, record.EntryRecovery != nil,
		record.EntryConfiguration != nil, record.ComponentRecovery != nil, record.Component != nil,
		record.NetworkRecovery != nil, record.Network != nil, record.VolumeRecovery != nil, record.Volume != nil,
		record.AttachRecovery != nil, record.AttachFailure != nil, record.AttachRemoval != nil,
		record.EnvironmentFailure != nil, record.EnvironmentRemoval != nil, record.ScriptFailure != nil,
		record.ScriptRecovery != nil, len(record.Script) != 0, record.Release != nil, record.Effect != nil,
		record.Completion != nil, record.Failure != nil, record.Recovery != nil, record.ScriptCompletion != nil} {
		if present {
			return invalid()
		}
	}
	if recovery.Report == nil {
		if recovery.Receipt != nil || !record.UpdatedAt.Equal(*record.Attempt.StartedAt) {
			return invalid()
		}
		return nil
	}
	_, evidence, err := backingprovision.EncodeRecoveryReport(record.Attempt, recovery.Source, recovery.Layout, *recovery.Report)
	if err != nil || !record.UpdatedAt.Equal(recovery.Report.ObservedAt) {
		return invalid()
	}
	if terminal := recovery.Receipt; terminal != nil {
		outcome := workexecution.Failed
		if recovery.Report.Status == backingprovision.RecoveryComplete {
			outcome = workexecution.Succeeded
		}
		if !sameBackingTerminal(record.Attempt, *terminal) || *terminal.Result != evidence || terminal.Outcome != outcome ||
			terminal.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
	}
	return nil
}
