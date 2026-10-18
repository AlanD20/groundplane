package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type EnvironmentProvisionRecord struct {
	Result    environmentexecution.Result `json:"result"`
	Submitted bool                        `json:"submitted"`
	Receipt   *workexecution.Attempt      `json:"receipt,omitempty"`
}

func (record attemptRecord) validateEnvironmentProvision() error {
	provision := record.EnvironmentProvision
	if provision == nil || record.Sequence != 0 || provision.Result.Validate(record.Attempt) != nil ||
		record.Attempt.StartedAt == nil || !provision.Result.ObservedAt.Equal(record.UpdatedAt) ||
		record.Backup != nil || record.RemovalRecovery != nil || record.LifecycleRecovery != nil || record.Lifecycle != nil ||
		record.AttachProvisionRecovery != nil || record.AttachProvisionFailure != nil || record.AttachProvision != nil ||
		record.EntryRecovery != nil || record.EntryConfiguration != nil || record.ComponentRecovery != nil || record.Component != nil ||
		record.NetworkRecovery != nil || record.Network != nil || record.VolumeRecovery != nil || record.Volume != nil ||
		record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil ||
		record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil || record.ScriptFailure != nil || record.ScriptRecovery != nil ||
		len(record.Script) != 0 || record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil ||
		record.Recovery != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	if receipt := provision.Receipt; receipt != nil {
		expected, err := provision.Result.Reference(record.Attempt)
		if err != nil || !provision.Submitted || receipt.Validate() != nil || !workexecution.SameAssignment(*receipt, record.Attempt) ||
			receipt.FinishedAt == nil || receipt.Result == nil || *receipt.Result != expected || receipt.StartedAt == nil ||
			!receipt.StartedAt.Equal(*record.Attempt.StartedAt) || receipt.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
	}
	return nil
}
