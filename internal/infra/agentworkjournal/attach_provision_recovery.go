package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type AttachProvisionRecoveryRecord struct {
	Source  attachexecution.ProvisionRecovery   `json:"source"`
	Report  attachexecution.ProvisionInspection `json:"report"`
	Receipt *workexecution.Attempt              `json:"receipt,omitempty"`
}

func (record attemptRecord) validateAttachProvisionRecovery() error {
	recovery := record.AttachProvisionRecovery
	if recovery == nil || record.AttachProvisionFailure != nil || record.AttachProvision != nil ||
		record.EntryRecovery != nil || record.EntryConfiguration != nil || record.ComponentRecovery != nil || record.Component != nil ||
		record.NetworkRecovery != nil || record.Network != nil || record.VolumeRecovery != nil || record.Volume != nil ||
		record.AttachRecovery != nil || record.AttachFailure != nil || record.AttachRemoval != nil || record.EnvironmentFailure != nil ||
		record.EnvironmentRemoval != nil || record.ScriptFailure != nil || record.ScriptRecovery != nil || record.ScriptCompletion != nil ||
		record.Release != nil || record.Effect != nil || record.Completion != nil || record.Failure != nil || record.Recovery != nil || len(record.Script) != 0 ||
		record.Sequence != 1 || !record.UpdatedAt.Equal(recovery.Report.ObservedAt) {
		return invalid()
	}
	input := attachexecution.ProvisionRecoveryInput{Attempt: record.Attempt, Source: recovery.Source}
	_, reference, err := attachexecution.EncodeProvisionInspection(input, recovery.Report)
	if err != nil {
		return err
	}
	if receipt := recovery.Receipt; receipt != nil {
		outcome := workexecution.Failed
		if recovery.Report.ExecutionStopped() {
			outcome = workexecution.Succeeded
		}
		if receipt.Validate() != nil || !workexecution.SameAssignment(record.Attempt, *receipt) ||
			(receipt.StartedAt == nil) != (record.Attempt.StartedAt == nil) ||
			receipt.StartedAt != nil &&
				!receipt.StartedAt.Equal(*record.Attempt.StartedAt) || receipt.FinishedAt == nil ||
			receipt.FinishedAt.Before(
				record.UpdatedAt,
			) || receipt.Result == nil || *receipt.Result != reference || receipt.Outcome != outcome {
			return invalid()
		}
	}
	return nil
}
