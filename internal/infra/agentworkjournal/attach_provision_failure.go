package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type AttachProvisionFailureDelivery struct {
	Reason   attachexecution.FailureReason `json:"reason"`
	Evidence []byte                        `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt        `json:"receipt,omitempty"`
}

func (record attemptRecord) validateAttachProvisionFailure() error {
	failure := record.AttachProvisionFailure
	if record.Attempt.Validate() != nil || record.Attempt.Executor.Executor != workexecution.Agent ||
		record.Attempt.FinishedAt != nil ||
		record.UpdatedAt.IsZero() ||
		record.UpdatedAt.Before(record.Attempt.AssignedAt) ||
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
		failure == nil ||
		!failure.Reason.Valid() ||
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
	if terminal := failure.Receipt; terminal != nil {
		if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
			terminal.Outcome == workexecution.Succeeded ||
			!workexecution.SameAssignment(*terminal, record.Attempt) ||
			terminal.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
		captured, err := attachexecution.DecodeProvisionFailure(*terminal, failure.Evidence, *terminal.Result)
		if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) ||
			(record.Attempt.StartedAt == nil) != (terminal.StartedAt == nil) ||
			record.Attempt.StartedAt != nil && !record.Attempt.StartedAt.Equal(*terminal.StartedAt) {
			return invalid()
		}
		if record.AttachProvision != nil && captured.Authority != record.AttachProvision.Authority {
			return invalid()
		}
	} else if len(failure.Evidence) != 0 {
		return invalid()
	}
	if record.AttachProvision == nil {
		if record.Sequence != 0 || !record.UpdatedAt.Equal(record.Attempt.AssignedAt) {
			return invalid()
		}
		return nil
	}
	if record.AttachProvision.Completion != nil {
		return invalid()
	}
	record.AttachProvisionFailure = nil
	return record.validateAttachProvision()
}
