package agentworkjournal

import (
	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

type AttachFailureDelivery struct {
	Reason   attachexecution.FailureReason `json:"reason"`
	Evidence []byte                        `json:"evidence,omitempty"`
	Receipt  *workexecution.Attempt        `json:"receipt,omitempty"`
}

func (record attemptRecord) validateAttachFailure() error {
	failure := record.AttachFailure
	if failure == nil || !failure.Reason.Valid() || record.EnvironmentFailure != nil || record.EnvironmentRemoval != nil ||
		record.ScriptFailure != nil || record.ScriptRecovery != nil || record.ScriptCompletion != nil || record.Release != nil ||
		record.Effect != nil || record.Completion != nil || record.Failure != nil || record.Recovery != nil || len(record.Script) != 0 {
		return invalid()
	}
	if terminal := failure.Receipt; terminal != nil {
		if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil || terminal.Outcome == workexecution.Succeeded ||
			!workexecution.SameAssignment(*terminal, record.Attempt) || terminal.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
		captured, err := attachexecution.DecodeRemovalFailure(*terminal, failure.Evidence, *terminal.Result)
		if err != nil || captured.Reason != failure.Reason || !captured.RecordedAt.Equal(*terminal.FinishedAt) ||
			(record.Attempt.StartedAt == nil) != (terminal.StartedAt == nil) ||
			record.Attempt.StartedAt != nil && !record.Attempt.StartedAt.Equal(*terminal.StartedAt) {
			return invalid()
		}
		if record.AttachRemoval != nil && captured.Authority != record.AttachRemoval.Authority {
			return invalid()
		}
	} else if len(failure.Evidence) != 0 {
		return invalid()
	}
	if record.AttachRemoval == nil {
		if record.Sequence != 0 || !record.UpdatedAt.Equal(record.Attempt.AssignedAt) {
			return invalid()
		}
		return nil
	}
	if record.AttachRemoval.Completion != nil {
		return invalid()
	}
	record.AttachFailure = nil
	return record.validateAttachRemoval()
}
