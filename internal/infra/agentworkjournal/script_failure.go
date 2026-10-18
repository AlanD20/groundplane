package agentworkjournal

import "github.com/AlanD20/groundplane/internal/common/workexecution"

type ScriptFailureDelivery struct {
	Receipt *workexecution.Attempt `json:"receipt,omitempty"`
}

func (record attemptRecord) validateScriptFailure() error {
	if record.ScriptFailure == nil || record.Release != nil || record.Recovery != nil || record.Failure != nil ||
		record.Effect != nil || record.Completion != nil || record.ScriptCompletion != nil {
		return invalid()
	}
	if terminal := record.ScriptFailure.Receipt; terminal != nil {
		if terminal.Validate() != nil || terminal.FinishedAt == nil || terminal.Result == nil ||
			!workexecution.SameAssignment(record.Attempt, *terminal) || terminal.FinishedAt.Before(record.UpdatedAt) {
			return invalid()
		}
	}
	if record.ScriptRecovery != nil {
		if record.ScriptRecovery.Proof != nil || record.ScriptRecovery.Receipt != nil {
			return invalid()
		}
		return record.validateScriptRecovery()
	}
	if record.Sequence == 0 {
		if len(record.Script) != 0 || !record.UpdatedAt.Equal(record.Attempt.AssignedAt) {
			return invalid()
		}
		return nil
	}
	// Validate retained acknowledged execution evidence with the ordinary record
	// rules, without re-entering this terminal-delivery branch.
	record.ScriptFailure = nil
	pending, err := record.pending()
	if err != nil {
		return err
	}
	if pending {
		return conflict()
	}
	return record.validate()
}
