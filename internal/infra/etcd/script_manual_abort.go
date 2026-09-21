package etcd

import (
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// abortAssignedManualScriptBeforeStart supplies absence evidence only for a
// durable not-started record. Its caller must atomically fence the execution,
// current Task/assignment, and source root when committing this replacement.
func abortAssignedManualScriptBeforeStart(
	execution scriptexecutions.ScriptExecutionRecord,
	at time.Time,
) (scriptexecutions.ScriptExecutionRecord, error) {
	if scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || execution.State != scriptexecutions.ScriptExecutionNotStarted ||
		execution.StartAuthorized || execution.AssignmentID != "" || !execution.ActiveReference ||
		!at.After(execution.UpdatedAt) {
		return scriptexecutions.ScriptExecutionRecord{}, errs.New(
			errs.KindStateConflict,
			"manual Script may already have started",
		)
	}
	outcome := scriptexecutions.ScriptOutcomeEvidence{
		Reason:     scriptexecutions.ScriptOutcomeAbortBeforeStart,
		ObservedAt: at.UTC(),
	}
	cleanup := scriptexecutions.ScriptCleanupEvidence{
		ContainerAbsent:          true,
		BodyAbsent:               true,
		ExecutionDirectoryAbsent: true,
	}
	digest, err := scriptControllerCleanupSHA256(outcome, cleanup)
	if err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	execution.State, execution.ControllerCleanup = scriptexecutions.ScriptExecutionCleanupProven, scriptexecutions.ScriptControllerCleanupManualAssignedAbort
	execution.Outcome, execution.Cleanup, execution.LastCheckpointSHA256 = &outcome, &cleanup, digest
	execution.ActiveReference, execution.UpdatedAt = false, at.UTC()
	if err := scriptexecutions.ValidateScriptExecutionRecord(execution); err != nil {
		return scriptexecutions.ScriptExecutionRecord{}, err
	}
	return execution, nil
}

func manualScriptAssignedAbortMatches(execution scriptexecutions.ScriptExecutionRecord) bool {
	if scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || execution.State != scriptexecutions.ScriptExecutionCleanupProven ||
		execution.ControllerCleanup != scriptexecutions.ScriptControllerCleanupManualAssignedAbort || execution.Outcome == nil ||
		execution.Outcome.Reason != scriptexecutions.ScriptOutcomeAbortBeforeStart ||
		execution.Cleanup == nil ||
		!execution.UpdatedAt.Equal(execution.Outcome.ObservedAt) {
		return false
	}
	digest, err := scriptControllerCleanupSHA256(*execution.Outcome, *execution.Cleanup)
	return err == nil && digest == execution.LastCheckpointSHA256
}
