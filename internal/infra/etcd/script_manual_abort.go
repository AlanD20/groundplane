package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// abortAssignedManualScriptBeforeStart supplies absence evidence only for a
// durable not-started record. Its caller must atomically fence the execution,
// current Task/assignment, and source root when committing this replacement.
func abortAssignedManualScriptBeforeStart(
	execution ScriptExecutionRecord,
	at time.Time,
) (ScriptExecutionRecord, error) {
	if validateScriptExecutionRecord(execution) != nil || execution.State != ScriptExecutionNotStarted ||
		execution.StartAuthorized || execution.AssignmentID != "" || !execution.ActiveReference || !at.After(execution.UpdatedAt) {
		return ScriptExecutionRecord{}, errs.New(errs.KindStateConflict, "manual Script may already have started")
	}
	outcome := ScriptOutcomeEvidence{Reason: ScriptOutcomeAbortBeforeStart, ObservedAt: at.UTC()}
	cleanup := ScriptCleanupEvidence{ContainerAbsent: true, BodyAbsent: true, ExecutionDirectoryAbsent: true}
	digest, err := scriptControllerCleanupSHA256(outcome, cleanup)
	if err != nil {
		return ScriptExecutionRecord{}, err
	}
	execution.State, execution.ControllerCleanup = ScriptExecutionCleanupProven, ScriptControllerCleanupManualAssignedAbort
	execution.Outcome, execution.Cleanup, execution.LastCheckpointSHA256 = &outcome, &cleanup, digest
	execution.ActiveReference, execution.UpdatedAt = false, at.UTC()
	if err := validateScriptExecutionRecord(execution); err != nil {
		return ScriptExecutionRecord{}, err
	}
	return execution, nil
}

func manualScriptAssignedAbortMatches(execution ScriptExecutionRecord) bool {
	if validateScriptExecutionRecord(execution) != nil || execution.State != ScriptExecutionCleanupProven ||
		execution.ControllerCleanup != ScriptControllerCleanupManualAssignedAbort || execution.Outcome == nil ||
		execution.Outcome.Reason != ScriptOutcomeAbortBeforeStart || execution.Cleanup == nil ||
		!execution.UpdatedAt.Equal(execution.Outcome.ObservedAt) {
		return false
	}
	digest, err := scriptControllerCleanupSHA256(*execution.Outcome, *execution.Cleanup)
	return err == nil && digest == execution.LastCheckpointSHA256
}
