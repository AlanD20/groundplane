package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func recoveredAuthoredZoneAbsence(task TaskRecord, assignment taskassignments.TaskAssignmentRecord) bool {
	return authoredBlueprintZoneRemovalTask(task) &&
		assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly &&
		assignment.ManagedNetworkProcedureSHA256 != "" && assignment.ReleaseRecoveryRecordSHA256 != "" &&
		retainedAuthoredZoneAbsence(task) && task.Result.ExecutionEpoch == assignment.ExecutionEpoch &&
		task.Result.ReleaseRecoveryRecordSHA256 == assignment.ReleaseRecoveryRecordSHA256
}

// This marker is stored only after recovery acknowledgement verifies the exact
// completed event and immutable procedure. It distinguishes effect retirement
// from the original terminal outcome when validating a retained Task replay.
func retainedAuthoredZoneAbsence(task TaskRecord) bool {
	return authoredBlueprintZoneRemovalTask(task) && task.Result != nil &&
		task.Result.Kind == taskjournal.TaskResultCompose && !task.Result.ReconciliationRequired &&
		task.Result.ExecutionEpoch > 1 && task.Result.ReleaseRecoveryRecordSHA256 != ""
}
