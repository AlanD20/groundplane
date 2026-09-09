package volumeremoval

import (
	"crypto/sha256"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type EnvironmentVolumeRemovalAssignment struct {
	OperationID     string
	TaskID          string
	AssignmentID    string
	AgentID         string
	AgentGeneration uint64
}

type EnvironmentVolumeRemovalPathResult struct {
	Assignment         EnvironmentVolumeRemovalAssignment
	RequestOrdinal     uint64
	RequestSHA256      [sha256.Size]byte
	ResponseSHA256     [sha256.Size]byte
	ResponseBytes      uint32
	MutationCount      uint32
	NextComponentStack []string
	NextCursor         []byte
	DirectoryAbsent    bool
	CompletedAt        time.Time
}

type EnvironmentVolumeRemovalResumeState struct {
	Runtime  etcd.Versioned[removalrecord.Runtime]
	Attempt  removalrecord.Attempt
	Progress etcd.Versioned[removalrecord.Progress]
	Pending  *etcd.Versioned[removalrecord.PendingPath]
}

func validateEnvironmentVolumeRemovalTask(
	task etcd.TaskRecord,
	runtime removalrecord.Runtime,
	attempt removalrecord.Attempt,
) error {
	if err := etcd.ValidateCapabilityTaskRecord(task); err != nil {
		return err
	}
	if err := removalrecord.ValidateRuntime(runtime); err != nil {
		return err
	}
	if err := removalrecord.ValidateAttempt(attempt); err != nil {
		return err
	}
	expectedParams := etcd.EnvironmentVolumeRemovalTaskParams(runtime, attempt.Ordinal)
	if task.ID != attempt.TaskID || task.ID != runtime.CurrentTaskID ||
		task.OperationID != runtime.OperationID || task.RetryOf != attempt.PredecessorTaskID ||
		task.Owner.EnvironmentID != runtime.EnvironmentID || task.Actor != etcd.TaskActorOperator ||
		task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskRemove || task.Target != runtime.VolumeID ||
		task.RenderGeneration != int32(runtime.DesiredGeneration) ||
		task.TimeoutSeconds != removalrecord.TimeoutSeconds ||
		task.IdempotencyKey != runtime.RootLocator.Key || len(task.Steps) != 1 ||
		task.Steps[0].ID != runtime.StepID || !equalVolumeRemovalParams(task.Params, expectedParams) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal Task inputs changed")
	}
	return nil
}

func equalVolumeRemovalParams(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range right {
		if left[key] != value {
			return false
		}
	}
	return true
}

func validateEnvironmentVolumeRemovalTaskResult(
	result etcd.TaskResultRecord,
	steps []etcd.TaskStepRecord,
	status etcd.TaskStatus,
) error {
	if err := etcd.ValidateCapabilityTaskResult(result, steps, status); err != nil {
		return err
	}
	if result.Kind != etcd.TaskResultEnvironmentDirectory ||
		result.Diagnostic != etcd.TaskResultDiagnosticNone || result.ReconciliationRequired ||
		len(result.Projects) != 0 || (status == etcd.TaskStatusCompleted && result.ExitCode != 0) {
		return errs.New(errs.KindValidationFailed, "Environment Volume removal result is invalid")
	}
	return nil
}

func volumeRemovalRootLocator(runtime removalrecord.Runtime) etcd.IdempotencyLocator {
	locator := runtime.RootLocator
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeKind(locator.ScopeKind),
		ScopeID:   locator.ScopeID, Method: locator.Method, Route: locator.Route, Key: locator.Key,
	}
}
