package volumeremoval

import (
	"crypto/sha256"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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
	Runtime  etcdstore.Versioned[removalrecord.Runtime]
	Attempt  removalrecord.Attempt
	Progress etcdstore.Versioned[removalrecord.Progress]
	Pending  *etcdstore.Versioned[removalrecord.PendingPath]
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
		task.IdempotencyKey != runtime.RootLocator.Key || !etcd.EnvironmentVolumeRemovalStepMatches(task.Steps, runtime.StepID) ||
		!equalVolumeRemovalParams(task.Params, expectedParams) {
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

func volumeRemovalRootLocator(runtime removalrecord.Runtime) idempotencyrecord.IdempotencyLocator {
	locator := runtime.RootLocator
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeKind(locator.ScopeKind),
		ScopeID:   locator.ScopeID, Method: locator.Method, Route: locator.Route, Key: locator.Key,
	}
}
