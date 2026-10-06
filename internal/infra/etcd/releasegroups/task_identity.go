package releasegroups

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type MutationTaskIdentity struct {
	Type     taskjournal.TaskType
	Executor taskjournal.TaskExecutor
	Target   string
	Status   taskjournal.TaskStatus
	Params   map[string]string
}

func ValidatePreparedDirect(prepared ReleaseGroupPreparedMutation, marker idempotency.IdempotencyMarker) error {
	if prepared.TaskType() != taskjournal.TaskCreate && prepared.TaskType() != taskjournal.TaskUpdate {
		return errs.New(
			errs.KindValidationFailed,
			"release group direct mutation type is invalid",
		)
	}
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID()) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.GroupID()) != nil {
		return errs.New(
			errs.KindValidationFailed,
			"release group direct mutation identity is invalid",
		)
	}
	if marker.Kind != idempotency.IdempotencyMarkerDirect ||
		marker.State != idempotency.IdempotencyMarkerCompleted ||
		marker.Locator.ScopeKind != idempotency.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != prepared.EnvironmentID() {
		return errs.New(
			errs.KindValidationFailed,
			"release group direct marker is invalid",
		)
	}
	if err := ValidateReleaseGroupPreparedFragment(prepared); err != nil {
		return err
	}
	return nil
}

func ValidatePreparedTask(prepared ReleaseGroupPreparedMutation, task MutationTaskIdentity) error {
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID()) != nil ||
		ids.Validate(ids.KindReleaseGroup, prepared.GroupID()) != nil || prepared.TaskType() != task.Type ||
		task.Executor != taskjournal.TaskExecutorController || task.Target != prepared.GroupID() ||
		task.Status != taskjournal.TaskStatusPending || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceReleaseGroup ||
		len(task.Params) != 1 ||
		(prepared.TaskType() != taskjournal.TaskCreate && prepared.TaskType() != taskjournal.TaskUpdate && prepared.TaskType() != taskjournal.TaskRemove) {
		return errs.New(errs.KindValidationFailed, "release group prepared mutation is invalid")
	}
	if prepared.TaskType() == taskjournal.TaskRemove && prepared.GroupRevision() <= 0 {
		return errs.New(errs.KindValidationFailed, "release group removal revision is invalid")
	}
	if err := ValidateReleaseGroupPreparedFragment(prepared); err != nil {
		return err
	}
	return prepared.ValidateRemovalPrimaryMutations()
}
