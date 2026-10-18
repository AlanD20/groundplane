package releasegroups

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

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

func ValidatePreparedRemoval(prepared ReleaseGroupPreparedMutation, input RemovalInput) error {
	if err := input.Validate(); err != nil {
		return err
	}
	if ids.Validate(ids.KindEnvironment, prepared.EnvironmentID()) != nil ||
		prepared.TaskType() != taskjournal.TaskRemove || prepared.GroupID() != input.GroupID ||
		prepared.EnvironmentID() != input.Scope.EnvironmentID || prepared.GroupRevision() != input.GroupRevision {
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
