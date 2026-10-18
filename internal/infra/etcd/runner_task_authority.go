package etcd

import (
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func newRunnerTaskInitiation(
	desired runnerrecord.RunnerDesiredRecord,
	parents runnerrecord.RunnerParents,
	actor taskjournal.TaskActor,
) (TaskInitiation, error) {
	owner, err := runnerTaskOwner(desired)
	if err != nil {
		return TaskInitiation{}, err
	}
	return newTaskInitiation(owner, actor, parents.ExistenceConditions()...)
}

func runnerTaskOwner(desired runnerrecord.RunnerDesiredRecord) (taskjournal.TaskOwner, error) {
	return runnerrecord.TaskOwner(desired)
}

func validateRunnerMarkerScope(
	desired runnerrecord.RunnerDesiredRecord,
	marker idempotencyrecord.IdempotencyMarker,
) error {
	scopeKind := desired.IdempotencyScope()
	if marker.Locator.ScopeKind != scopeKind || marker.Locator.ScopeID != desired.OwnerID {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its owner-scoped task")
	}
	return nil
}

func runnerReplayTargetsEqual(
	left *idempotencyrecord.IdempotencyReplayTarget,
	right *idempotencyrecord.IdempotencyReplayTarget,
) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}
