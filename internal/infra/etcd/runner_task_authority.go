package etcd

import (
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func newRunnerTaskInitiation(
	desired runnerrecord.RunnerDesiredRecord,
	parents runnerParents,
	actor TaskActor,
) (TaskInitiation, error) {
	owner, err := runnerTaskOwner(desired)
	if err != nil {
		return TaskInitiation{}, err
	}
	fences := []etcdstore.Condition{{Key: hierarchyrecord.TenantKey(desired.TenantID), ModRevision: parents.tenant.Revision}}
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		fences = append(fences, etcdstore.Condition{Key: hierarchyrecord.ProjectKey(desired.OwnerID), ModRevision: parents.project.Revision})
	}
	return newTaskInitiation(owner, actor, fences...)
}

func validateRunnerCreateTask(desired runnerrecord.RunnerDesiredRecord, task TaskRecord) error {
	owner, err := runnerTaskOwner(desired)
	if err != nil {
		return err
	}
	if task.Executor != TaskExecutorController || task.Type != TaskCreate || task.Target != desired.ID ||
		task.Owner != owner ||
		task.Status != TaskStatusPending || task.IdempotencyKey == "" || len(task.Params) != 2 ||
		task.Params[TaskResourceKindParam] != TaskResourceRunner ||
		task.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return errs.New(errs.KindValidationFailed, "runner creation task has invalid durable input")
	}
	return nil
}

func runnerTaskOwner(desired runnerrecord.RunnerDesiredRecord) (TaskOwner, error) {
	if err := runnerrecord.ValidateRunnerOwnership(desired); err != nil {
		return TaskOwner{}, err
	}
	if desired.OwnerKind == runnerrecord.RunnerOwnerTenant {
		return TenantTaskOwner(desired.TenantID)
	}
	return TenantProjectTaskOwner(desired.TenantID, desired.OwnerID)
}

func validateRunnerCreateMarker(desired runnerrecord.RunnerDesiredRecord, task TaskRecord, marker IdempotencyMarker) error {
	return validateRunnerOperationMarker(desired, task, marker, http.MethodPost, "/runners", nil, true)
}

func validateRunnerRetryMarker(desired runnerrecord.RunnerDesiredRecord, task TaskRecord, marker IdempotencyMarker) error {
	if err := validateRunnerRetryMarkerEnvelope(task, marker); err != nil {
		return err
	}
	return validateRunnerMarkerScope(desired, marker)
}

func validateRunnerDeleteMarker(desired runnerrecord.RunnerDesiredRecord, task TaskRecord, marker IdempotencyMarker) error {
	target := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRunner, ID: desired.ID}
	return validateRunnerOperationMarker(desired, task, marker, http.MethodDelete, "/runners/{id}", &target, true)
}

func validateRunnerOperationMarker(
	desired runnerrecord.RunnerDesiredRecord,
	task TaskRecord,
	marker IdempotencyMarker,
	method string,
	route string,
	replayTarget *IdempotencyReplayTarget,
	requireOperationKey bool,
) error {
	if err := validateRunnerMarkerEnvelope(task, marker, method, route, replayTarget, requireOperationKey); err != nil {
		return err
	}
	return validateRunnerMarkerScope(desired, marker)
}

func validateRunnerRetryMarkerEnvelope(task TaskRecord, marker IdempotencyMarker) error {
	if marker.Locator.ScopeKind != IdempotencyScopeTenant && marker.Locator.ScopeKind != IdempotencyScopeProject {
		return errs.New(errs.KindValidationFailed, "runner retry marker scope is invalid")
	}
	return validateRunnerMarkerEnvelope(task, marker, http.MethodPost, "/runners/{id}/retry", nil, false)
}

func validateRunnerMarkerEnvelope(
	task TaskRecord,
	marker IdempotencyMarker,
	method string,
	route string,
	replayTarget *IdempotencyReplayTarget,
	requireOperationKey bool,
) error {
	if task.IdempotencyKey == "" || (requireOperationKey && task.IdempotencyKey != marker.Locator.Key) ||
		marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != method || marker.Locator.Route != route ||
		!validTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() ||
		!runnerReplayTargetsEqual(marker.ReplayTarget, replayTarget) {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its task")
	}
	return validateIdempotencyMarker(marker)
}

func validateRunnerMarkerScope(desired runnerrecord.RunnerDesiredRecord, marker IdempotencyMarker) error {
	scopeKind := IdempotencyScopeTenant
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		scopeKind = IdempotencyScopeProject
	}
	if marker.Locator.ScopeKind != scopeKind || marker.Locator.ScopeID != desired.OwnerID {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its owner-scoped task")
	}
	return nil
}

func runnerReplayTargetsEqual(left *IdempotencyReplayTarget, right *IdempotencyReplayTarget) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func bindRunnerTaskMarker(task TaskRecord, marker IdempotencyMarker) TaskRecord {
	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	return task
}
