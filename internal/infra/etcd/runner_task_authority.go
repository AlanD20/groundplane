package etcd

import (
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

func newRunnerTaskInitiation(
	desired runnerrecord.RunnerDesiredRecord,
	parents runnerParents,
	actor taskjournal.TaskActor,
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
	if task.Executor != taskjournal.TaskExecutorController || task.Type != taskjournal.TaskCreate || task.Target != desired.ID ||
		task.Owner != owner ||
		task.Status != taskjournal.TaskStatusPending || task.IdempotencyKey == "" || len(task.Params) != 2 ||
		task.Params[taskjournal.TaskResourceKindParam] != TaskResourceRunner ||
		task.Params[RunnerRegistrationTokenPresentParam] != "true" {
		return errs.New(errs.KindValidationFailed, "runner creation task has invalid durable input")
	}
	return nil
}

func runnerTaskOwner(desired runnerrecord.RunnerDesiredRecord) (taskjournal.TaskOwner, error) {
	if err := runnerrecord.ValidateRunnerOwnership(desired); err != nil {
		return taskjournal.TaskOwner{}, err
	}
	if desired.OwnerKind == runnerrecord.RunnerOwnerTenant {
		return taskjournal.TenantTaskOwner(desired.TenantID)
	}
	return taskjournal.TenantProjectTaskOwner(desired.TenantID, desired.OwnerID)
}

func validateRunnerCreateMarker(desired runnerrecord.RunnerDesiredRecord, task TaskRecord, marker idempotencyrecord.IdempotencyMarker) error {
	return validateRunnerOperationMarker(desired, task, marker, http.MethodPost, "/runners", nil, true)
}

func validateRunnerRetryMarker(desired runnerrecord.RunnerDesiredRecord, task TaskRecord, marker idempotencyrecord.IdempotencyMarker) error {
	if err := validateRunnerRetryMarkerEnvelope(task, marker); err != nil {
		return err
	}
	return validateRunnerMarkerScope(desired, marker)
}

func validateRunnerDeleteMarker(desired runnerrecord.RunnerDesiredRecord, task TaskRecord, marker idempotencyrecord.IdempotencyMarker) error {
	target := idempotencyrecord.IdempotencyReplayTarget{Kind: idempotencyrecord.IdempotencyReplayTargetRunner, ID: desired.ID}
	return validateRunnerOperationMarker(desired, task, marker, http.MethodDelete, "/runners/{id}", &target, true)
}

func validateRunnerOperationMarker(
	desired runnerrecord.RunnerDesiredRecord,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	method string,
	route string,
	replayTarget *idempotencyrecord.IdempotencyReplayTarget,
	requireOperationKey bool,
) error {
	if err := validateRunnerMarkerEnvelope(task, marker, method, route, replayTarget, requireOperationKey); err != nil {
		return err
	}
	return validateRunnerMarkerScope(desired, marker)
}

func validateRunnerRetryMarkerEnvelope(task TaskRecord, marker idempotencyrecord.IdempotencyMarker) error {
	if marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeTenant && marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeProject {
		return errs.New(errs.KindValidationFailed, "runner retry marker scope is invalid")
	}
	return validateRunnerMarkerEnvelope(task, marker, http.MethodPost, "/runners/{id}/retry", nil, false)
}

func validateRunnerMarkerEnvelope(
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	method string,
	route string,
	replayTarget *idempotencyrecord.IdempotencyReplayTarget,
	requireOperationKey bool,
) error {
	if task.IdempotencyKey == "" || (requireOperationKey && task.IdempotencyKey != marker.Locator.Key) ||
		marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.Method != method || marker.Locator.Route != route ||
		!idempotencyrecord.ValidTaskResponse(marker.Response, task.ID) ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) ||
		!marker.TerminalAt.IsZero() || !marker.RetainUntil.IsZero() ||
		!runnerReplayTargetsEqual(marker.ReplayTarget, replayTarget) {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its task")
	}
	return idempotencyrecord.ValidateIdempotencyMarker(marker)
}

func validateRunnerMarkerScope(desired runnerrecord.RunnerDesiredRecord, marker idempotencyrecord.IdempotencyMarker) error {
	scopeKind := idempotencyrecord.IdempotencyScopeTenant
	if desired.OwnerKind == runnerrecord.RunnerOwnerProject {
		scopeKind = idempotencyrecord.IdempotencyScopeProject
	}
	if marker.Locator.ScopeKind != scopeKind || marker.Locator.ScopeID != desired.OwnerID {
		return errs.New(errs.KindValidationFailed, "runner task marker does not match its owner-scoped task")
	}
	return nil
}

func runnerReplayTargetsEqual(left *idempotencyrecord.IdempotencyReplayTarget, right *idempotencyrecord.IdempotencyReplayTarget) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return *left == *right
}

func bindRunnerTaskMarker(task TaskRecord, marker idempotencyrecord.IdempotencyMarker) TaskRecord {
	task = cloneTaskRecord(task)
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	return task
}
