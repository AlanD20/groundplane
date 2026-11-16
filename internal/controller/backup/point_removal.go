package backup

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const recoveryPointRemovalRoute = "/environments/{id}/recovery-points/{point}"

type acceptedPointRemoval struct {
	PointID string   `json:"point_id"`
	After   []string `json:"after,omitempty"`
}

func (service *BackupPruneService) RemoveRecoveryPoint(ctx context.Context, environmentID, pointID, key string,
	input api.TaskRunAfterRequest,
) (idempotency.IdempotencyResponse, error) {
	if service == nil || service.accepted == nil || service.coordinator == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Point removal is not configured")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil || ids.Validate(ids.KindRecoveryPoint, pointID) != nil {
		return idempotency.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Point removal identity is invalid",
		)
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: recoveryPointRemovalRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path: []requestidempotency.PathBinding{
			{Name: "id", Value: environmentID},
			{Name: "point", Value: pointID},
		}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(
			requestidempotency.Object(
				requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
			),
		)})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{
		Method:    http.MethodDelete,
		Route:     recoveryPointRemovalRoute,
		Key:       key,
		ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID:   environmentID,
	}
	resolution, exists, err := service.idempotency.ResolveExisting(ctx, locator, backupRunEvidence{candidate: evidence})
	if err != nil || exists {
		return resolution.Response, err
	}
	owner, err := service.runtime.ReadBackupTaskAcceptance(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	at := service.now().UTC()
	task := workexecution.Task{
		ID:         ids.New(ids.KindTask),
		Actor:      workexecution.OperatorActor,
		Action:     workexecution.ActionBackupPrune,
		TargetID:   environmentID,
		TargetName: owner.Name,
		Scope: workexecution.Scope{
			TenantID:      owner.Owner.TenantID,
			ProjectID:     owner.Owner.ProjectID,
			EnvironmentID: environmentID,
		},
		After: append(
			[]string(nil),
			input.After...),
		CreatedAt: at,
		UpdatedAt: at,
		Deadline:  at.Add(acceptedaction.QueueTimeout),
	}
	request := acceptedPointRemoval{PointID: pointID, After: append([]string(nil), input.After...)}
	return acceptedaction.Submit(
		ctx,
		service.accepted,
		acceptedaction.Submission{
			Task:     task,
			Kind:     "backup-point-remove",
			Locator:  locator,
			Intent:   intent,
			Evidence: evidence,
		},
		request,
	)
}

func (service *BackupPruneService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{
		acceptedaction.Registration(
			service.accepted,
			"backup-point-remove",
			workexecution.ActionBackupPrune,
			ids.KindEnvironment,
			service.prepareAcceptedPointRemoval,
		),
	}
}

func (service *BackupPruneService) prepareAcceptedPointRemoval(ctx context.Context, accepted workqueue.TaskSnapshot,
	_ int64, at time.Time, request acceptedPointRemoval,
) (taskdependencies.Prepared, error) {
	task := accepted.Task
	prepared, err := service.runtime.PrepareRecoveryPointRemoval(
		ctx,
		task.Scope.EnvironmentID,
		request.PointID,
		task.ID,
		ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "point-remove-operation"),
		at,
	)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer prepared.Publication.Clear()
	captured, item, input, err := buildPruneWork(prepared)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if captured.Scope != task.Scope {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Point removal ownership changed while waiting",
		)
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = captured.Intent, 1, at
	item.ID = ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "point-remove-work")
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, 30*time.Minute)
	authority, err := prepared.Publication.PrepareWorkAuthority(ctx, task, item, input)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, err
}
