package backup

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type acceptedRestore struct {
	Request     api.RestoreRequest `json:"request"`
	HasIdentity bool               `json:"has_identity"`
}

func (service *RestoreService) Restore(ctx context.Context, environmentID, key string,
	request api.RestoreRequest,
) (idempotency.IdempotencyResponse, error) {
	if service == nil || service.accepted == nil || ctx == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Restore acceptance is unavailable")
	}
	identity, evidence, err := service.prepareRestoreIntent(ctx, environmentID, request)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(identity)
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environmentID, Method: http.MethodPost, Route: restoreRoute, Key: key}
	resolved, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, evidence)
	if err != nil || existing {
		return resolved.Response, err
	}
	owner, err := service.runtime.ReadBackupTaskAcceptance(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	at := time.Now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionRestore, TargetID: environmentID, TargetName: owner.Name,
		Scope: workexecution.Scope{
			TenantID:      owner.Owner.TenantID,
			ProjectID:     owner.Owner.ProjectID,
			EnvironmentID: environmentID,
		},
		After: append(
			[]string(nil),
			request.After...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	if len(identity) != 0 {
		if err := service.secrets.retainQueuedRestoreIdentity(ctx, task.ID, environmentID, task.Deadline, identity); err != nil {
			return idempotency.IdempotencyResponse{}, err
		}
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		service.secrets.identities.release(task.ID)
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	// A private old-era identity remains request-only even while queued. Neither
	// encrypted accepted storage nor diagnostic history receives its bytes.
	request.AgeIdentity = ""
	response, err := acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{
		Task: task, Kind: "backup-restore", Locator: locator, Intent: intent, Evidence: evidence},
		acceptedRestore{Request: request, HasIdentity: len(identity) != 0})
	if err != nil && !restorePublicationUnknown(err) {
		service.secrets.identities.release(task.ID)
	}
	return response, err
}

func (service *RestoreService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.accepted, "backup-restore",
		workexecution.ActionRestore, ids.KindEnvironment, service.prepareAcceptedRestore)}
}

func (service *RestoreService) prepareAcceptedRestore(ctx context.Context, snapshot workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedRestore,
) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	var identity []byte
	if input.HasIdentity {
		var err error
		identity, err = service.secrets.queuedRestoreIdentity(ctx, task.ID, task.Scope.EnvironmentID)
		if err != nil {
			return taskdependencies.Prepared{}, err
		}
		defer clear(identity)
	}
	prepared, err := service.prepareRestore(ctx, task.Scope.EnvironmentID, task.ID,
		ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "restore-operation"), input.Request, at,
		input.HasIdentity, false)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer prepared.Publication.Clear()
	captured, item, native, err := buildRestoreWork(prepared,
		ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "restore-input"))
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if captured.Scope != task.Scope {
		return taskdependencies.Prepared{}, errs.New(errs.KindStateConflict, "Restore ownership changed while waiting")
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = captured.Intent, 1, at
	item.ID = ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "restore-work")
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, 6*time.Hour)
	if input.HasIdentity {
		if err := service.secrets.RetainRestoreIdentity(ctx, prepared.Restore, identity); err != nil {
			return taskdependencies.Prepared{}, err
		}
	}
	authority, err := prepared.Publication.PrepareWorkAuthority(ctx, task, item, native)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, err
}
