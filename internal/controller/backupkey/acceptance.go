package backupkey

import (
	"context"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	"github.com/AlanD20/groundplane/internal/controller/taskdependencies"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) RotateBackupKey(ctx context.Context, environmentID, key string,
	input api.TaskRunAfterRequest,
) (idempotency.IdempotencyResponse, error) {
	if service == nil || service.accepted == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backup key acceptance is not configured")
	}
	evidence, err := service.idempotency.Prepare(ctx, environmentID, input.After)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environmentID, Method: http.MethodPost, Route: backupKeyRotationRoute, Key: key}
	resolved, exists, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil || exists {
		return resolved.Response, err
	}
	owner, err := service.repository.ReadBackupTaskAcceptance(ctx, environmentID)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	intent, err := evidence.DurableRecord()
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(intent.Ciphertext)
	at := service.now().UTC()
	task := workexecution.Task{ID: ids.New(ids.KindTask), Actor: workexecution.OperatorActor,
		Action: workexecution.ActionRotate, TargetID: environmentID, TargetName: owner.Name,
		Scope: workexecution.Scope{
			TenantID:      owner.Owner.TenantID,
			ProjectID:     owner.Owner.ProjectID,
			EnvironmentID: environmentID,
		},
		After: append(
			[]string(nil),
			input.After...), CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{Task: task, Kind: "backup-key-rotate",
		Locator: locator, Intent: intent, Evidence: evidence}, input)
}

func (service *Service) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.accepted, "backup-key-rotate",
		workexecution.ActionRotate, ids.KindEnvironment, service.prepareAcceptedRotation)}
}

func (service *Service) prepareAcceptedRotation(ctx context.Context, task workqueue.TaskSnapshot,
	_ int64, at time.Time, _ api.TaskRunAfterRequest,
) (taskdependencies.Prepared, error) {
	material, err := service.keys.Create(ctx)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer clear(material.Ciphertext)
	accepted := task.Task
	prepared, err := service.repository.PrepareBackupKeyRotation(ctx, etcd.BackupKeyRotationInput{
		EnvironmentID: accepted.Scope.EnvironmentID, TaskID: accepted.ID,
		WorkItemID:  ids.DeriveAt(ids.KindWorkItem, accepted.CreatedAt, accepted.ID, "key-rotate-work"),
		OperationID: ids.DeriveAt(ids.KindOperation, accepted.CreatedAt, accepted.ID, "key-rotate-operation"),
		PlanID:      ids.DeriveAt(ids.KindPlan, accepted.CreatedAt, accepted.ID, "key-rotate-input"), CreatedAt: at,
	}, material)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer prepared.Clear()
	capture, err := service.repository.PrepareBackupKeyRotationAuthority(ctx, prepared, accepted, at)
	return taskdependencies.Prepared{
		Task:      capture.Task,
		Graph:     []workexecution.Item{capture.Item},
		Authority: capture.Authority,
	}, err
}
