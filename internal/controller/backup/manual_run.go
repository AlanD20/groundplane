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
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *BackupRunService) RunBackup(ctx context.Context, environmentID, key string,
	input api.TaskRunAfterRequest, fixedRevision ...int64,
) (idempotency.IdempotencyResponse, error) {
	if service == nil || service.accepted == nil || service.coordinator == nil || len(fixedRevision) > 1 {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Backup acceptance is not configured")
	}
	// The manual API accepts an action, not a captured MVCC view. A caller may
	// request a fixed read only when it is compiled, never silently refresh it.
	request := acceptedBackupRun{After: append([]string(nil), input.After...)}
	if len(fixedRevision) == 1 {
		request.FixedRevision = fixedRevision[0]
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: backupRunRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: environmentID}}, Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(input.After)},
			requestidempotency.Field{
				Name:  "fixed_revision",
				Value: requestidempotency.Integer(request.FixedRevision),
			},
		))})
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer digest.Destroy()
	evidence, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer evidence.Destroy()
	locator := idempotency.IdempotencyLocator{Method: http.MethodPost, Route: backupRunRoute, Key: key,
		ScopeKind: idempotency.IdempotencyScopeEnvironment, ScopeID: environmentID}
	resolution, exists, err := service.idempotency.ResolveExisting(ctx, locator, backupRunEvidence{candidate: evidence})
	if err != nil || exists {
		return resolution.Response, err
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
		Action: workexecution.ActionBackup, TargetID: environmentID, TargetName: owner.Name,
		Scope: workexecution.Scope{
			TenantID:      owner.Owner.TenantID,
			ProjectID:     owner.Owner.ProjectID,
			EnvironmentID: environmentID,
		},
		After: request.After, CreatedAt: at, UpdatedAt: at, Deadline: at.Add(acceptedaction.QueueTimeout)}
	return acceptedaction.Submit(ctx, service.accepted, acceptedaction.Submission{Task: task, Kind: "backup-run",
		Locator: locator, Intent: intent, Evidence: evidence}, request)
}

type acceptedBackupRun struct {
	After         []string `json:"after,omitempty"`
	FixedRevision int64    `json:"fixed_revision,omitempty"`
}

func (service *BackupRunService) AcceptedPreparations() []taskdependencies.Registration {
	return []taskdependencies.Registration{acceptedaction.Registration(service.accepted, "backup-run",
		workexecution.ActionBackup, ids.KindEnvironment, service.prepareAcceptedRun)}
}

func (service *BackupRunService) prepareAcceptedRun(ctx context.Context, snapshot workqueue.TaskSnapshot,
	_ int64, at time.Time, input acceptedBackupRun,
) (taskdependencies.Prepared, error) {
	task := snapshot.Task
	planID := ids.DeriveAt(ids.KindPlan, task.CreatedAt, task.ID, "backup-input")
	prepared, err := service.repository.PrepareBackupRun(ctx, BackupRunPrepareInput{
		EnvironmentID: task.Scope.EnvironmentID, TaskID: task.ID,
		OperationID: ids.DeriveAt(ids.KindOperation, task.CreatedAt, task.ID, "backup-operation"),
		PlanID:      planID, FixedRevision: input.FixedRevision, CreatedAt: at,
		Initiator: backupruntime.BackupRunInitiatorOperator})
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	defer prepared.Publication.Clear()
	capturedTask, item, inputBytes, err := buildBackupRunWork(prepared, planID)
	if err != nil {
		return taskdependencies.Prepared{}, err
	}
	if capturedTask.Scope != task.Scope {
		return taskdependencies.Prepared{}, errs.New(
			errs.KindStateConflict,
			"Backup Environment ownership changed while waiting",
		)
	}
	task.Intent, task.ExpectedItems, task.UpdatedAt = capturedTask.Intent, 1, at
	item.ID = ids.DeriveAt(ids.KindWorkItem, task.CreatedAt, task.ID, "backup-work")
	item.Deadline = acceptedaction.ExecutionDeadline(task, at, 6*time.Hour)
	authority, err := prepared.Publication.PrepareWorkAuthority(ctx, task, item, inputBytes)
	return taskdependencies.Prepared{Task: task, Graph: []workexecution.Item{item}, Authority: authority}, err
}
