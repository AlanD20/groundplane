package backup

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
	"time"
)

const (
	backupRunRoute          = "/environments/{id}/backup-run"
	scheduledBackupRunRoute = "/system/backup-schedules/{id}/backup-run"
)

// BackupRunService coordinates the exact bodyless manual backup intent.
type BackupRunService struct {
	repository  BackupRunRepository
	plans       BackupRunPlanBuilder
	idempotency backupRunIdempotency
	now         func() time.Time
}

// NewBackupRunService constructs the application-level manual backup
// publisher. The idempotency dependency is injected so replay ordering is
// testable without an HTTP server.
func NewBackupRunService(
	repository BackupRunRepository,
	plans BackupRunPlanBuilder,
	idempotency backupRunIdempotency,
) *BackupRunService {
	return &BackupRunService{
		repository:  repository,
		plans:       plans,
		idempotency: idempotency,
		now:         time.Now,
	}
}

// RunBackup publishes one operator/manual backup. An optional fixed revision
// is accepted for callers that already selected a read revision; zero leaves
// revision selection to the repository after replay resolution.
func (service *BackupRunService) RunBackup(
	ctx context.Context,
	environmentID, idempotencyKey string,
	fixedRevision ...int64,
) (etcd.IdempotencyResponse, error) {
	return service.runBackup(
		ctx, environmentID, idempotencyKey, backupRunRoute,
		etcd.BackupRunInitiatorOperator, nil, time.Time{}, fixedRevision...,
	)
}

// RunScheduledBackup publishes one deterministic occurrence in a Controller-
// only route namespace. evaluatedAt is the tick boundary that selected the
// latest occurrence and becomes the atomic schedule progress boundary.
func (service *BackupRunService) RunScheduledBackup(
	ctx context.Context, environmentID string, policyRevision int64,
	scheduledAt, evaluatedAt time.Time, fixedRevision ...int64,
) (etcd.IdempotencyResponse, error) {
	if policyRevision <= 0 || scheduledAt.IsZero() || evaluatedAt.IsZero() ||
		scheduledAt.After(evaluatedAt) {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed, "scheduled backup identity is invalid",
		)
	}
	key := fmt.Sprintf(
		"scheduled:%d:%s", policyRevision,
		scheduledAt.UTC().Format("20060102T150405Z"),
	)
	return service.runBackup(
		ctx, environmentID, key, scheduledBackupRunRoute,
		etcd.BackupRunInitiatorSchedule, &scheduledAt, evaluatedAt, fixedRevision...,
	)
}

func (service *BackupRunService) RetryBackupTask(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	if service == nil || service.repository == nil || service.plans == nil {
		return etcd.IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"backup run service is not configured",
		)
	}
	planID := ids.New(ids.KindPlan)
	prepared, err := service.repository.PrepareBackupRunRetry(ctx, BackupRunRetryPrepareInput{
		SourceTaskID: sourceTaskID,
		TaskID:       retryTaskID,
		CreatedAt:    marker.CreatedAt,
	})
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	defer prepared.Publication.Clear()
	steps := make([]etcd.TaskStepRecord, len(prepared.Run.Sources))
	for index := range steps {
		steps[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	task := etcd.TaskRecord{
		ID:                retryTaskID,
		OperationID:       prepared.SourceTask.OperationID,
		RetryOf:           sourceTaskID,
		IdempotencyKey:    prepared.SourceTask.IdempotencyKey,
		Owner:             prepared.Owner,
		Actor:             etcd.TaskActorOperator,
		Executor:          etcd.TaskExecutorAgent,
		PlanID:            planID,
		Type:              etcd.TaskBackup,
		Target:            prepared.Run.EnvironmentID,
		Steps:             steps,
		TimeoutSeconds:    backupRunTaskTimeoutSeconds,
		Status:            etcd.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         marker.CreatedAt,
		UpdatedAt:         marker.CreatedAt,
	}
	sealed, err := service.plans.BuildBackupRunPlan(BackupRunPlanInput{
		Task:   task,
		Run:    prepared.Run,
		Upload: BackupRunUploadAuthorities(prepared.Run),
	})
	if err != nil {
		return etcd.IdempotencyTransactionResult{}, err
	}
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	return prepared.Publication.Publish(ctx, task, sealed, marker)
}

func (service *BackupRunService) runBackup(
	ctx context.Context,
	environmentID, idempotencyKey, route string,
	initiator etcd.BackupRunInitiator, scheduledAt *time.Time,
	createdAtOverride time.Time, fixedRevision ...int64,
) (etcd.IdempotencyResponse, error) {
	if service == nil || service.repository == nil || service.plans == nil ||
		service.idempotency == nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"backup run service is not configured",
		)
	}
	if environmentID == "" || idempotencyKey == "" {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"environment and idempotency key are required",
		)
	}
	if len(fixedRevision) > 1 {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"at most one fixed revision may be supplied",
		)
	}
	revision := int64(0)
	if len(fixedRevision) == 1 {
		revision = fixedRevision[0]
	}

	locator := etcd.IdempotencyLocator{
		Method: http.MethodPost, Route: route, Key: idempotencyKey,
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: environmentID,
	}
	evidence, err := service.idempotency.Prepare(ctx, environmentID, route)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer evidence.candidate.Destroy()
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		return resolution.Response, nil
	}

	createdAt := service.now().UTC().Truncate(time.Millisecond)
	if !createdAtOverride.IsZero() {
		createdAt = createdAtOverride.UTC().Truncate(time.Second)
	}
	taskID, operationID, planID := ids.New(
		ids.KindTask,
	), ids.New(
		ids.KindOperation,
	), ids.New(
		ids.KindPlan,
	)
	prepared, err := service.repository.PrepareBackupRun(ctx, BackupRunPrepareInput{
		EnvironmentID: environmentID, TaskID: taskID, OperationID: operationID,
		PlanID: planID, FixedRevision: revision, CreatedAt: createdAt,
		Initiator: initiator, ScheduledAt: scheduledAt,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer prepared.Publication.Clear()

	steps := make([]etcd.TaskStepRecord, len(prepared.Run.Sources))
	for index := range steps {
		steps[index] = etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}
	}
	task := etcd.TaskRecord{
		ID:                taskID,
		OperationID:       operationID,
		Owner:             prepared.Owner,
		Actor:             etcd.TaskActorOperator,
		Executor:          etcd.TaskExecutorAgent,
		PlanID:            planID,
		Type:              etcd.TaskBackup,
		Target:            environmentID,
		Steps:             steps,
		TimeoutSeconds:    6 * 60 * 60,
		Status:            etcd.TaskStatusPending,
		NextEventSequence: 1,
		CreatedAt:         createdAt,
		UpdatedAt:         createdAt,
	}
	if initiator == etcd.BackupRunInitiatorSchedule {
		task.Actor = etcd.TaskActorSystem
	}
	sealed, err := service.plans.BuildBackupRunPlan(BackupRunPlanInput{
		Task: task, Run: prepared.Run, Upload: BackupRunUploadAuthorities(prepared.Run),
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	// The sealed digest is task metadata, not a parameter or materialization.
	// The persistence seam validates this exact digest before publication.
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)

	responseBody, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := etcd.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        append([]byte(nil), responseBody...),
	}
	marker, err := service.idempotency.NewMarker(evidence, locator, response, taskID, createdAt)
	clear(responseBody)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, publishErr := prepared.Publication.Publish(ctx, task, sealed, marker)
	if publishErr != nil {
		if !errors.Is(publishErr, errs.New(errs.KindStorageUnavailable, "")) &&
			!errors.Is(publishErr, context.DeadlineExceeded) {
			return etcd.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, publishErr)
		if err != nil {
			return etcd.IdempotencyResponse{}, err
		}
		return resolution.Response, nil
	}
	resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		resolution.Response = response
	}
	return resolution.Response, nil
}
