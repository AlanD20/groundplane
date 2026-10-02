package backup

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backupPruneRoute = "/system/backup-retention/{id}/prune"

// BackupPruneService drains retained, operation-owned tombstones from bounded
// pages. Each tick publishes at most one Task and leaves a continuation for the
// scheduler. A failed or timed-out Task returns only surviving tombstones to
// pending under the original operation id, so a later tick retries them.
type BackupPruneService struct {
	runtime               *etcd.BackupRuntimeRepository
	idempotency           backupRunIdempotency
	now                   func() time.Time
	manifestCleanupCursor string
}

func NewBackupPruneService(runtime *etcd.BackupRuntimeRepository,
	idempotency backupRunIdempotency,
) (*BackupPruneService, error) {
	if runtime == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "backup prune service dependencies are required")
	}
	return &BackupPruneService{runtime: runtime, idempotency: idempotency, now: time.Now}, nil
}

// Tick accepts the prior continuation (empty for the first page). Its returned
// continuation must be retained by the Controller scheduler; an empty value
// wraps the following tick to the first page. A held Restore or other active
// Environment operation is skipped by the atomic lock admission.
func (service *BackupPruneService) Tick(ctx context.Context,
	startExclusive string,
) (string, bool, error) {
	if service == nil || service.runtime == nil || service.idempotency == nil {
		return startExclusive, false, errs.New(errs.KindInternal, "backup prune service is not configured")
	}
	cleanupCursor, err := service.runtime.PruneVolumeManifestBatch(ctx, service.manifestCleanupCursor)
	if err != nil {
		return startExclusive, false, err
	}
	service.manifestCleanupCursor = cleanupCursor
	page, err := service.runtime.ListPendingBackupPruneCandidates(ctx, startExclusive)
	if err != nil {
		return startExclusive, false, err
	}
	if len(page.Pending) == 0 {
		return page.Next, false, nil
	}
	first := page.Pending[0]
	selected := make([]etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord], 0,
		backupruntime.MaximumBackupPruneDispatchPoints)
	selected = append(selected, first)
	for _, candidate := range page.Pending[1:] {
		if len(selected) == backupruntime.MaximumBackupPruneDispatchPoints {
			break
		}
		record := candidate.Record
		if record.OperationID == first.Record.OperationID &&
			record.Point.EnvironmentID == first.Record.Point.EnvironmentID &&
			record.PolicyRevision == first.Record.PolicyRevision &&
			record.PolicySHA256 == first.Record.PolicySHA256 {
			selected = append(selected, candidate)
		}
	}
	continuation := backupruntime.BackupRecoveryPointPruneKey(selected[len(selected)-1].Record.Point.ID)
	if page.Next == "" && continuation == backupruntime.BackupRecoveryPointPruneKey(
		page.Pending[len(page.Pending)-1].Record.Point.ID,
	) {
		continuation = ""
	}
	if err := service.dispatch(ctx, selected); err != nil {
		if errors.Is(err, errs.New(errs.KindStateConflict, "")) {
			return continuation, false, nil
		}
		return startExclusive, false, err
	}
	return continuation, true, nil
}

func (service *BackupPruneService) dispatch(ctx context.Context,
	selected []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) error {
	first := selected[0].Record
	taskID, planID := ids.New(ids.KindTask), ids.New(ids.KindPlan)
	createdAt := service.now().UTC().Truncate(time.Millisecond)
	for _, item := range selected {
		if !createdAt.After(item.Record.UpdatedAt) {
			createdAt = item.Record.UpdatedAt.Add(time.Millisecond)
		}
	}
	prepared, err := service.runtime.PrepareBackupPrune(ctx, selected, taskID, createdAt)
	if err != nil {
		return err
	}
	defer prepared.Publication.Clear()
	steps := make([]taskjournal.TaskStepRecord, len(selected))
	executionIDs := make([]string, len(selected))
	for index := range steps {
		steps[index] = taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: ids.New(ids.KindStep)}
		executionIDs[index] = ids.NewULID()
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: first.OperationID, Owner: prepared.Owner,
		Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		PlanID: planID, Type: taskjournal.TaskBackupPrune, Target: first.Point.EnvironmentID,
		Steps: steps, TimeoutSeconds: int64(executionplan.MaximumBackupPruneStepTimeoutSeconds),
		Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	sealed, err := BuildBackupPrunePlan(BackupPrunePlanInput{
		Task: task, Scope: prepared.Scope, Dispatch: prepared.Dispatch,
		Evidence: prepared.Evidence, ExecutionIDs: executionIDs,
	})
	if err != nil {
		return err
	}
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	key := "prune:" + taskID
	locator := idempotencyrecord.IdempotencyLocator{
		Method: http.MethodPost, Route: backupPruneRoute, Key: key,
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: first.Point.EnvironmentID,
	}
	evidence, err := service.idempotency.Prepare(ctx, first.Point.EnvironmentID, backupPruneRoute)
	if err != nil {
		return err
	}
	defer evidence.candidate.Destroy()
	if _, exists, err := service.idempotency.ResolveExisting(ctx, locator, evidence); err != nil {
		return err
	} else if exists {
		return nil
	}
	body, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	if err != nil {
		return errs.Wrap(errs.KindInternal, err)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: body,
	}
	marker, err := service.idempotency.NewMarker(evidence, locator, response, taskID, createdAt)
	clear(body)
	if err != nil {
		return err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, publishErr := prepared.Publication.Publish(ctx, task, sealed, marker)
	if publishErr != nil {
		if !errors.Is(publishErr, errs.New(errs.KindStorageUnavailable, "")) &&
			!errors.Is(publishErr, context.DeadlineExceeded) {
			return publishErr
		}
		_, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, publishErr)
		return err
	}
	_, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	return err
}
