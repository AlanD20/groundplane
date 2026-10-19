package backup

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
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
	idempotency           backupPruneIdempotency
	now                   func() time.Time
	manifestCleanupCursor string
}

type backupPruneIdempotency interface {
	backupRunIdempotency
	PreparePointRemoval(context.Context, string, string) (backupRunEvidence, error)
}

func NewBackupPruneService(runtime *etcd.BackupRuntimeRepository,
	idempotency backupPruneIdempotency,
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
	last, dispatchErr := service.dispatch(ctx, selected)
	if dispatchErr != nil && !errors.Is(dispatchErr, errs.New(errs.KindStateConflict, "")) {
		return startExclusive, false, dispatchErr
	}
	if last == "" {
		last = selected[len(selected)-1].Record.Point.ID
	}
	continuation := backupruntime.BackupRecoveryPointPruneKey(last)
	if page.Next == "" && continuation == backupruntime.BackupRecoveryPointPruneKey(
		page.Pending[len(page.Pending)-1].Record.Point.ID,
	) {
		continuation = ""
	}
	if dispatchErr != nil {
		return continuation, false, nil
	}
	return continuation, true, nil
}

func (service *BackupPruneService) dispatch(ctx context.Context,
	selected []etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord],
) (string, error) {
	first := selected[0].Record
	taskID := ids.New(ids.KindTask)
	createdAt := service.now().UTC().Truncate(time.Millisecond)
	for _, item := range selected {
		if !createdAt.After(item.Record.UpdatedAt) {
			createdAt = item.Record.UpdatedAt.Add(time.Millisecond)
		}
	}
	key := "prune:" + taskID
	locator := idempotencyrecord.IdempotencyLocator{
		Method: http.MethodPost, Route: backupPruneRoute, Key: key,
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: first.Point.EnvironmentID,
	}
	evidence, err := service.idempotency.Prepare(ctx, first.Point.EnvironmentID, backupPruneRoute)
	if err != nil {
		return "", err
	}
	defer evidence.candidate.Destroy()
	if _, exists, err := service.idempotency.ResolveExisting(ctx, locator, evidence); err != nil {
		return "", err
	} else if exists {
		return selected[len(selected)-1].Record.Point.ID, nil
	}
	// The native execution/replay envelope is part of the batch budget. A
	// rejected preflight performs no writes; remaining Points stay pending and
	// the cursor advances only through the batch actually accepted.
	for len(selected) > 0 {
		prepared, err := service.runtime.PrepareBackupPrune(ctx, selected, taskID, createdAt)
		if err != nil {
			return "", err
		}
		_, fits, err := service.publishFittingPrepared(ctx, prepared, locator, evidence, taskjournal.TaskActorSystem)
		prepared.Publication.Clear()
		if err != nil {
			return "", err
		}
		if fits {
			return selected[len(selected)-1].Record.Point.ID, nil
		}
		selected = selected[:len(selected)-1]
	}
	return "", errs.New(errs.KindInternal, "one Recovery Point exceeds the atomic prune publication limit")
}

func (service *BackupPruneService) publishPrepared(ctx context.Context,
	prepared etcd.PreparedBackupPrune, locator idempotencyrecord.IdempotencyLocator,
	evidence backupRunEvidence, actor taskjournal.TaskActor,
) (idempotencyrecord.IdempotencyResponse, error) {
	response, fits, err := service.publishFittingPrepared(ctx, prepared, locator, evidence, actor)
	if err == nil && !fits {
		err = errs.New(errs.KindInternal, "one Recovery Point exceeds the atomic prune publication limit")
	}
	return response, err
}

func (service *BackupPruneService) publishFittingPrepared(ctx context.Context,
	prepared etcd.PreparedBackupPrune, locator idempotencyrecord.IdempotencyLocator,
	evidence backupRunEvidence, actor taskjournal.TaskActor,
) (idempotencyrecord.IdempotencyResponse, bool, error) {
	zero := idempotencyrecord.IdempotencyResponse{}
	dispatch := prepared.Dispatch
	taskID, createdAt := dispatch.TaskID, dispatch.CreatedAt
	if actor != taskjournal.TaskActorSystem {
		return zero, false, errs.New(errs.KindValidationFailed, "Prune requires its system execution actor")
	}
	task, item, captured, err := buildPruneWork(prepared)
	if err != nil {
		return zero, false, err
	}
	body, err := json.Marshal(struct {
		TaskID string `json:"task_id"`
	}{TaskID: taskID})
	if err != nil {
		return zero, false, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker, err := service.idempotency.NewMarker(evidence, locator, response, taskID, createdAt)
	clear(body)
	if err != nil {
		return zero, false, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	fits, err := prepared.Publication.FitsWork(ctx, task, item, captured, marker)
	if err != nil || !fits {
		return zero, false, err
	}
	result, publishErr := prepared.Publication.PublishWork(ctx, task, item, captured, marker)
	if publishErr != nil {
		if !errors.Is(publishErr, errs.New(errs.KindStorageUnavailable, "")) &&
			!errors.Is(publishErr, context.DeadlineExceeded) {
			return zero, false, publishErr
		}
		resolved, err := service.idempotency.ResolveUnknown(ctx, locator, evidence, publishErr)
		return resolved.Response, true, err
	}
	resolved, err := service.idempotency.ResolveKnown(ctx, evidence, result)
	if err == nil && resolved.Kind == requestidempotency.ResolutionApplied {
		resolved.Response = response
	}
	return resolved.Response, true, err
}
