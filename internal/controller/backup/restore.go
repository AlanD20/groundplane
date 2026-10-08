package backup

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const restoreRoute = "/environments/{id}/restore"

// RestoreService selects an immutable target before publishing one
// native Restore and Task atomically. Identity bytes never enter that write.
type RestoreService struct {
	versionObserver DatabaseVersionObserver
	runtime         *etcd.BackupRuntimeRepository
	coordinator     *requestidempotency.Coordinator
	idempotency     *etcd.IdempotencyRepository
	secrets         *BackupSecretResolver
	volumeRoot      string
	resolveDatabase backupplanning.BackupDatabaseIdentityResolver
	serviceFacts    backupplanning.BackupServiceFactResolver
}

func NewRestoreService(runtime *etcd.BackupRuntimeRepository,
	coordinator *requestidempotency.Coordinator, idempotencyRepository *etcd.IdempotencyRepository,
	secrets *BackupSecretResolver, resolveDatabase backupplanning.BackupDatabaseIdentityResolver,
	serviceFacts backupplanning.BackupServiceFactResolver, volumeRoot string,
	versionObserver DatabaseVersionObserver,
) (*RestoreService, error) {
	if runtime == nil || coordinator == nil || idempotencyRepository == nil || secrets == nil ||
		resolveDatabase == nil || serviceFacts == nil || versionObserver == nil ||
		!filepath.IsAbs(volumeRoot) || filepath.Clean(volumeRoot) != volumeRoot {
		return nil, errs.New(errs.KindInternal, "Restore dependencies are required")
	}
	return &RestoreService{runtime: runtime, coordinator: coordinator, idempotency: idempotencyRepository,
		secrets: secrets, resolveDatabase: resolveDatabase, serviceFacts: serviceFacts, volumeRoot: volumeRoot, versionObserver: versionObserver}, nil
}

func (service *RestoreService) Restore(ctx context.Context, environmentID, idempotencyKey string,
	request apiTypes.RestoreRequest,
) (idempotency.IdempotencyResponse, error) {
	if service == nil || ctx == nil {
		return idempotency.IdempotencyResponse{}, errs.New(errs.KindInternal, "Restore is not configured")
	}
	identity, candidate, err := service.prepareRestoreIntent(ctx, environmentID, request)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(identity)
	defer candidate.Destroy()
	locator := idempotency.IdempotencyLocator{ScopeKind: idempotency.IdempotencyScopeEnvironment,
		ScopeID: environmentID, Method: http.MethodPost, Route: restoreRoute, Key: idempotencyKey}
	resolution, existing, err := service.coordinator.ResolveExisting(ctx, service.idempotency, locator, candidate)
	if err != nil || existing {
		return resolution.Response, err
	}
	createdAt := time.Now().UTC().Truncate(time.Millisecond)
	taskID := ids.New(ids.KindTask)
	prepared, err := service.prepareRestore(
		ctx,
		environmentID,
		taskID,
		ids.New(ids.KindOperation),
		request,
		createdAt,
		len(identity) != 0,
		false,
	)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	defer prepared.Publication.Clear()
	task := etcd.TaskRecord{ID: taskID, OperationID: prepared.Restore.OperationID, Owner: prepared.Owner,
		Actor: taskjournal.TaskActorOperator, Executor: taskjournal.TaskExecutorAgent,
		PlanID: ids.New(ids.KindPlan), Type: taskjournal.TaskRestore, Target: environmentID,
		Steps: []taskjournal.TaskStepRecord{
			{ID: prepared.Authority.StepId, Kind: taskjournal.TaskStepOperation},
		},
		TimeoutSeconds: backupRunTaskTimeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt}
	sealed, err := prepared.buildPlan(task)
	if err != nil {
		return idempotency.IdempotencyResponse{}, err
	}
	task.PlanHash = hex.EncodeToString(sealed.PlanHash)
	task.Steps = taskjournal.CaptureStepDescriptions(task.Steps, sealed.Steps)
	body, err := json.Marshal(apiTypes.TaskAccepted{TaskID: taskID})
	if err != nil {
		return idempotency.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	response := idempotency.IdempotencyResponse{
		Status:      http.StatusAccepted,
		ContentKind: "application/json",
		Body:        body,
	}
	marker, err := newBackupTaskMarker(candidate, locator, response, taskID, createdAt)
	if err != nil {
		clear(body)
		return idempotency.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	keepIdentity := false
	if len(identity) != 0 {
		if err := service.secrets.RetainRestoreIdentity(ctx, prepared.Restore, identity); err != nil {
			clear(body)
			return idempotency.IdempotencyResponse{}, err
		}
		defer func() {
			if !keepIdentity {
				// Release has no I/O and must clear the value after definite
				// rejection even if the HTTP request was cancelled meanwhile.
				service.secrets.identities.release(taskID)
			}
		}()
	}
	result, publishErr := prepared.Publication.Publish(ctx, task, sealed, marker)
	if publishErr != nil {
		clear(body)
		if !restorePublicationUnknown(publishErr) {
			return idempotency.IdempotencyResponse{}, publishErr
		}
		// Missing evidence is not proof that the Task did not commit. Keep
		// the volatile value available to that exact Task until its deadline.
		keepIdentity = true
		if errors.Is(publishErr, context.Canceled) {
			return idempotency.IdempotencyResponse{}, publishErr
		}
		resolution, err = service.coordinator.ResolveUnknown(ctx, service.idempotency, locator, candidate, publishErr)
		return resolution.Response, err
	}
	outcome, _, _, classifyErr := result.Classify()
	if classifyErr != nil {
		clear(body)
		keepIdentity = true
		return idempotency.IdempotencyResponse{}, classifyErr
	}
	keepIdentity = outcome == etcd.IdempotencyKnownApplied
	resolution, err = service.coordinator.ResolveKnown(ctx, candidate, result)
	if err != nil {
		clear(body)
		return idempotency.IdempotencyResponse{}, err
	}
	if resolution.Kind == requestidempotency.ResolutionApplied {
		return response, nil
	}
	clear(body)
	return resolution.Response, nil
}

func restorePublicationUnknown(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, errs.New(errs.KindStorageUnavailable, ""))
}
