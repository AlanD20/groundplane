package tasks

import (
	"context"
	"encoding/json"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskRetryRoute = "/tasks/{id}/retry"

type taskRetryRepository interface {
	GetTask(context.Context, string) (etcdstore.Versioned[etcd.TaskRecord], error)
	GetTaskRetryScope(context.Context, string) (etcd.TaskRetryScope, error)
	RetryTask(
		context.Context,
		string,
		string,
		etcd.TaskActor,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
	RetryTaskWithInitiation(
		context.Context,
		string,
		string,
		etcd.TaskInitiation,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type backupTaskRetryer interface {
	RetryBackupTask(
		context.Context,
		string,
		string,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type taskRetryEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type taskRetryIdempotency interface {
	Prepare(context.Context, string, requestidempotency.Scope) (taskRetryEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		taskRetryEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		taskRetryEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		taskRetryEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableTaskRetryIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewRetryIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableTaskRetryIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "task retry idempotency is not configured")
	}
	return &durableTaskRetryIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableTaskRetryIdempotency) Prepare(
	ctx context.Context,
	taskID string,
	scope requestidempotency.Scope,
) (taskRetryEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost, Route: taskRetryRoute, Scope: scope,
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: taskID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
	})
	if err != nil {
		return taskRetryEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return taskRetryEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return taskRetryEvidence{}, err
	}
	return taskRetryEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableTaskRetryIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence taskRetryEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableTaskRetryIdempotency) ResolveKnown(
	ctx context.Context,
	evidence taskRetryEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableTaskRetryIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence taskRetryEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type taskRetryService struct {
	repository  taskRetryRepository
	idempotency taskRetryIdempotency
	backups     backupTaskRetryer
	now         func() time.Time
}

func NewRetryService(
	repository taskRetryRepository,
	idempotency taskRetryIdempotency,
	backups backupTaskRetryer,
) (*taskRetryService, error) {
	if repository == nil || idempotency == nil || backups == nil {
		return nil, errs.New(errs.KindInternal, "task retry service is not configured")
	}
	return &taskRetryService{
		repository:  repository,
		idempotency: idempotency,
		backups:     backups,
		now:         time.Now,
	}, nil
}

func (service *taskRetryService) RetryTask(
	ctx context.Context,
	sourceTaskID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.retryTask(ctx, sourceTaskID, idempotencyKey, nil)
}

func (service *taskRetryService) RetryTaskWithInitiation(
	ctx context.Context,
	sourceTaskID string,
	idempotencyKey string,
	initiation etcd.TaskInitiation,
) (etcd.IdempotencyResponse, error) {
	return service.retryTask(ctx, sourceTaskID, idempotencyKey, &initiation)
}

func (service *taskRetryService) retryTask(
	ctx context.Context,
	sourceTaskID string,
	idempotencyKey string,
	initiation *etcd.TaskInitiation,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "task retry context is required")
	}
	if ids.Validate(ids.KindTask, sourceTaskID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "task id is invalid")
	}
	source, err := service.repository.GetTask(ctx, sourceTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if source.Record.Params[etcd.TaskResourceKindParam] == etcd.TaskResourceController {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindTaskNotRetryable, "Controller update requires a fresh explicit release selection after recovery",
		)
	}
	if source.Record.Type == etcd.TaskBackupPrune {
		return etcd.IdempotencyResponse{}, errs.Newf(
			errs.KindTaskNotRetryable,
			"internal task %s of type %s is not operator-retryable",
			sourceTaskID,
			source.Record.Type,
		)
	}
	durableScope, err := service.repository.GetTaskRetryScope(ctx, sourceTaskID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	intentScope, err := taskRetryIntentScope(durableScope)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, sourceTaskID, intentScope)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: durableScope.Kind, ScopeID: durableScope.ID,
		Method: http.MethodPost, Route: taskRetryRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "task retry replay resolution is invalid")
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	now := service.now().UTC()
	retryTaskID := ids.New(ids.KindTask)
	responseBody, err := json.Marshal(apiTypes.TaskAccepted{TaskID: retryTaskID})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker := etcd.IdempotencyMarker{
		Kind: etcd.IdempotencyMarkerTask, State: etcd.IdempotencyMarkerPending,
		Locator: locator, Intent: evidence.durable, Response: response,
		TaskID: retryTaskID, CreatedAt: now, UpdatedAt: now,
	}
	var result etcd.IdempotencyTransactionResult
	var retryErr error
	if source.Record.Type == etcd.TaskBackup {
		if initiation != nil {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindTaskNotRetryable,
				"backup Tasks do not accept system retry initiation",
			)
		}
		result, retryErr = service.backups.RetryBackupTask(
			ctx, sourceTaskID, retryTaskID, marker,
		)
	} else if initiation == nil {
		result, retryErr = service.repository.RetryTask(
			ctx, sourceTaskID, retryTaskID, etcd.TaskActorOperator, marker,
		)
	} else {
		result, retryErr = service.repository.RetryTaskWithInitiation(
			ctx, sourceTaskID, retryTaskID, *initiation, marker,
		)
	}
	if retryErr != nil {
		if !isUnknownTaskRetryOutcome(retryErr) {
			return etcd.IdempotencyResponse{}, retryErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, retryErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "task retry resolution is invalid")
	}
}

func taskRetryIntentScope(scope etcd.TaskRetryScope) (requestidempotency.Scope, error) {
	switch scope.Kind {
	case etcd.IdempotencyScopePlatform:
		if scope.ID != "-" {
			return requestidempotency.Scope{}, errs.New(errs.KindInternal, "task retry platform scope is invalid")
		}
		return requestidempotency.Scope{Kind: requestidempotency.ScopePlatform}, nil
	case etcd.IdempotencyScopeTenant:
		return requestidempotency.Scope{Kind: requestidempotency.ScopeTenant, ID: scope.ID}, nil
	case etcd.IdempotencyScopeProject:
		return requestidempotency.Scope{Kind: requestidempotency.ScopeProject, ID: scope.ID}, nil
	case etcd.IdempotencyScopeEnvironment:
		return requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: scope.ID}, nil
	default:
		return requestidempotency.Scope{}, errs.New(errs.KindInternal, "task retry owner scope is invalid")
	}
}

func isUnknownTaskRetryOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
