package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const taskRetryRoute = "/tasks/{id}/retry"

type taskRetryRepository interface {
	GetTaskRetryScope(context.Context, string) (etcd.TaskRetryScope, error)
	RetryTask(context.Context, string, string, etcd.IdempotencyMarker) (etcd.IdempotencyTransactionResult, error)
}

type taskRetryEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type taskRetryIdempotency interface {
	Prepare(context.Context, string, idempotentintent.Scope) (taskRetryEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		taskRetryEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		taskRetryEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		taskRetryEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableTaskRetryIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableTaskRetryIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableTaskRetryIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Task retry idempotency is not configured")
	}
	return &durableTaskRetryIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableTaskRetryIdempotency) Prepare(
	ctx context.Context,
	taskID string,
	scope idempotentintent.Scope,
) (taskRetryEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost, Route: taskRetryRoute, Scope: scope,
		Path:  []idempotentintent.PathBinding{{Name: "id", Value: taskID}},
		Query: idempotentintent.Object(), Body: idempotentintent.NoBody(),
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
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableTaskRetryIdempotency) ResolveKnown(
	ctx context.Context,
	evidence taskRetryEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableTaskRetryIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence taskRetryEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type taskRetryService struct {
	repository  taskRetryRepository
	idempotency taskRetryIdempotency
	now         func() time.Time
}

func newTaskRetryService(
	repository taskRetryRepository,
	idempotency taskRetryIdempotency,
) (*taskRetryService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Task retry service is not configured")
	}
	return &taskRetryService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *taskRetryService) RetryTask(
	ctx context.Context,
	sourceTaskID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task retry context is required")
	}
	if ids.Validate(ids.KindTask, sourceTaskID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindValidationFailed, "Task id is invalid")
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
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task retry replay resolution is invalid")
		}
		return cloneIdempotencyResponse(resolution.Response), nil
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
	result, retryErr := service.repository.RetryTask(ctx, sourceTaskID, retryTaskID, marker)
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
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Task retry resolution is invalid")
	}
}

func taskRetryIntentScope(scope etcd.TaskRetryScope) (idempotentintent.Scope, error) {
	switch scope.Kind {
	case etcd.IdempotencyScopePlatform:
		if scope.ID != "-" {
			return idempotentintent.Scope{}, errs.New(errs.KindInternal, "Task retry platform scope is invalid")
		}
		return idempotentintent.Scope{Kind: idempotentintent.ScopePlatform}, nil
	case etcd.IdempotencyScopeTenant:
		return idempotentintent.Scope{Kind: idempotentintent.ScopeTenant, ID: scope.ID}, nil
	case etcd.IdempotencyScopeProject:
		return idempotentintent.Scope{Kind: idempotentintent.ScopeProject, ID: scope.ID}, nil
	case etcd.IdempotencyScopeEnvironment:
		return idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: scope.ID}, nil
	default:
		return idempotentintent.Scope{}, errs.New(errs.KindInternal, "Task retry owner scope is invalid")
	}
}

func isUnknownTaskRetryOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
