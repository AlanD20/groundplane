package services

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type serviceLifecycleEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type serviceLifecycleIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(
		context.Context,
		etcd.IdempotencyLocator,
		string,
		etcd.TaskType,
		string,
	) (serviceLifecycleEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		serviceLifecycleEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		serviceLifecycleEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		serviceLifecycleEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableServiceLifecycleIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewLifecycleIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableServiceLifecycleIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle idempotency is not configured")
	}
	return &durableServiceLifecycleIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableServiceLifecycleIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableServiceLifecycleIdempotency) Prepare(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	serviceID string,
	taskType etcd.TaskType,
	route string,
) (serviceLifecycleEvidence, error) {
	if locator.ScopeKind != etcd.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return serviceLifecycleEvidence{}, errs.New(errs.KindInternal, "Service lifecycle replay scope is invalid")
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  route,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: serviceID}},
		Query:  requestidempotency.Object(),
		Body:   requestidempotency.NoBody(),
	})
	if err != nil {
		return serviceLifecycleEvidence{}, err
	}
	defer digest.Destroy()
	if taskType != etcd.TaskStart && taskType != etcd.TaskStop && taskType != etcd.TaskDestroy {
		return serviceLifecycleEvidence{}, errs.New(errs.KindInternal, "Service lifecycle intent type is invalid")
	}
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return serviceLifecycleEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return serviceLifecycleEvidence{}, err
	}
	return serviceLifecycleEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableServiceLifecycleIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceLifecycleEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableServiceLifecycleIdempotency) ResolveKnown(
	ctx context.Context,
	evidence serviceLifecycleEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableServiceLifecycleIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceLifecycleEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}
