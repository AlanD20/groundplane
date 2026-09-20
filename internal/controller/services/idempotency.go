package services

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type serviceMutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type serviceMutationIntent struct {
	method        string
	route         string
	environmentID string
	serviceID     string
	body          requestidempotency.Value
}

type serviceMutationIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	Prepare(context.Context, serviceMutationIntent) (serviceMutationEvidence, error)
	MatchesStaged(context.Context, serviceMutationEvidence, etcd.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		serviceMutationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		serviceMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		serviceMutationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableServiceMutationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func (service *durableServiceMutationIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableServiceMutationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence serviceMutationEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func NewMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableServiceMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Service mutation idempotency is not configured")
	}
	return &durableServiceMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableServiceMutationIdempotency) Prepare(
	ctx context.Context,
	intent serviceMutationIntent,
) (serviceMutationEvidence, error) {
	path := []requestidempotency.PathBinding(nil)
	if intent.serviceID != "" {
		path = []requestidempotency.PathBinding{{Name: "id", Value: intent.serviceID}}
	}
	body := requestidempotency.JSONBody(intent.body)
	if intent.method == http.MethodDelete {
		body = requestidempotency.NoBody()
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: intent.method, Route: intent.route,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: intent.environmentID},
		Path:  path, Query: requestidempotency.Object(), Body: body,
	})
	if err != nil {
		return serviceMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return serviceMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return serviceMutationEvidence{}, err
	}
	return serviceMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableServiceMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableServiceMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence serviceMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableServiceMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence serviceMutationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}
