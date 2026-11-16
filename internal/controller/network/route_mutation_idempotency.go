package network

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type routeMutationIntent struct {
	method        string
	route         string
	environmentID string
	path          []requestidempotency.PathBinding
	body          requestidempotency.Value
}

type routeMutationIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	Prepare(context.Context, routeMutationIntent) (routeMutationEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		routeMutationEvidence,
	) (requestidempotency.Resolution, bool, error)
}

type durableRouteMutationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  idempotencyEvidenceRepository
}

func newDurableRouteMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository idempotencyEvidenceRepository,
) (*durableRouteMutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation idempotency is not configured")
	}
	return &durableRouteMutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableRouteMutationIdempotency) Prepare(
	ctx context.Context,
	intent routeMutationIntent,
) (routeMutationEvidence, error) {
	body := requestidempotency.JSONBody(intent.body)

	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope: requestidempotency.Scope{
			Kind: requestidempotency.ScopeEnvironment,
			ID:   intent.environmentID,
		},
		Path:  intent.path,
		Query: requestidempotency.Object(),
		Body:  body,
	})
	if err != nil {
		return routeMutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return routeMutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return routeMutationEvidence{}, err
	}
	return routeMutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableRouteMutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableRouteMutationIdempotency) ResolveReplayLocator(ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget, method, route, key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func routeMutationLocator(intent routeMutationIntent, idempotencyKey string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment, ScopeID: intent.environmentID,
		Method: intent.method, Route: intent.route, Key: idempotencyKey,
	}
}

func routeReplayResponse(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Route replay resolution is invalid",
		)
	}
	return cloneIdempotencyResponse(resolution.Response), nil
}
