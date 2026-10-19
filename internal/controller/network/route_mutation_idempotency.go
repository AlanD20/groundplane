package network

import (
	"context"
	"errors"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
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
	MatchesStaged(context.Context, routeMutationEvidence, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		routeMutationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		routeMutationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		routeMutationEvidence,
		error,
	) (requestidempotency.Resolution, error)
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
	if intent.method == http.MethodDelete {
		body = requestidempotency.NoBody()
	}
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

func (service *durableRouteMutationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence routeMutationEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *durableRouteMutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence routeMutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableRouteMutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (service *routeMutationService) resolveRouteMutation(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
	result etcd.IdempotencyTransactionResult,
	mutationErr error,
	response idempotencyrecord.IdempotencyResponse,
) (idempotencyrecord.IdempotencyResponse, error) {
	var resolution requestidempotency.Resolution
	var err error
	if mutationErr != nil {
		if !isUnknownRouteMutationOutcome(mutationErr) {
			clear(response.Body)
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return response, nil
	case requestidempotency.ResolutionReplay:
		clear(response.Body)
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Route mutation resolution is invalid",
		)
	}
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

func isUnknownRouteMutationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
