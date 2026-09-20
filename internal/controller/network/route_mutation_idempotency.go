package network

import (
	"context"
	"encoding/json"
	"errors"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
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
	Prepare(context.Context, routeMutationIntent) (routeMutationEvidence, error)
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
	repository  requestidempotency.EvidenceRepository
}

func newDurableRouteMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository requestidempotency.EvidenceRepository,
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
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope: requestidempotency.Scope{
			Kind: requestidempotency.ScopeEnvironment,
			ID:   intent.environmentID,
		},
		Path:  intent.path,
		Query: requestidempotency.Object(),
		Body:  requestidempotency.JSONBody(intent.body),
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

func (service *routeMutationService) routeResponseMarker(
	locator idempotencyrecord.IdempotencyLocator,
	evidence routeMutationEvidence,
	record routerecord.Record,
	taskID string,
) (idempotencyrecord.IdempotencyResponse, idempotencyrecord.IdempotencyMarker, error) {
	body, err := json.Marshal(apiTypes.RouteTaskAccepted{Route: routeAPIResponse(record), TaskID: taskID})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(body)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusAccepted, ContentKind: "application/json", Body: append([]byte(nil), body...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		clear(response.Body)
		return idempotencyrecord.IdempotencyResponse{}, idempotencyrecord.IdempotencyMarker{}, err
	}
	return response, marker, nil
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route mutation resolution is invalid")
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
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route replay resolution is invalid")
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
