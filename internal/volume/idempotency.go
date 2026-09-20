package volume

import (
	"context"
	"errors"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type mutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type mutationIntent struct {
	method        string
	route         string
	environmentID string
	volumeID      string
	query         requestidempotency.Value
	body          requestidempotency.Body
}

type mutationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newMutationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*mutationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Volume mutation idempotency is not configured")
	}
	return &mutationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *mutationIdempotency) Prepare(
	ctx context.Context,
	intent mutationIntent,
) (mutationEvidence, error) {
	path := []requestidempotency.PathBinding(nil)
	if intent.volumeID != "" {
		path = []requestidempotency.PathBinding{{Name: "id", Value: intent.volumeID}}
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: intent.environmentID},
		Path:   path,
		Query:  intent.query,
		Body:   intent.body,
	})
	if err != nil {
		return mutationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return mutationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return mutationEvidence{}, err
	}
	return mutationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *mutationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence mutationEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *mutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence mutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *mutationIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *mutationIdempotency) ResolveOperationRootExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence mutationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveOperationRootExisting(
		ctx, service.repository, locator, evidence.candidate,
	)
}

func (service *mutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence mutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *mutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence mutationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func mutationLocator(intent mutationIntent, idempotencyKey string) idempotencyrecord.IdempotencyLocator {
	return idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   intent.environmentID,
		Method:    intent.method,
		Route:     intent.route,
		Key:       idempotencyKey,
	}
}

func replayResponse(resolution requestidempotency.Resolution) (idempotencyrecord.IdempotencyResponse, error) {
	if resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume replay resolution is invalid")
	}
	return cloneResponse(resolution.Response), nil
}

func cloneResponse(response idempotencyrecord.IdempotencyResponse) idempotencyrecord.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}

func isUnknownPublicationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
