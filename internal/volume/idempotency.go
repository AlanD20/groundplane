package volume

import (
	"context"
	"errors"

	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type mutationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type mutationIntent struct {
	method        string
	route         string
	environmentID string
	volumeID      string
	query         idempotentintent.Value
	body          idempotentintent.Body
}

type mutationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newMutationIdempotency(
	coordinator *idempotentintent.Coordinator,
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
	path := []idempotentintent.PathBinding(nil)
	if intent.volumeID != "" {
		path = []idempotentintent.PathBinding{{Name: "id", Value: intent.volumeID}}
	}
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: intent.method,
		Route:  intent.route,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: intent.environmentID},
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
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *mutationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence mutationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *mutationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence mutationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *mutationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence mutationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func mutationLocator(intent mutationIntent, idempotencyKey string) etcd.IdempotencyLocator {
	return etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   intent.environmentID,
		Method:    intent.method,
		Route:     intent.route,
		Key:       idempotencyKey,
	}
}

func replayResponse(resolution idempotentintent.Resolution) (etcd.IdempotencyResponse, error) {
	if resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Volume replay resolution is invalid")
	}
	return cloneResponse(resolution.Response), nil
}

func cloneResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
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
