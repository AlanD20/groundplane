package operations

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type entryDesiredRemovalEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type entryDesiredRemovalIdempotency interface {
	Prepare(context.Context, string, string) (entryDesiredRemovalEvidence, error)
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	MatchesStaged(context.Context, entryDesiredRemovalEvidence, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		entryDesiredRemovalEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryDesiredRemovalEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		entryDesiredRemovalEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableEntryDesiredRemovalIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewRemovalIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryDesiredRemovalIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry removal idempotency is not configured")
	}
	return &durableEntryDesiredRemovalIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryDesiredRemovalIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	entryID string,
) (entryDesiredRemovalEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: entryEditRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: entryID}},
		Query: requestidempotency.Object(), Body: requestidempotency.NoBody(),
	})
	if err != nil {
		return entryDesiredRemovalEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryDesiredRemovalEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryDesiredRemovalEvidence{}, err
	}
	return entryDesiredRemovalEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableEntryDesiredRemovalIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryDesiredRemovalEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence entryDesiredRemovalEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryDesiredRemovalEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryDesiredRemovalIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence entryDesiredRemovalEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}
