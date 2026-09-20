package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"net/http"
)

type entryBulkUpsertEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type entryBulkUpsertIdempotency interface {
	Prepare(context.Context, entryBulkUpsertInput) (entryBulkUpsertEvidence, error)
	MatchesStaged(context.Context, entryBulkUpsertEvidence, etcd.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		entryBulkUpsertEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryBulkUpsertEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		entryBulkUpsertEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableEntryBulkUpsertIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewBulkUpsertIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryBulkUpsertIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry bulk upsert idempotency is not configured")
	}
	return &durableEntryBulkUpsertIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryBulkUpsertIdempotency) Prepare(
	ctx context.Context,
	input entryBulkUpsertInput,
) (entryBulkUpsertEvidence, error) {
	items := make([]requestidempotency.Value, len(input.entries))
	for index, entry := range input.entries {
		digest := sha256.Sum256([]byte(entry.value))
		items[index] = requestidempotency.Object(
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(entry.key)},
			requestidempotency.Field{Name: "value_sha256", Value: requestidempotency.String(hex.EncodeToString(digest[:]))},
		)
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  entryBulkUpsertRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: input.environmentID},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(input.environmentID)},
			requestidempotency.Field{Name: "entries", Value: requestidempotency.List(items...)},
			requestidempotency.Field{Name: "exposure", Value: canonicalEntryExposure(input.exposure)},
			requestidempotency.Field{Name: "secret", Value: requestidempotency.Bool(input.secret)},
		)),
	})
	if err != nil {
		return entryBulkUpsertEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryBulkUpsertEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryBulkUpsertEvidence{}, err
	}
	return entryBulkUpsertEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryBulkUpsertIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryBulkUpsertEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *durableEntryBulkUpsertIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryBulkUpsertEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryBulkUpsertIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryBulkUpsertEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryBulkUpsertIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryBulkUpsertEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}
