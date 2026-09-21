package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type entryCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type entryCreationIdempotency interface {
	Prepare(context.Context, string, core.EnvEntry) (entryCreationEvidence, error)
	MatchesStaged(context.Context, entryCreationEvidence, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		entryCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		entryCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

type durableEntryCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry creation idempotency is not configured")
	}
	return &durableEntryCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryCreationIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	entry core.EnvEntry,
) (entryCreationEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  entryCreationRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(environmentID)},
			requestidempotency.Field{Name: "exposure", Value: canonicalEntryExposure(entry.Exposure)},
			requestidempotency.Field{Name: "gid", Value: canonicalEntryNumericID(entry.GID)},
			requestidempotency.Field{Name: "key", Value: requestidempotency.String(entry.Key)},
			requestidempotency.Field{Name: "path", Value: requestidempotency.String(entry.Path)},
			requestidempotency.Field{Name: "secret", Value: requestidempotency.Bool(entry.Secret)},
			requestidempotency.Field{Name: "source", Value: canonicalEntrySource(entry)},
			requestidempotency.Field{Name: "type", Value: requestidempotency.String(string(entry.Kind))},
			requestidempotency.Field{Name: "uid", Value: canonicalEntryNumericID(entry.UID)},
		)),
	})
	if err != nil {
		return entryCreationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryCreationEvidence{}, err
	}
	return entryCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryCreationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryCreationEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

func (service *durableEntryCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence entryCreationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence entryCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func canonicalEntryNumericID(value *uint32) requestidempotency.Value {
	if value == nil {
		return requestidempotency.Null()
	}
	return requestidempotency.UnsignedInteger(uint64(*value))
}

func canonicalEntryExposure(exposure []string) requestidempotency.Value {
	values := make([]requestidempotency.Value, len(exposure))
	for index, value := range exposure {
		values[index] = requestidempotency.String(value)
	}
	return requestidempotency.List(values...)
}

func canonicalEntrySource(entry core.EnvEntry) requestidempotency.Value {
	source := entry.Source
	switch source.Kind {
	case core.SourceLiteral:
		if entry.Secret {
			digest := sha256.Sum256([]byte(source.Literal))
			return requestidempotency.Object(
				requestidempotency.Field{Name: "kind", Value: requestidempotency.String(string(source.Kind))},
				requestidempotency.Field{
					Name:  "literal_sha256",
					Value: requestidempotency.String(hex.EncodeToString(digest[:])),
				},
			)
		}
		return requestidempotency.Object(
			requestidempotency.Field{Name: "kind", Value: requestidempotency.String(string(source.Kind))},
			requestidempotency.Field{Name: "literal", Value: requestidempotency.String(source.Literal)},
		)
	case core.SourceSecretRef:
		return requestidempotency.Object(
			requestidempotency.Field{Name: "kind", Value: requestidempotency.String(string(source.Kind))},
			requestidempotency.Field{Name: "secret_ref", Value: requestidempotency.String(source.SecretRef)},
		)
	case core.SourceFact:
		return requestidempotency.Object(
			requestidempotency.Field{Name: "attach_id", Value: requestidempotency.String(source.Fact.Attach)},
			requestidempotency.Field{Name: "fact", Value: requestidempotency.String(source.Fact.Key)},
			requestidempotency.Field{Name: "grant_attach_id", Value: requestidempotency.String(source.Fact.Grant)},
			requestidempotency.Field{Name: "kind", Value: requestidempotency.String(string(source.Kind))},
		)
	default:
		return requestidempotency.Object()
	}
}
