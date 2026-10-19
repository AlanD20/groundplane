package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	entryEditRoute           = "/entries/{id}"
	maximumEntryEditAttempts = 3
)

type entryEditInput struct {
	Source   core.EntrySource
	Exposure []string
}

type entryEditEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type entryEditIdempotency interface {
	Prepare(context.Context, string, string, entryEditInput) (entryEditEvidence, error)
	MatchesStaged(context.Context, entryEditEvidence, idempotencyrecord.ProtectedIntentRecord) (bool, error)
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		entryEditEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryEditEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		entryEditEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

func (service *durableEntryEditIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryEditEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

type durableEntryEditIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  *etcd.IdempotencyRepository
}

func NewEditIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableEntryEditIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry edit idempotency is not configured")
	}
	return &durableEntryEditIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableEntryEditIdempotency) Prepare(
	ctx context.Context,
	environmentID string,
	entryID string,
	input entryEditInput,
) (entryEditEvidence, error) {
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPatch,
		Route:  entryEditRoute,
		Scope:  requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: environmentID},
		Path:   []requestidempotency.PathBinding{{Name: "id", Value: entryID}},
		Query:  requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "exposure", Value: canonicalEntryExposure(input.Exposure)},
			requestidempotency.Field{Name: "source", Value: canonicalEntryEditSource(input.Source)},
		)),
	})
	if err != nil {
		return entryEditEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return entryEditEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return entryEditEvidence{}, err
	}
	return entryEditEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableEntryEditIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableEntryEditIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence entryEditEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryEditIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryEditEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryEditIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence entryEditEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(
		ctx,
		service.repository,
		locator,
		evidence.candidate,
		original,
	)
}

func prepareEntryEditInput(input apiTypes.EntryEditRequest) (entryEditInput, error) {
	source, err := entryCreationSource(input.Source)
	if err != nil {
		return entryEditInput{}, err
	}
	exposure, err := normalizeEntryExposure(input.Exposure)
	if err != nil {
		return entryEditInput{}, err
	}
	return entryEditInput{Source: source, Exposure: exposure}, nil
}

func canonicalEntryEditSource(source core.EntrySource) requestidempotency.Value {
	switch source.Kind {
	case core.SourceLiteral:
		digest := sha256.Sum256([]byte(source.Literal))
		return requestidempotency.Object(
			requestidempotency.Field{
				Name:  "kind",
				Value: requestidempotency.String(string(source.Kind)),
			},
			requestidempotency.Field{
				Name:  "literal_sha256",
				Value: requestidempotency.String(hex.EncodeToString(digest[:])),
			},
		)
	case core.SourceSecretRef:
		return requestidempotency.Object(
			requestidempotency.Field{
				Name:  "kind",
				Value: requestidempotency.String(string(source.Kind)),
			},
			requestidempotency.Field{
				Name:  "secret_ref",
				Value: requestidempotency.String(source.SecretRef),
			},
		)
	case core.SourceFact:
		return requestidempotency.Object(
			requestidempotency.Field{
				Name:  "attach_id",
				Value: requestidempotency.String(source.Fact.Attach),
			},
			requestidempotency.Field{Name: "fact", Value: requestidempotency.String(source.Fact.Key)},
			requestidempotency.Field{
				Name:  "grant_attach_id",
				Value: requestidempotency.String(source.Fact.Grant),
			},
			requestidempotency.Field{
				Name:  "kind",
				Value: requestidempotency.String(string(source.Kind)),
			},
		)
	default:
		return requestidempotency.Object()
	}
}
