package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
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

type entryEditRepository interface {
	GetEntry(context.Context, string) (etcdstore.Versioned[entryrecord.Record], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[servicerecord.ServiceRecord], error)
	ReplaceEntryIdempotent(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[entryrecord.Record],
		core.EnvEntry,
		string,
		entryrecord.EntryValueGeneration,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
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

type entryEditService struct {
	repository  entryEditRepository
	generator   entryCreationGenerator
	idempotency entryEditIdempotency
	now         func() time.Time
}

func newEntryEditService(
	repository entryEditRepository,
	generator entryCreationGenerator,
	idempotency entryEditIdempotency,
) (*entryEditService, error) {
	if repository == nil || generator == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Entry edit service is not configured")
	}
	return &entryEditService{
		repository:  repository,
		generator:   generator,
		idempotency: idempotency,
		now:         time.Now,
	}, nil
}

func (service *entryEditService) EditEntry(
	ctx context.Context,
	entryID string,
	request apiTypes.EntryEditRequest,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit context is required",
		)
	}
	if ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit requires a stable Entry id",
		)
	}
	input, err := prepareEntryEditInput(request)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		response, err := service.editEntryOnce(ctx, entryID, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Entry edit retry bound was not enforced",
	)
}

func (service *entryEditService) editEntryOnce(
	ctx context.Context,
	entryID string,
	input entryEditInput,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	target := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetEntry,
		ID:   entryID,
	}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPatch,
		entryEditRoute,
		idempotencyKey,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayEntryEdit(ctx, locator, entryID, input)
	}
	current, err := service.repository.GetEntry(ctx, entryID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	locator = idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   current.Record.EnvironmentID,
		Method:    http.MethodPatch,
		Route:     entryEditRoute,
		Key:       idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator.ScopeID, entryID, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry edit replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}
	desired := current.Record.Entry
	desired.Source = input.Source
	desired.Exposure = append([]string(nil), input.Exposure...)
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if err := service.validateExposure(
		ctx,
		current.Record.EnvironmentID,
		desired.Exposure,
	); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	generationID := ids.New(ids.KindConfig)
	generation, err := service.generator.Generate(
		ctx, project.Record.ID, environment.Record.ID, desired, generationID, now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer entrygeneration.ClearEntryValueGeneration(&generation)
	persisted := desired
	if persisted.Secret && persisted.Source.Kind == core.SourceLiteral {
		persisted.Source.Literal = ""
	}
	responseBody, err := json.Marshal(entryCreationResponse(persisted))
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status:      http.StatusOK,
		ContentKind: "application/json",
		Body:        append([]byte(nil), responseBody...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(
		locator,
		evidence.durable,
		response,
		now,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	marker.ReplayTarget = &target
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, mutationErr := service.repository.ReplaceEntryIdempotent(
		ctx,
		environment,
		project,
		current,
		persisted,
		generationID,
		generation,
		marker,
	)
	if mutationErr != nil {
		if !isUnknownEntryCreationOutcome(mutationErr) {
			return idempotencyrecord.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit resolution is invalid",
		)
	}
}

func (service *entryEditService) replayEntryEdit(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	entryID string,
	input entryEditInput,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator.ScopeID, entryID, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != requestidempotency.ResolutionReplay {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit replay index is inconsistent",
		)
	}
	return requestidempotency.CloneResponse(resolution.Response), nil
}

func (service *entryEditService) validateExposure(
	ctx context.Context,
	environmentID string,
	exposure []string,
) error {
	if len(exposure) == 1 && exposure[0] == "all" {
		return nil
	}
	missing := make(map[string]struct{}, len(exposure))
	for _, name := range exposure {
		missing[name] = struct{}{}
	}
	cursor := ""
	for {
		page, err := service.repository.ListServices(
			ctx,
			environmentID,
			etcdstore.PageRequest{Limit: 200, Cursor: cursor},
		)
		if err != nil {
			return err
		}
		for _, item := range page.Items {
			delete(missing, item.Record.Desired.Name)
		}
		if len(missing) == 0 {
			return nil
		}
		if page.NextCursor == "" {
			for _, name := range exposure {
				if _, absent := missing[name]; absent {
					return errs.Newf(
						errs.KindValidationFailed,
						"Entry exposure service %q was not found",
						name,
					)
				}
			}
			return errs.New(errs.KindInternal, "Entry exposure validation is inconsistent")
		}
		cursor = page.NextCursor
	}
}

type durableEntryEditRepository struct {
	*durableEntryCreationRepository
}

func newDurableEntryEditRepository(
	repository *durableEntryCreationRepository,
) (*durableEntryEditRepository, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Entry edit repositories are not configured")
	}
	return &durableEntryEditRepository{durableEntryCreationRepository: repository}, nil
}

func (repository *durableEntryEditRepository) GetEntry(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[entryrecord.Record], error) {
	return repository.entries.GetEntry(ctx, id)
}

func (repository *durableEntryEditRepository) ReplaceEntryIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	current etcdstore.Versioned[entryrecord.Record],
	desired core.EnvEntry,
	generationID string,
	generation entryrecord.EntryValueGeneration,
	marker idempotencyrecord.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.entries.ReplaceEntryIdempotent(
		ctx,
		environment,
		project,
		current,
		desired,
		generationID,
		generation,
		marker,
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

func prepareEntryEdit(
	current core.EnvEntry,
	input apiTypes.EntryEditRequest,
) (core.EnvEntry, error) {
	prepared, err := prepareEntryEditInput(input)
	if err != nil {
		return core.EnvEntry{}, err
	}
	desired := current
	desired.Source = prepared.Source
	desired.Exposure = prepared.Exposure
	return desired, nil
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
