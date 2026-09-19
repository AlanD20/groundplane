package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	entrycontroller "github.com/AlanD20/groundplane/internal/controller/entry"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
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
	GetEntry(context.Context, string) (etcd.Versioned[etcd.EntryRecord], error)
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	ListServices(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.ServiceRecord], error)
	ReplaceEntryIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.Versioned[etcd.EntryRecord],
		core.EnvEntry,
		string,
		etcd.EntryValueGeneration,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type entryEditEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type entryEditIdempotency interface {
	Prepare(context.Context, string, string, entryEditInput) (entryEditEvidence, error)
	MatchesStaged(context.Context, entryEditEvidence, etcd.ProtectedIntentRecord) (bool, error)
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		entryEditEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryEditEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		entryEditEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

func (service *durableEntryEditIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryEditEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

type durableEntryEditIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableEntryEditIdempotency(
	coordinator *idempotentintent.Coordinator,
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
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPatch,
		Route:  entryEditRoute,
		Scope:  idempotentintent.Scope{Kind: idempotentintent.ScopeEnvironment, ID: environmentID},
		Path:   []idempotentintent.PathBinding{{Name: "id", Value: entryID}},
		Query:  idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "exposure", Value: canonicalEntryExposure(input.Exposure)},
			idempotentintent.Field{Name: "source", Value: canonicalEntryEditSource(input.Source)},
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
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableEntryEditIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryEditEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableEntryEditIdempotency) ResolveKnown(
	ctx context.Context,
	evidence entryEditEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableEntryEditIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence entryEditEvidence,
	original error,
) (idempotentintent.Resolution, error) {
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

type entryMutationService struct {
	desired *entryDesiredMutationService
	bulk    *entryBulkUpsertService
}

func newEntryMutationService(
	desired *entryDesiredMutationService,
	bulk *entryBulkUpsertService,
) (*entryMutationService, error) {
	if desired == nil || bulk == nil {
		return nil, errs.New(errs.KindInternal, "Entry mutation service is not configured")
	}
	return &entryMutationService{desired: desired, bulk: bulk}, nil
}

func (service *entryMutationService) RemoveEntry(
	ctx context.Context,
	request entrycontroller.RemoveRequest,
) (entrycontroller.RemovalOutcome, error) {
	return service.desired.RemoveEntry(ctx, request)
}

func (service *entryMutationService) CreateEntry(
	ctx context.Context,
	input apiTypes.EntryCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.desired.CreateEntry(ctx, input, idempotencyKey)
}

func (service *entryMutationService) BulkUpsertEntries(
	ctx context.Context,
	input apiTypes.EntryBulkUpsertRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.bulk.BulkUpsertEntries(ctx, input, idempotencyKey)
}

func (service *entryMutationService) EditEntry(
	ctx context.Context,
	entryID string,
	input apiTypes.EntryEditRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.desired.EditEntry(ctx, entryID, input, idempotencyKey)
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
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit context is required",
		)
	}
	if ids.Validate(ids.KindEnvEntry, entryID) != nil {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Entry edit requires a stable Entry id",
		)
	}
	input, err := prepareEntryEditInput(request)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumEntryEditAttempts; attempt++ {
		response, err := service.editEntryOnce(ctx, entryID, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryEditAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(
		errs.KindInternal,
		"Entry edit retry bound was not enforced",
	)
}

func (service *entryEditService) editEntryOnce(
	ctx context.Context,
	entryID string,
	input entryEditInput,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	target := etcd.IdempotencyReplayTarget{Kind: etcd.IdempotencyReplayTargetEntry, ID: entryID}
	locator, indexed, err := service.idempotency.ResolveReplayLocator(
		ctx,
		target,
		http.MethodPatch,
		entryEditRoute,
		idempotencyKey,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if indexed {
		return service.replayEntryEdit(ctx, locator, entryID, input)
	}
	current, err := service.repository.GetEntry(ctx, entryID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	locator = etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   current.Record.EnvironmentID,
		Method:    http.MethodPatch,
		Route:     entryEditRoute,
		Key:       idempotencyKey,
	}
	evidence, err := service.idempotency.Prepare(ctx, locator.ScopeID, entryID, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry edit replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	desired := current.Record.Entry
	desired.Source = input.Source
	desired.Exposure = append([]string(nil), input.Exposure...)
	environment, err := service.repository.GetEnvironment(ctx, current.Record.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if err := service.validateExposure(
		ctx,
		current.Record.EnvironmentID,
		desired.Exposure,
	); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	generationID := ids.New(ids.KindConfig)
	generation, err := service.generator.Generate(
		ctx, project.Record.ID, environment.Record.ID, desired, generationID, now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer entrygeneration.ClearEntryValueGeneration(&generation)
	persisted := desired
	if persisted.Secret && persisted.Source.Kind == core.SourceLiteral {
		persisted.Source.Literal = ""
	}
	responseBody, err := json.Marshal(entryCreationResponse(persisted))
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status:      http.StatusOK,
		ContentKind: "application/json",
		Body:        append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(
		locator,
		evidence.durable,
		response,
		now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
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
			return etcd.IdempotencyResponse{}, mutationErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, mutationErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit resolution is invalid",
		)
	}
}

func (service *entryEditService) replayEntryEdit(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	entryID string,
	input entryEditInput,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, locator.ScopeID, entryID, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if !existing || resolution.Kind != idempotentintent.ResolutionReplay {
		return etcd.IdempotencyResponse{}, errs.New(
			errs.KindInternal,
			"Entry edit replay index is inconsistent",
		)
	}
	return cloneIdempotencyResponse(resolution.Response), nil
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
			etcd.PageRequest{Limit: 200, Cursor: cursor},
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
) (etcd.Versioned[etcd.EntryRecord], error) {
	return repository.entries.GetEntry(ctx, id)
}

func (repository *durableEntryEditRepository) ReplaceEntryIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	current etcd.Versioned[etcd.EntryRecord],
	desired core.EnvEntry,
	generationID string,
	generation etcd.EntryValueGeneration,
	marker etcd.IdempotencyMarker,
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

func canonicalEntryEditSource(source core.EntrySource) idempotentintent.Value {
	switch source.Kind {
	case core.SourceLiteral:
		digest := sha256.Sum256([]byte(source.Literal))
		return idempotentintent.Object(
			idempotentintent.Field{
				Name:  "kind",
				Value: idempotentintent.String(string(source.Kind)),
			},
			idempotentintent.Field{
				Name:  "literal_sha256",
				Value: idempotentintent.String(hex.EncodeToString(digest[:])),
			},
		)
	case core.SourceSecretRef:
		return idempotentintent.Object(
			idempotentintent.Field{
				Name:  "kind",
				Value: idempotentintent.String(string(source.Kind)),
			},
			idempotentintent.Field{
				Name:  "secret_ref",
				Value: idempotentintent.String(source.SecretRef),
			},
		)
	case core.SourceFact:
		return idempotentintent.Object(
			idempotentintent.Field{
				Name:  "attach_id",
				Value: idempotentintent.String(source.Fact.Attach),
			},
			idempotentintent.Field{Name: "fact", Value: idempotentintent.String(source.Fact.Key)},
			idempotentintent.Field{
				Name:  "grant_attach_id",
				Value: idempotentintent.String(source.Fact.Grant),
			},
			idempotentintent.Field{
				Name:  "kind",
				Value: idempotentintent.String(string(source.Kind)),
			},
		)
	default:
		return idempotentintent.Object()
	}
}
