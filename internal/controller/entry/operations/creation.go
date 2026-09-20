package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"net/http"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/entrygeneration"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	entryCreationRoute           = "/entries"
	maximumEntryCreationAttempts = 3
	maximumEntryNumericID        = int64(1<<32 - 2)
)

type entryCreationRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	ListServices(context.Context, string, etcdstore.PageRequest) (etcdstore.Page[etcd.ServiceRecord], error)
	CreateEntryIdempotent(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		entryrecord.Record,
		etcd.EntryValueGeneration,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type entryCreationGenerator interface {
	Generate(
		context.Context,
		string,
		string,
		core.EnvEntry,
		string,
		time.Time,
	) (etcd.EntryValueGeneration, error)
}

type entryCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type entryCreationIdempotency interface {
	Prepare(context.Context, string, core.EnvEntry) (entryCreationEvidence, error)
	MatchesStaged(context.Context, entryCreationEvidence, etcd.ProtectedIntentRecord) (bool, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		entryCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		entryCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		entryCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
}

func (service *durableEntryCreationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence entryCreationEvidence,
	existing etcd.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
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

func (service *durableEntryCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
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
	locator etcd.IdempotencyLocator,
	evidence entryCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type entryCreationService struct {
	repository  entryCreationRepository
	generator   entryCreationGenerator
	idempotency entryCreationIdempotency
	now         func() time.Time
}

func newEntryCreationService(
	repository entryCreationRepository,
	generator entryCreationGenerator,
	idempotency entryCreationIdempotency,
) (*entryCreationService, error) {
	if repository == nil || generator == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Entry creation service is not configured")
	}
	return &entryCreationService{
		repository: repository, generator: generator, idempotency: idempotency, now: time.Now,
	}, nil
}

func (service *entryCreationService) CreateEntry(
	ctx context.Context,
	input apiTypes.EntryCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry creation context is required")
	}
	for attempt := 0; attempt < maximumEntryCreationAttempts; attempt++ {
		response, err := service.createEntryOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumEntryCreationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry creation retry bound was not enforced")
}

func (service *entryCreationService) createEntryOnce(
	ctx context.Context,
	input apiTypes.EntryCreateRequest,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	entry, err := prepareEntryCreation(input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	evidence, err := service.idempotency.Prepare(ctx, input.EnvironmentID, entry)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment, ScopeID: input.EnvironmentID,
		Method: http.MethodPost, Route: entryCreationRoute, Key: idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Entry creation replay resolution is invalid",
			)
		}
		return requestidempotency.CloneResponse(resolution.Response), nil
	}

	environment, err := service.repository.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if err := service.validateExposure(ctx, input.EnvironmentID, entry.Exposure); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	now := service.now().UTC()
	generationID := ids.New(ids.KindConfig)
	generation, err := service.generator.Generate(
		ctx,
		project.Record.ID,
		environment.Record.ID,
		entry,
		generationID,
		now,
	)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer entrygeneration.ClearEntryValueGeneration(&generation)
	persisted := entry
	if persisted.Secret && persisted.Source.Kind == core.SourceLiteral {
		persisted.Source.Literal = ""
	}
	record, err := entryrecord.NewRecord(environment.Record.ID, persisted, generationID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(entryCreationResponse(persisted))
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json", Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, now)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateEntryIdempotent(
		ctx, environment, project, record, generation, marker,
	)
	if createErr != nil {
		if !isUnknownEntryCreationOutcome(createErr) {
			return etcd.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return requestidempotency.CloneResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return requestidempotency.CloneResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Entry creation resolution is invalid")
	}
}

func (service *entryCreationService) validateExposure(
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
		page, err := service.repository.ListServices(ctx, environmentID, etcdstore.PageRequest{Limit: 200, Cursor: cursor})
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
					return errs.Newf(errs.KindServiceNotFound, "Entry exposure service %q was not found", name)
				}
			}
			return errs.New(errs.KindInternal, "Entry exposure validation did not identify a missing service")
		}
		cursor = page.NextCursor
	}
}

func prepareEntryCreation(input apiTypes.EntryCreateRequest) (core.EnvEntry, error) {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil {
		return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "Entry creation requires a stable Environment id")
	}
	source, err := entryCreationSource(input.Source)
	if err != nil {
		return core.EnvEntry{}, err
	}
	exposure, err := normalizeEntryExposure(input.Exposure)
	if err != nil {
		return core.EnvEntry{}, err
	}
	uid, err := entryCreationNumericID(input.UID, "uid")
	if err != nil {
		return core.EnvEntry{}, err
	}
	gid, err := entryCreationNumericID(input.GID, "gid")
	if err != nil {
		return core.EnvEntry{}, err
	}
	entry := core.EnvEntry{
		ID: ids.New(ids.KindEnvEntry), Kind: core.EntryKind(input.Type), Key: input.Key, Path: input.Path,
		UID: uid, GID: gid, Source: source, Exposure: exposure, Secret: input.Secret,
	}
	if len(entry.Source.Literal) > apiTypes.MaximumEntryValueBytes {
		return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "Entry literal exceeds the 256 KiB limit")
	}
	if entry.Source.Kind == core.SourceSecretRef && !entry.Secret {
		return core.EnvEntry{}, errs.New(errs.KindValidationFailed, "Reusable Secret requires a secret Entry")
	}
	validationEntry := entry
	if validationEntry.Secret && validationEntry.Source.Kind == core.SourceLiteral {
		validationEntry.Source.Literal = ""
	}
	if err := validationEntry.Validate(); err != nil {
		return core.EnvEntry{}, errs.Wrap(errs.KindValidationFailed, err)
	}
	if entry.Kind == core.EntryKindFile {
		if err := entrymaterialization.ValidateDesiredDestination(entry.Path); err != nil {
			return core.EnvEntry{}, err
		}
	}
	return entry, nil
}

func entryCreationSource(input apiTypes.EntrySource) (core.EntrySource, error) {
	source := core.EntrySource{
		Kind: core.EntrySourceKind(input.Kind), Literal: input.Literal, SecretRef: input.SecretRef,
	}
	switch source.Kind {
	case core.SourceLiteral:
		if input.SecretRef != "" || input.AttachID != "" || input.GrantAttachID != "" || input.Fact != "" {
			return core.EntrySource{}, errs.New(
				errs.KindValidationFailed,
				"Entry literal source carries another source kind",
			)
		}
	case core.SourceSecretRef:
		if input.SecretRef == "" || input.Literal != "" || input.AttachID != "" ||
			input.GrantAttachID != "" || input.Fact != "" {
			return core.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry secret_ref source is invalid")
		}
	case core.SourceFact:
		if input.Literal != "" || input.SecretRef != "" || input.AttachID == "" || input.Fact == "" ||
			ids.Validate(ids.KindAttach, input.AttachID) != nil ||
			(input.GrantAttachID != "" && ids.Validate(ids.KindAttach, input.GrantAttachID) != nil) {
			return core.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry fact source is invalid")
		}
		source.Fact = &core.FactRef{Attach: input.AttachID, Grant: input.GrantAttachID, Key: input.Fact}
	default:
		return core.EntrySource{}, errs.New(errs.KindValidationFailed, "Entry source kind is invalid")
	}
	return source, nil
}

func normalizeEntryExposure(input []string) ([]string, error) {
	if len(input) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Entry exposure is required")
	}
	exposure := append([]string(nil), input...)
	seen := make(map[string]struct{}, len(exposure))
	for _, value := range exposure {
		if value == "" {
			return nil, errs.New(errs.KindValidationFailed, "Entry exposure contains an empty service")
		}
		if _, duplicate := seen[value]; duplicate {
			return nil, errs.New(errs.KindValidationFailed, "Entry exposure contains a duplicate service")
		}
		seen[value] = struct{}{}
	}
	if _, all := seen["all"]; all {
		if len(exposure) != 1 {
			return nil, errs.New(errs.KindValidationFailed, "Entry exposure all must be the only value")
		}
		return []string{"all"}, nil
	}
	sort.Strings(exposure)
	return exposure, nil
}

func entryCreationNumericID(value *int64, field string) (*uint32, error) {
	if value == nil {
		return nil, nil
	}
	if *value < 0 || *value > maximumEntryNumericID {
		return nil, errs.Newf(errs.KindValidationFailed, "Entry %s is outside the supported numeric range", field)
	}
	converted := uint32(*value)
	return &converted, nil
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

func entryCreationResponse(entry core.EnvEntry) apiTypes.Entry {
	response := apiTypes.Entry{
		ID: entry.ID, Type: string(entry.Kind), Key: entry.Key, Path: entry.Path,
		Source: apiTypes.EntrySource{
			Kind: string(entry.Source.Kind), Literal: entry.Source.Literal, SecretRef: entry.Source.SecretRef,
		},
		Exposure: append([]string(nil), entry.Exposure...), Secret: entry.Secret,
	}
	if entry.UID != nil {
		value := int64(*entry.UID)
		response.UID = &value
	}
	if entry.GID != nil {
		value := int64(*entry.GID)
		response.GID = &value
	}
	if entry.Source.Fact != nil {
		response.Source.AttachID = entry.Source.Fact.Attach
		response.Source.GrantAttachID = entry.Source.Fact.Grant
		response.Source.Fact = entry.Source.Fact.Key
	}
	return response
}

func isUnknownEntryCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}

type durableEntryCreationRepository struct {
	hierarchy *etcd.HierarchyRepository
	entries   *etcd.EntryRepository
	services  *etcd.ServiceRepository
}

func newDurableEntryCreationRepository(
	hierarchy *etcd.HierarchyRepository,
	entries *etcd.EntryRepository,
	services *etcd.ServiceRepository,
) (*durableEntryCreationRepository, error) {
	if hierarchy == nil || entries == nil || services == nil {
		return nil, errs.New(errs.KindInternal, "Entry creation repositories are not configured")
	}
	return &durableEntryCreationRepository{hierarchy: hierarchy, entries: entries, services: services}, nil
}

func (repository *durableEntryCreationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableEntryCreationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableEntryCreationRepository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[etcd.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *durableEntryCreationRepository) CreateEntryIdempotent(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	record entryrecord.Record,
	generation etcd.EntryValueGeneration,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.entries.CreateEntryIdempotent(ctx, environment, project, record, generation, marker)
}
