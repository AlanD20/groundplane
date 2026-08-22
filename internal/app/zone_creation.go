package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	zoneCreationRoute           = "/zones"
	maximumZoneCreationAttempts = 3
)

type zoneCreationRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error)
	CreateZoneIdempotent(
		context.Context,
		etcd.Versioned[etcd.EnvironmentRecord],
		etcd.Versioned[etcd.ProjectRecord],
		etcd.ZoneRecord,
		etcd.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type durableZoneCreationRepository struct {
	hierarchy *etcd.HierarchyRepository
	zones     *etcd.ZoneRepository
}

func newDurableZoneCreationRepository(
	hierarchy *etcd.HierarchyRepository,
	zones *etcd.ZoneRepository,
) (*durableZoneCreationRepository, error) {
	if hierarchy == nil || zones == nil {
		return nil, errs.New(errs.KindInternal, "Zone creation repositories are not configured")
	}
	return &durableZoneCreationRepository{hierarchy: hierarchy, zones: zones}, nil
}

func (repository *durableZoneCreationRepository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *durableZoneCreationRepository) GetProject(
	ctx context.Context,
	id string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *durableZoneCreationRepository) CreateZoneIdempotent(
	ctx context.Context,
	environment etcd.Versioned[etcd.EnvironmentRecord],
	project etcd.Versioned[etcd.ProjectRecord],
	record etcd.ZoneRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.zones.CreateZoneIdempotent(ctx, environment, project, record, marker)
}

type zoneCreationEvidence struct {
	candidate idempotentintent.ProtectedEvidence
	durable   etcd.ProtectedIntentRecord
}

type zoneCreationIdempotency interface {
	Prepare(context.Context, apiTypes.ZoneCreate) (zoneCreationEvidence, error)
	ResolveExisting(
		context.Context,
		etcd.IdempotencyLocator,
		zoneCreationEvidence,
	) (idempotentintent.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		zoneCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (idempotentintent.Resolution, error)
	ResolveUnknown(
		context.Context,
		etcd.IdempotencyLocator,
		zoneCreationEvidence,
		error,
	) (idempotentintent.Resolution, error)
}

type durableZoneCreationIdempotency struct {
	coordinator *idempotentintent.Coordinator
	repository  *etcd.IdempotencyRepository
}

func newDurableZoneCreationIdempotency(
	coordinator *idempotentintent.Coordinator,
	repository *etcd.IdempotencyRepository,
) (*durableZoneCreationIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Zone creation idempotency is not configured")
	}
	return &durableZoneCreationIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableZoneCreationIdempotency) Prepare(
	ctx context.Context,
	input apiTypes.ZoneCreate,
) (zoneCreationEvidence, error) {
	version, digest, err := idempotentintent.Canonicalize(ctx, idempotentintent.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  zoneCreationRoute,
		Scope: idempotentintent.Scope{
			Kind: idempotentintent.ScopeEnvironment,
			ID:   input.EnvironmentID,
		},
		Query: idempotentintent.Object(),
		Body: idempotentintent.JSONBody(idempotentintent.Object(
			idempotentintent.Field{Name: "environment_id", Value: idempotentintent.String(input.EnvironmentID)},
			idempotentintent.Field{Name: "internal", Value: idempotentintent.Bool(input.Internal)},
			idempotentintent.Field{Name: "name", Value: idempotentintent.String(input.Name)},
			idempotentintent.Field{Name: "subnet", Value: idempotentintent.String(input.Subnet)},
		)),
	})
	if err != nil {
		return zoneCreationEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return zoneCreationEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return zoneCreationEvidence{}, err
	}
	return zoneCreationEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableZoneCreationIdempotency) ResolveExisting(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence zoneCreationEvidence,
) (idempotentintent.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableZoneCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence zoneCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableZoneCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
	evidence zoneCreationEvidence,
	original error,
) (idempotentintent.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

type zoneCreationService struct {
	repository  zoneCreationRepository
	idempotency zoneCreationIdempotency
	deletions   *zoneDeletionService
	now         func() time.Time
}

func newZoneCreationService(
	repository zoneCreationRepository,
	idempotency zoneCreationIdempotency,
) (*zoneCreationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Zone creation service is not configured")
	}
	return &zoneCreationService{repository: repository, idempotency: idempotency, now: time.Now}, nil
}

func (service *zoneCreationService) CreateZone(
	ctx context.Context,
	input apiTypes.ZoneCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	if ctx == nil {
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone creation context is required")
	}
	if err := validateZoneCreationInput(input); err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumZoneCreationAttempts; attempt++ {
		response, err := service.createZoneOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumZoneCreationAttempts-1 {
			return etcd.IdempotencyResponse{}, err
		}
	}
	return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone creation retry bound was not enforced")
}

func (service *zoneCreationService) createZoneOnce(
	ctx context.Context,
	input apiTypes.ZoneCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, input)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := etcd.IdempotencyLocator{
		ScopeKind: etcd.IdempotencyScopeEnvironment,
		ScopeID:   input.EnvironmentID,
		Method:    http.MethodPost,
		Route:     zoneCreationRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != idempotentintent.ResolutionReplay {
			return etcd.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Zone creation replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	environment, err := service.repository.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	ownerKind := core.ZoneOwnerEnvironment
	ownerID := environment.Record.ID
	if project.Record.Kind == etcd.ProjectKindBacking {
		ownerKind = core.ZoneOwnerBackingProject
		ownerID = project.Record.ID
	}
	record, err := etcd.NewZoneRecord(environment.Record.ID, core.Zone{
		ID: ids.New(ids.KindNetwork), Name: input.Name, Subnet: input.Subnet,
		Internal: input.Internal, OwnerKind: ownerKind, OwnerID: ownerID,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.Zone{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Name: record.Desired.Name,
		Subnet: record.Desired.Subnet, Internal: record.Desired.Internal,
		OwnerKind: apiTypes.ZoneOwnerKind(record.Desired.OwnerKind), OwnerID: record.Desired.OwnerID,
	})
	if err != nil {
		return etcd.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := etcd.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker, err := etcd.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, service.now().UTC())
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.CreateZoneIdempotent(ctx, environment, project, record, marker)
	if createErr != nil {
		if !isUnknownZoneCreationOutcome(createErr) {
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
	case idempotentintent.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case idempotentintent.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return etcd.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone creation resolution is invalid")
	}
}

func validateZoneCreationInput(input apiTypes.ZoneCreate) error {
	if ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil {
		return errs.New(errs.KindValidationFailed, "Zone creation requires a stable Environment id")
	}
	if err := composekey.Validate(input.Name); err != nil {
		return err
	}
	subnet, err := ipam.ParseIPv4Prefix(input.Subnet)
	if err != nil || subnet.String() != input.Subnet {
		return errs.New(errs.KindValidationFailed, "Zone subnet must be a canonical IPv4 CIDR")
	}
	return nil
}

func isUnknownZoneCreationOutcome(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStorageUnavailable
}
