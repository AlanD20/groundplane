package network

import (
	"context"
	"encoding/json"
	"errors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/composekey"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/ipam"
	"github.com/AlanD20/groundplane/internal/controller/desiredrevision"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
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
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironmentComposeProjection(
		context.Context,
		string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		etcd.EnvironmentBlueprintStageClaimRequest,
	) (etcd.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		etcd.EnvironmentBlueprintStageRequest,
	) (etcd.EnvironmentBlueprintSeal, error)
	PublishEnvironmentZoneDesiredRevisionDirect(
		context.Context,
		etcd.EnvironmentZoneDesiredPublication,
	) (etcd.IdempotencyTransactionResult, error)
}

type zoneCreationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type zoneCreationIdempotency interface {
	Prepare(context.Context, apiTypes.ZoneCreate) (zoneCreationEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		zoneCreationEvidence,
	) (requestidempotency.Resolution, bool, error)
	ResolveKnown(
		context.Context,
		zoneCreationEvidence,
		etcd.IdempotencyTransactionResult,
	) (requestidempotency.Resolution, error)
	ResolveUnknown(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		zoneCreationEvidence,
		error,
	) (requestidempotency.Resolution, error)
	MatchesStaged(context.Context, zoneCreationEvidence, idempotencyrecord.ProtectedIntentRecord) (bool, error)
}

type durableZoneCreationIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  requestidempotency.EvidenceRepository
}

func newDurableZoneCreationIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository requestidempotency.EvidenceRepository,
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
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodPost,
		Route:  zoneCreationRoute,
		Scope: requestidempotency.Scope{
			Kind: requestidempotency.ScopeEnvironment,
			ID:   input.EnvironmentID,
		},
		Query: requestidempotency.Object(),
		Body: requestidempotency.JSONBody(requestidempotency.Object(
			requestidempotency.Field{Name: "environment_id", Value: requestidempotency.String(input.EnvironmentID)},
			requestidempotency.Field{Name: "internal", Value: requestidempotency.Bool(input.Internal)},
			requestidempotency.Field{Name: "name", Value: requestidempotency.String(input.Name)},
			requestidempotency.Field{Name: "subnet", Value: requestidempotency.String(input.Subnet)},
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
	locator idempotencyrecord.IdempotencyLocator,
	evidence zoneCreationEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

func (service *durableZoneCreationIdempotency) ResolveKnown(
	ctx context.Context,
	evidence zoneCreationEvidence,
	result etcd.IdempotencyTransactionResult,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveKnown(ctx, evidence.candidate, result)
}

func (service *durableZoneCreationIdempotency) ResolveUnknown(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence zoneCreationEvidence,
	original error,
) (requestidempotency.Resolution, error) {
	return service.coordinator.ResolveUnknown(ctx, service.repository, locator, evidence.candidate, original)
}

func (service *durableZoneCreationIdempotency) MatchesStaged(
	ctx context.Context,
	evidence zoneCreationEvidence,
	existing idempotencyrecord.ProtectedIntentRecord,
) (bool, error) {
	return service.coordinator.MatchesDurable(ctx, evidence.candidate, existing)
}

type zoneCreationService struct {
	repository  zoneCreationRepository
	idempotency zoneCreationIdempotency
	deletions   *zoneDeletionService
	impacts     *zoneRemovalImpactService
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
) (idempotencyrecord.IdempotencyResponse, error) {
	if ctx == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone creation context is required")
	}
	if err := validateZoneCreationInput(input); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	for attempt := 0; attempt < maximumZoneCreationAttempts; attempt++ {
		response, err := service.createZoneOnce(ctx, input, idempotencyKey)
		if err == nil {
			return response, nil
		}
		kind, ok := errs.KindOf(err)
		if !ok || kind != errs.KindStateConflict || attempt == maximumZoneCreationAttempts-1 {
			return idempotencyrecord.IdempotencyResponse{}, err
		}
	}
	return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone creation retry bound was not enforced")
}

func (service *zoneCreationService) createZoneOnce(
	ctx context.Context,
	input apiTypes.ZoneCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	evidence, err := service.idempotency.Prepare(ctx, input)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(evidence.durable.Ciphertext)
	locator := idempotencyrecord.IdempotencyLocator{
		ScopeKind: idempotencyrecord.IdempotencyScopeEnvironment,
		ScopeID:   input.EnvironmentID,
		Method:    http.MethodPost,
		Route:     zoneCreationRoute,
		Key:       idempotencyKey,
	}
	resolution, existing, err := service.idempotency.ResolveExisting(ctx, locator, evidence)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if existing {
		if resolution.Kind != requestidempotency.ResolutionReplay {
			return idempotencyrecord.IdempotencyResponse{}, errs.New(
				errs.KindInternal,
				"Zone creation replay resolution is invalid",
			)
		}
		return cloneIdempotencyResponse(resolution.Response), nil
	}
	projection, found, err := service.repository.GetEnvironmentComposeProjection(ctx, input.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if !found {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Environment has no current desired revision",
		)
	}
	environment, err := service.repository.GetEnvironment(ctx, input.EnvironmentID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	project, err := service.repository.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if environment.ReadRevision <= 0 || projection.ReadRevision != environment.ReadRevision ||
		project.ReadRevision != environment.ReadRevision {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Zone creation hierarchy changed during selection",
		)
	}
	if environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		environment.Record.ID != input.EnvironmentID || projection.Record.EnvironmentID != input.EnvironmentID ||
		project.Record.ID != environment.Record.ProjectID {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Zone creation requires a ready Environment hierarchy",
		)
	}
	environmentPool, err := ipam.ParseIPv4Prefix(environment.Record.NetworkPool)
	if err != nil || environmentPool.String() != environment.Record.NetworkPool {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Environment network pool is invalid")
	}
	requestedSubnet, _ := ipam.ParseIPv4Prefix(input.Subnet)
	if requestedSubnet.Bits() < environmentPool.Bits() || !environmentPool.Contains(requestedSubnet.Addr()) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindValidationFailed,
			"Zone subnet must be inside the Environment network pool",
		)
	}
	if projection.Record.RenderGeneration == ^uint64(0) {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(
			errs.KindStateConflict,
			"Environment render generation is exhausted",
		)
	}
	createdAt := service.now().UTC()
	claim, err := desiredrevision.Claim(ctx, service.repository, desiredrevision.ClaimInput{
		EnvironmentID: input.EnvironmentID, CandidateTaskID: ids.New(ids.KindTask),
		Locator: locator, Intent: evidence.durable,
		MatchExistingIntent: func(matchContext context.Context, existing idempotencyrecord.ProtectedIntentRecord) (bool, error) {
			return service.idempotency.MatchesStaged(matchContext, evidence, existing)
		},
		BaselineHeadRevision: projection.Revision,
		SourceKind:           etcd.EnvironmentBlueprintSourceMutation,
		RenderGeneration:     projection.Record.RenderGeneration + 1,
		CreatedAt:            createdAt,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	allocator, err := desiredrevision.NewBlueprintIdentityAllocator(claim)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	ownerKind := core.ZoneOwnerEnvironment
	ownerID := environment.Record.ID
	if project.Record.Kind == hierarchyrecord.ProjectKindBacking {
		ownerKind = core.ZoneOwnerBackingProject
		ownerID = project.Record.ID
	}
	record, err := zonerecord.NewRecord(environment.Record.ID, core.Zone{
		ID: allocator.Named(ids.KindNetwork, "zone/create"), Name: input.Name, Subnet: input.Subnet,
		Internal: input.Internal, OwnerKind: ownerKind, OwnerID: ownerID,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	candidate, err := buildZoneCreationProjection(
		projection.Record, project.Record, record, claim.RevisionID, claim.RenderGeneration,
	)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	projectionEvidence, err := desiredrevision.PreflightProjection(candidate)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	if _, err := service.repository.StageEnvironmentBlueprintRevision(ctx, etcd.EnvironmentBlueprintStageRequest{
		Claim: claim,
		Mutation: &etcd.EnvironmentDesiredMutationAudit{Zone: &etcd.EnvironmentZoneMutationAudit{
			Action: etcd.EnvironmentZoneMutationCreate, BaseRevisionID: projection.Record.RevisionID,
			ZoneID: record.Desired.ID,
			Request: &etcd.EnvironmentZoneMutationRequest{
				EnvironmentID: input.EnvironmentID, Name: input.Name, Subnet: input.Subnet, Internal: input.Internal,
			},
		}},
		Projection: candidate, DependencyDigest: projectionEvidence.DependencyDigest,
	}); err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	responseBody, err := json.Marshal(apiTypes.Zone{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Name: record.Desired.Name,
		Subnet: record.Desired.Subnet, Internal: record.Desired.Internal,
		OwnerKind: apiTypes.ZoneOwnerKind(record.Desired.OwnerKind), OwnerID: record.Desired.OwnerID,
	})
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.Wrap(errs.KindInternal, err)
	}
	defer clear(responseBody)
	response := idempotencyrecord.IdempotencyResponse{
		Status: http.StatusCreated, ContentKind: "application/json",
		Body: append([]byte(nil), responseBody...),
	}
	marker, err := idempotencyrecord.NewCompletedDirectIdempotencyMarker(locator, evidence.durable, response, claim.CreatedAt)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	defer clear(marker.Intent.Ciphertext)
	defer clear(marker.Response.Body)
	result, createErr := service.repository.PublishEnvironmentZoneDesiredRevisionDirect(
		ctx,
		etcd.EnvironmentZoneDesiredPublication{
			Project: project, Environment: environment, ExpectedHeadRevision: projection.Revision,
			Claim: claim,
			Revision: etcd.EnvironmentDesiredRevisionIdentity{
				EnvironmentID: environment.Record.ID, RevisionID: claim.RevisionID,
			},
			Projection: candidate, Zone: record, Marker: marker,
		},
	)
	if createErr != nil {
		if !isUnknownZoneCreationOutcome(createErr) {
			return idempotencyrecord.IdempotencyResponse{}, createErr
		}
		resolution, err = service.idempotency.ResolveUnknown(ctx, locator, evidence, createErr)
	} else {
		resolution, err = service.idempotency.ResolveKnown(ctx, evidence, result)
	}
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	switch resolution.Kind {
	case requestidempotency.ResolutionApplied:
		return cloneIdempotencyResponse(response), nil
	case requestidempotency.ResolutionReplay:
		return cloneIdempotencyResponse(resolution.Response), nil
	default:
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Zone creation resolution is invalid")
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
