package network

import (
	"context"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"net/http"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/acceptedaction"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/attachremoval"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	zoneDeletionRoute                = "/zones/{id}"
	backingZoneCascadeTimeoutSeconds = int64(24 * 60 * 60)
)

type zoneDeletionRepository interface {
	backingZoneHookInputs
	CaptureBackingZoneDetaches(context.Context, string, string, int64) ([]attachremoval.DetachSnapshot, error)
	ResolveResourceHost(context.Context) (workexecution.Placement, error)
	PrepareNetworkWork(context.Context, etcd.NetworkWorkPublication) (workqueue.Change, error)
	FinishNativePreparationDraft(context.Context, string, bool) (bool, error)
	CleanupNativePreparationDraft(
		context.Context,
		workqueue.TaskSnapshot,
		time.Time,
	) (blueprints.EnvironmentBlueprintStageClaim, bool, error)
	AbandonNetworkWorkInput(context.Context, workqueue.TaskSnapshot, string, []string) (bool, error)
	AbandonEnvironmentBlueprintStage(context.Context, blueprints.EnvironmentBlueprintStageClaim) error
	GetEnvironmentDesiredInput(
		context.Context,
		string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error)
	GetZone(context.Context, string) (etcdstore.Versioned[zonerecord.Record], error)
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetEnvironmentZoneRemovalAuthorities(
		context.Context,
		string,
	) (environmentchanges.EnvironmentZoneRemovalAuthorities, bool, error)
	ClaimEnvironmentBlueprintStage(
		context.Context,
		blueprints.EnvironmentBlueprintStageClaimRequest,
	) (blueprints.EnvironmentBlueprintStageClaim, error)
	StageEnvironmentBlueprintRevision(
		context.Context,
		blueprints.EnvironmentBlueprintStageRequest,
	) (blueprints.EnvironmentBlueprintSeal, error)
}

type zoneDeletionEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type zoneDeletionIdempotency interface {
	ResolveReplayLocator(
		context.Context,
		idempotencyrecord.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (idempotencyrecord.IdempotencyLocator, bool, error)
	Prepare(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		string,
		string,
		[]string,
	) (zoneDeletionEvidence, error)
	ResolveExisting(
		context.Context,
		idempotencyrecord.IdempotencyLocator,
		zoneDeletionEvidence,
	) (requestidempotency.Resolution, bool, error)
}

type durableZoneDeletionIdempotency struct {
	coordinator *requestidempotency.Coordinator
	repository  idempotencyEvidenceRepository
}

func newDurableZoneDeletionIdempotency(
	coordinator *requestidempotency.Coordinator,
	repository idempotencyEvidenceRepository,
) (*durableZoneDeletionIdempotency, error) {
	if coordinator == nil || repository == nil {
		return nil, errs.New(errs.KindInternal, "Zone deletion idempotency is not configured")
	}
	return &durableZoneDeletionIdempotency{coordinator: coordinator, repository: repository}, nil
}

func (service *durableZoneDeletionIdempotency) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return service.repository.ResolveReplayLocator(ctx, target, method, route, key)
}

func (service *durableZoneDeletionIdempotency) Prepare(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	zoneID string,
	impactToken string,
	after []string,
) (zoneDeletionEvidence, error) {
	if locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		ids.Validate(ids.KindEnvironment, locator.ScopeID) != nil {
		return zoneDeletionEvidence{}, errs.New(errs.KindInternal, "Zone deletion replay scope is invalid")
	}
	query := requestidempotency.Object()
	if impactToken != "" {
		query = requestidempotency.Object(requestidempotency.Field{
			Name: "impact_token", Value: requestidempotency.String(impactToken),
		})
	}
	version, digest, err := requestidempotency.Canonicalize(ctx, requestidempotency.CanonicalIntentV1{
		Method: http.MethodDelete, Route: zoneDeletionRoute,
		Scope: requestidempotency.Scope{Kind: requestidempotency.ScopeEnvironment, ID: locator.ScopeID},
		Path:  []requestidempotency.PathBinding{{Name: "id", Value: zoneID}},
		Query: query, Body: requestidempotency.JSONBody(requestidempotency.Object(requestidempotency.Field{Name: "after", Value: acceptedaction.AfterValue(after)})),
	})
	if err != nil {
		return zoneDeletionEvidence{}, err
	}
	defer digest.Destroy()
	candidate, err := service.coordinator.ProtectIntent(ctx, version, digest)
	if err != nil {
		return zoneDeletionEvidence{}, err
	}
	durable, err := candidate.DurableRecord()
	if err != nil {
		return zoneDeletionEvidence{}, err
	}
	return zoneDeletionEvidence{candidate: candidate, durable: durable}, nil
}

func (service *durableZoneDeletionIdempotency) ResolveExisting(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
	evidence zoneDeletionEvidence,
) (requestidempotency.Resolution, bool, error) {
	return service.coordinator.ResolveExisting(ctx, service.repository, locator, evidence.candidate)
}

type zoneDeletionService struct {
	accepted    *acceptedaction.Submitter
	repository  zoneDeletionRepository
	idempotency zoneDeletionIdempotency
	impacts     zoneDeletionImpactResolver
	now         func() time.Time
}

type zoneDeletionImpactResolver interface {
	GetZoneRemovalImpact(context.Context, string) (apiTypes.ZoneRemovalImpact, error)
}

func newZoneDeletionService(
	repository zoneDeletionRepository,
	idempotency zoneDeletionIdempotency,
	accepted *acceptedaction.Submitter,
) (*zoneDeletionService, error) {
	if repository == nil || idempotency == nil || accepted == nil {
		return nil, errs.New(errs.KindInternal, "Zone deletion service is not configured")
	}
	return &zoneDeletionService{
		repository:  repository,
		idempotency: idempotency,
		accepted:    accepted,
		now:         time.Now,
	}, nil
}
