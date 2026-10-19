// Package network adapts the shared etcd store to the Controller's Network
// capability. It is the only place that assembles Zone and Route persistence
// repositories; Controller use cases receive this capability-specific adapter
// instead of individual concrete stores.
package network

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/backinghook"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemovalDatabaseResolver supplies preview facts and encrypted hook captures
// for a backing Zone cascade. It is colocated with the adapter because Attach
// persistence records must not cross the Network capability's public service
// boundary.
type RemovalDatabaseResolver interface {
	ResolveRemovalDatabase(context.Context, etcdstore.Versioned[attachrecord.Record], func(string) error) error
	SealBackingHookInputsAtRevision(context.Context, string, string, backinghook.Configuration, int64) (*taskconfiguration.BackingHookEncryptedInputs, error)
}

// Repository owns all durable records used by the Network capability.
type Repository struct {
	hierarchy   *etcd.HierarchyRepository
	services    *etcd.ServiceRepository
	zones       *etcd.ZoneRepository
	routes      *etcd.RouteRepository
	attaches    *etcd.AttachRepository
	tasks       *etcd.TaskRepository
	idempotency *etcd.IdempotencyRepository
	desired     *desiredrevisionstore.Repository
	publication *etcd.EnvironmentBlueprintRepository
	facts       RemovalDatabaseResolver
}

func (repository *Repository) GetRouteProviderRuntimeAtRevision(
	ctx context.Context, componentID string, revision int64,
) (etcdstore.Versioned[componentplanning.Runtime], bool, error) {
	return repository.routes.GetRouteProviderRuntimeAtRevision(ctx, componentID, revision)
}

func (repository *Repository) EnableDesiredRevisions(desired *desiredrevisionstore.Repository) error {
	if repository == nil || desired == nil {
		return errs.New(errs.KindInternal, "Network desired revision persistence is not configured")
	}
	repository.desired = desired
	return nil
}

func (repository *Repository) ClaimEnvironmentBlueprintStage(
	ctx context.Context,
	request blueprints.EnvironmentBlueprintStageClaimRequest,
) (blueprints.EnvironmentBlueprintStageClaim, error) {
	if repository.desired == nil {
		return blueprints.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Network desired revision persistence is not configured",
		)
	}
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}

func (repository *Repository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request blueprints.EnvironmentBlueprintStageRequest,
) (blueprints.EnvironmentBlueprintSeal, error) {
	if repository.desired == nil {
		return blueprints.EnvironmentBlueprintSeal{}, errs.New(
			errs.KindInternal,
			"Network desired revision persistence is not configured",
		)
	}
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}

func (repository *Repository) GetEnvironmentDesiredInput(
	ctx context.Context, environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput], bool, error) {
	if repository.desired == nil {
		return etcdstore.Versioned[projectionrecord.EnvironmentDesiredInput]{}, false, errs.New(
			errs.KindInternal, "Network desired revision persistence is not configured",
		)
	}
	return repository.desired.GetEnvironmentDesiredInput(ctx, environmentID)
}

func (repository *Repository) PublishEnvironmentZoneDesiredRevisionDirect(
	ctx context.Context,
	input etcd.EnvironmentZoneDesiredPublication,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.hierarchy.PublishEnvironmentZoneDesiredRevisionDirect(ctx, input)
}

func (repository *Repository) InitializeEnvironmentDesiredState(ctx context.Context, environmentID string,
	locator idempotencyrecord.IdempotencyLocator, intent idempotencyrecord.ProtectedIntentRecord, now time.Time,
) error {
	return repository.hierarchy.InitializeEnvironmentDesiredState(ctx, environmentID, locator, intent, now)
}

// NewRepository constructs the complete Network persistence adapter.
func NewRepository(
	hierarchy *etcd.HierarchyRepository,
	services *etcd.ServiceRepository,
	zones *etcd.ZoneRepository,
	routes *etcd.RouteRepository,
	attaches *etcd.AttachRepository,
	tasks *etcd.TaskRepository,
	idempotency *etcd.IdempotencyRepository,
	publication *etcd.EnvironmentBlueprintRepository,
	facts RemovalDatabaseResolver,
) (*Repository, error) {
	if hierarchy == nil || services == nil || zones == nil || routes == nil || attaches == nil || tasks == nil ||
		idempotency == nil || publication == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "Network persistence dependencies are not configured")
	}
	return &Repository{
		hierarchy: hierarchy, services: services, zones: zones, routes: routes,
		attaches: attaches, tasks: tasks, idempotency: idempotency, publication: publication, facts: facts,
	}, nil
}

func (repository *Repository) Read(
	ctx context.Context,
	locator idempotencyrecord.IdempotencyLocator,
) (*etcd.IdempotencyEvidence, error) {
	return repository.idempotency.Read(ctx, locator)
}

func (repository *Repository) ResolveReplayLocator(
	ctx context.Context,
	target idempotencyrecord.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (idempotencyrecord.IdempotencyLocator, bool, error) {
	return repository.idempotency.ResolveReplayLocator(ctx, target, method, route, key)
}

func (repository *Repository) GetEnvironment(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error) {
	return repository.hierarchy.GetEnvironment(ctx, id)
}

func (repository *Repository) GetProject(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error) {
	return repository.hierarchy.GetProject(ctx, id)
}

func (repository *Repository) GetService(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[servicerecord.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *Repository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[servicerecord.ServiceRecord], error) {
	return repository.services.ListServices(ctx, environmentID, request)
}

func (repository *Repository) GetZone(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[zonerecord.Record], error) {
	return repository.zones.GetZone(ctx, id)
}

func (repository *Repository) ListZones(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[zonerecord.Record], error) {
	return repository.zones.ListZones(ctx, environmentID, request)
}

func (repository *Repository) GetRoute(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[routerecord.Record], error) {
	return repository.routes.GetRoute(ctx, id)
}

func (repository *Repository) ListRoutes(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[routerecord.Record], error) {
	return repository.routes.ListRoutes(ctx, environmentID, request)
}

func (repository *Repository) SnapshotRevision(ctx context.Context) (int64, error) {
	return repository.routes.SnapshotRevision(ctx)
}

func (repository *Repository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *Repository) GetEnvironmentZoneRemovalAuthorities(
	ctx context.Context,
	environmentID string,
) (environmentchanges.EnvironmentZoneRemovalAuthorities, bool, error) {
	return repository.hierarchy.GetEnvironmentZoneRemovalAuthorities(ctx, environmentID)
}

func (repository *Repository) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
}

func (repository *Repository) GetDeletionTombstone(
	ctx context.Context,
	kind deletionrecord.DeletionTargetKind,
	id string,
) (etcdstore.Versioned[deletionrecord.DeletionTombstoneRecord], bool, error) {
	return repository.hierarchy.GetDeletionTombstone(ctx, kind, id)
}

func (repository *Repository) ListAttachesByBackingNetworkAtRevision(
	ctx context.Context,
	projectID string,
	networkID string,
	revision int64,
) ([]etcdstore.Versioned[attachrecord.Record], error) {
	return repository.attaches.ListAttachesByBackingNetworkAtRevision(ctx, projectID, networkID, revision)
}

func (repository *Repository) ResolveRemovalDatabase(
	ctx context.Context,
	attach etcdstore.Versioned[attachrecord.Record],
	yield func(string) error,
) error {
	return repository.facts.ResolveRemovalDatabase(ctx, attach, yield)
}

func (repository *Repository) GetZoneRemovalIntent(
	ctx context.Context,
	operationID string,
) (etcdstore.Versioned[environmentchanges.ZoneRemovalIntent], bool, error) {
	return repository.hierarchy.GetZoneRemovalIntent(ctx, operationID)
}
