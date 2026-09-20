// Package network adapts the shared etcd store to the Controller's Network
// capability. It is the only place that assembles Zone and Route persistence
// repositories; Controller use cases receive this capability-specific adapter
// instead of individual concrete stores.
package network

import (
	"context"
	attachrecord "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	desiredrevisionstore "github.com/AlanD20/groundplane/internal/infra/etcd/desiredrevision"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RemovalDatabaseResolver resolves the one dynamic fact needed to preview a
// backing Zone cascade. It is colocated with the adapter because Attach
// persistence records must not cross the Network capability's public service
// boundary.
type RemovalDatabaseResolver interface {
	ResolveRemovalDatabase(context.Context, etcdstore.Versioned[attachrecord.Record], func(string) error) error
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
	facts       RemovalDatabaseResolver
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
	request etcd.EnvironmentBlueprintStageClaimRequest,
) (etcd.EnvironmentBlueprintStageClaim, error) {
	if repository.desired == nil {
		return etcd.EnvironmentBlueprintStageClaim{}, errs.New(
			errs.KindInternal,
			"Network desired revision persistence is not configured",
		)
	}
	return repository.desired.ClaimEnvironmentBlueprintStage(ctx, request)
}

func (repository *Repository) StageEnvironmentBlueprintRevision(
	ctx context.Context,
	request etcd.EnvironmentBlueprintStageRequest,
) (etcd.EnvironmentBlueprintSeal, error) {
	if repository.desired == nil {
		return etcd.EnvironmentBlueprintSeal{}, errs.New(
			errs.KindInternal,
			"Network desired revision persistence is not configured",
		)
	}
	return repository.desired.StageEnvironmentBlueprintRevision(ctx, request)
}

func (repository *Repository) PublishEnvironmentZoneDesiredRevisionDirect(
	ctx context.Context,
	input etcd.EnvironmentZoneDesiredPublication,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.hierarchy.PublishEnvironmentZoneDesiredRevisionDirect(ctx, input)
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
	facts RemovalDatabaseResolver,
) (*Repository, error) {
	if hierarchy == nil || services == nil || zones == nil || routes == nil || attaches == nil || tasks == nil ||
		idempotency == nil || facts == nil {
		return nil, errs.New(errs.KindInternal, "Network persistence dependencies are not configured")
	}
	return &Repository{
		hierarchy: hierarchy, services: services, zones: zones, routes: routes,
		attaches: attaches, tasks: tasks, idempotency: idempotency, facts: facts,
	}, nil
}

func (repository *Repository) Read(
	ctx context.Context,
	locator etcd.IdempotencyLocator,
) (*etcd.IdempotencyEvidence, error) {
	return repository.idempotency.Read(ctx, locator)
}

func (repository *Repository) ResolveReplayLocator(
	ctx context.Context,
	target etcd.IdempotencyReplayTarget,
	method string,
	route string,
	key string,
) (etcd.IdempotencyLocator, bool, error) {
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
) (etcdstore.Versioned[etcd.ServiceRecord], error) {
	return repository.services.GetService(ctx, id)
}

func (repository *Repository) ListServices(
	ctx context.Context,
	environmentID string,
	request etcdstore.PageRequest,
) (etcdstore.Page[etcd.ServiceRecord], error) {
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

func (repository *Repository) BeginZoneDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	zone etcdstore.Versioned[zonerecord.Record],
	authorities etcd.EnvironmentZoneRemovalAuthorities,
	tombstone etcd.DeletionTombstoneRecord,
	intent etcd.ZoneRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.zones.BeginZoneDeletionWithTask(
		ctx,
		environment,
		project,
		zone,
		authorities,
		tombstone,
		intent,
		task,
		marker,
	)
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

func (repository *Repository) BeginRouteMutationWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[etcd.ServiceRecord],
	current *etcdstore.Versioned[routerecord.Record],
	record routerecord.Record,
	intent etcd.RouteMutationIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.routes.BeginRouteMutationWithTask(
		ctx, environment, project, target, current, record, intent, task, marker,
	)
}

func (repository *Repository) GetEnvironmentComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentComposeProjection(ctx, environmentID)
}

func (repository *Repository) GetEnvironmentZoneRemovalAuthorities(
	ctx context.Context,
	environmentID string,
) (etcd.EnvironmentZoneRemovalAuthorities, bool, error) {
	return repository.hierarchy.GetEnvironmentZoneRemovalAuthorities(ctx, environmentID)
}

func (repository *Repository) GetEnvironmentAppliedComposeProjection(
	ctx context.Context,
	environmentID string,
) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return repository.hierarchy.GetEnvironmentAppliedComposeProjection(ctx, environmentID)
}

func (repository *Repository) BeginRouteDeletionWithTask(
	ctx context.Context,
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	target etcdstore.Versioned[etcd.ServiceRecord],
	route etcdstore.Versioned[routerecord.Record],
	projection *etcdstore.Versioned[etcd.EnvironmentComposeProjection],
	tombstone etcd.DeletionTombstoneRecord,
	intent etcd.RouteRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.routes.BeginRouteDeletionWithTask(
		ctx, environment, project, target, route, projection, tombstone, intent, task, marker,
	)
}

func (repository *Repository) GetDeletionTombstone(
	ctx context.Context,
	kind etcd.DeletionTargetKind,
	id string,
) (etcdstore.Versioned[etcd.DeletionTombstoneRecord], bool, error) {
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

func (repository *Repository) GetTask(
	ctx context.Context,
	id string,
) (etcdstore.Versioned[etcd.TaskRecord], error) {
	return repository.tasks.GetTask(ctx, id)
}

func (repository *Repository) GetSystemTaskInitiation(
	ctx context.Context,
	id string,
) (etcd.TaskInitiation, error) {
	return repository.tasks.GetSystemTaskInitiation(ctx, id)
}

func (repository *Repository) GetZoneRemovalIntent(
	ctx context.Context,
	operationID string,
) (etcdstore.Versioned[etcd.ZoneRemovalIntent], bool, error) {
	return repository.hierarchy.GetZoneRemovalIntent(ctx, operationID)
}

func (repository *Repository) HandoffBackingZoneDeletion(
	ctx context.Context,
	zone etcdstore.Versioned[zonerecord.Record],
	parentTaskID string,
	tombstone etcdstore.Versioned[etcd.DeletionTombstoneRecord],
	intent etcd.ZoneRemovalIntent,
	task etcd.TaskRecord,
	marker etcd.IdempotencyMarker,
) (etcd.IdempotencyTransactionResult, error) {
	return repository.zones.HandoffBackingZoneDeletion(ctx, zone, parentTaskID, tombstone, intent, task, marker)
}
