// Package network owns the Controller use cases for Environment Zones and
// Routes. Application wiring constructs one Service; HTTP handlers depend on
// that concrete capability instead of rebuilding lifecycle logic in app.
package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/controller"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	corenetwork "github.com/AlanD20/groundplane/internal/core/network"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	networketcd "github.com/AlanD20/groundplane/internal/infra/etcd/network"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Service is the concrete Network capability used by every human surface.
type Service struct {
	repository     *networketcd.Repository
	zoneReads      *zoneReadService
	zoneMutations  *zoneCreationService
	zoneImpacts    *zoneRemovalImpactService
	zoneDeletionID zoneDeletionIdempotency
	routeReads     *routeReadService
	routeMutations *routeMutationService
}

type idempotencyEvidenceRepository interface {
	requestidempotency.EvidenceRepository
	ResolveReplayLocator(
		context.Context,
		etcd.IdempotencyReplayTarget,
		string,
		string,
		string,
	) (etcd.IdempotencyLocator, bool, error)
}

// NewEtcdService constructs the complete Network capability from its durable
// adapters and shared Controller facilities. It performs composition only;
// all Zone and Route decisions remain in this package.
func NewEtcdService(
	repository *networketcd.Repository,
	plans *controller.TaskPlanResolver,
	coordinator *requestidempotency.Coordinator,
) (*Service, error) {
	if repository == nil || plans == nil || coordinator == nil {
		return nil, errs.New(errs.KindInternal, "network capability dependencies are not configured")
	}
	zoneReads, err := newZoneReadService(repository)
	if err != nil {
		return nil, err
	}
	zoneCreationIdempotency, err := newDurableZoneCreationIdempotency(coordinator, repository)
	if err != nil {
		return nil, err
	}
	zoneMutations, err := newZoneCreationService(repository, zoneCreationIdempotency)
	if err != nil {
		return nil, err
	}
	zoneDeletionIdempotency, err := newDurableZoneDeletionIdempotency(coordinator, repository)
	if err != nil {
		return nil, err
	}
	zoneDeletions, err := newZoneDeletionService(repository, plans, zoneDeletionIdempotency)
	if err != nil {
		return nil, err
	}
	zoneImpacts, err := newZoneRemovalImpactService(repository, repository, repository, repository)
	if err != nil {
		return nil, err
	}
	zoneMutations.deletions = zoneDeletions
	zoneMutations.impacts = zoneImpacts
	zoneDeletions.impacts = zoneImpacts

	routeReads, err := newRouteReadService(repository)
	if err != nil {
		return nil, err
	}
	routeMutationIdempotency, err := newDurableRouteMutationIdempotency(coordinator, repository)
	if err != nil {
		return nil, err
	}
	if err := plans.EnableRoutePlans(repository); err != nil {
		return nil, err
	}
	routeMutations, err := newRouteMutationServiceWithPlanner(repository, routeMutationIdempotency, plans)
	if err != nil {
		return nil, err
	}
	routeDeletions, err := newRouteRemovalService(
		repository,
		plans,
		&durableRouteRemovalIdempotency{coordinator: coordinator, repository: repository},
	)
	if err != nil {
		return nil, err
	}
	routeMutations.deletions = routeDeletions
	return &Service{
		repository: repository,
		zoneReads:  zoneReads, zoneMutations: zoneMutations, zoneImpacts: zoneImpacts,
		zoneDeletionID: zoneDeletionIdempotency,
		routeReads:     routeReads, routeMutations: routeMutations,
	}, nil
}

// NewBackingZoneCascadeExecutor constructs the Controller executor for the
// sole Zone dependency-cascade exception. The executor stays part of the
// Network capability while app supplies already-constructed collaborators.
func (service *Service) NewBackingZoneCascadeExecutor(
	detaches backingZoneCascadeDetaches,
	retries backingZoneCascadeRetries,
	plans *controller.TaskPlanResolver,
) (*backingZoneCascadeService, error) {
	if service == nil || service.repository == nil || service.zoneDeletionID == nil {
		return nil, errs.New(errs.KindInternal, "network capability is not configured")
	}
	return newBackingZoneCascadeService(
		service.repository,
		detaches,
		retries,
		plans,
		service.zoneDeletionID,
	)
}

func (service *Service) GetZone(
	ctx context.Context,
	id string,
) (corenetwork.Zone, error) {
	stored, err := service.zoneReads.GetZone(ctx, id)
	if err != nil {
		return corenetwork.Zone{}, err
	}
	return projectZone(stored.Record), nil
}

func (service *Service) ListZones(
	ctx context.Context,
	environmentID string,
	request corenetwork.PageRequest,
) (corenetwork.Page[corenetwork.Zone], error) {
	stored, err := service.zoneReads.ListZones(ctx, environmentID, etcd.PageRequest{
		Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil {
		return corenetwork.Page[corenetwork.Zone]{}, err
	}
	items := make([]corenetwork.Zone, len(stored.Items))
	for index, item := range stored.Items {
		items[index] = projectZone(item.Record)
	}
	return corenetwork.Page[corenetwork.Zone]{Items: items, NextCursor: stored.NextCursor}, nil
}

func (service *Service) CreateZone(
	ctx context.Context,
	input corenetwork.CreateZoneRequest,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	response, err := service.zoneMutations.CreateZone(ctx, apiTypes.ZoneCreate{
		EnvironmentID: input.EnvironmentID, Name: input.Name, Subnet: input.Subnet, Internal: input.Internal,
	}, idempotencyKey)
	return projectMutationResponse(response), err
}

func (service *Service) GetZoneRemovalImpact(
	ctx context.Context,
	id string,
) (corenetwork.ZoneRemovalImpact, error) {
	impact, err := service.zoneImpacts.GetZoneRemovalImpact(ctx, id)
	if err != nil {
		return corenetwork.ZoneRemovalImpact{}, err
	}
	return projectZoneRemovalImpact(impact), nil
}

func (service *Service) RemoveZone(
	ctx context.Context,
	id string,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	response, err := service.zoneMutations.RemoveZone(ctx, id, idempotencyKey)
	return projectMutationResponse(response), err
}

func (service *Service) RemoveZoneWithImpact(
	ctx context.Context,
	id string,
	idempotencyKey string,
	impactToken string,
) (corenetwork.MutationResponse, error) {
	response, err := service.zoneMutations.RemoveZoneWithImpact(ctx, id, idempotencyKey, impactToken)
	return projectMutationResponse(response), err
}

func (service *Service) GetRoute(
	ctx context.Context,
	id string,
) (corenetwork.Route, error) {
	stored, err := service.routeReads.GetRoute(ctx, id)
	if err != nil {
		return corenetwork.Route{}, err
	}
	return projectRoute(stored.Record), nil
}

func (service *Service) ListRoutes(
	ctx context.Context,
	environmentID string,
	request corenetwork.PageRequest,
) (corenetwork.Page[corenetwork.Route], error) {
	stored, err := service.routeReads.ListRoutes(ctx, environmentID, etcd.PageRequest{
		Limit: request.Limit, Cursor: request.Cursor,
	})
	if err != nil {
		return corenetwork.Page[corenetwork.Route]{}, err
	}
	items := make([]corenetwork.Route, len(stored.Items))
	for index, item := range stored.Items {
		items[index] = projectRoute(item.Record)
	}
	return corenetwork.Page[corenetwork.Route]{Items: items, NextCursor: stored.NextCursor}, nil
}

func (service *Service) CreateRoute(
	ctx context.Context,
	input corenetwork.CreateRouteRequest,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	response, err := service.routeMutations.CreateRoute(ctx, apiTypes.RouteCreate{
		EnvironmentID: input.EnvironmentID, Host: input.Host, Path: input.Path, Exposure: string(input.Exposure),
		TargetServiceID: input.TargetServiceID, TargetPort: input.TargetPort,
	}, idempotencyKey)
	return projectMutationResponse(response), err
}

func (service *Service) EditRoute(
	ctx context.Context,
	id string,
	input corenetwork.EditRouteRequest,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	response, err := service.routeMutations.EditRoute(
		ctx, id, apiTypes.RouteEdit{Exposure: string(input.Exposure)}, idempotencyKey,
	)
	return projectMutationResponse(response), err
}

func (service *Service) RemoveRoute(
	ctx context.Context,
	id string,
	idempotencyKey string,
) (corenetwork.MutationResponse, error) {
	response, err := service.routeMutations.RemoveRoute(ctx, id, idempotencyKey)
	return projectMutationResponse(response), err
}

func projectZone(record etcd.ZoneRecord) corenetwork.Zone {
	return corenetwork.Zone{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Name: record.Desired.Name,
		Subnet: record.Desired.Subnet, Internal: record.Desired.Internal,
		OwnerKind: corenetwork.ZoneOwnerKind(record.Desired.OwnerKind), OwnerID: record.Desired.OwnerID,
	}
}

func projectRoute(record etcd.RouteRecord) corenetwork.Route {
	return corenetwork.Route{
		ID: record.Desired.ID, EnvironmentID: record.EnvironmentID, Host: record.Desired.Host,
		Path: record.Desired.Path, Exposure: corenetwork.RouteExposure(record.Desired.Exposure),
		TargetServiceID: record.Desired.TargetServiceID, TargetPort: record.Desired.TargetPort,
		Status: corenetwork.RouteStatus(record.Observed.Status),
	}
}

func projectMutationResponse(response etcd.IdempotencyResponse) corenetwork.MutationResponse {
	return corenetwork.MutationResponse{
		Status: response.Status, ContentKind: response.ContentKind, Body: append([]byte(nil), response.Body...),
	}
}

func projectZoneRemovalImpact(impact apiTypes.ZoneRemovalImpact) corenetwork.ZoneRemovalImpact {
	result := corenetwork.ZoneRemovalImpact{
		ZoneID: impact.ZoneID, ZoneName: impact.ZoneName, Mode: corenetwork.ZoneRemovalImpactMode(impact.Mode),
		ImpactToken: impact.ImpactToken,
		Attaches:    make([]corenetwork.ZoneRemovalImpactAttach, len(impact.Attaches)),
		Services:    make([]corenetwork.ZoneRemovalImpactService, len(impact.Services)),
		Databases:   make([]corenetwork.ZoneRemovalImpactDatabase, len(impact.Databases)),
	}
	for index, attach := range impact.Attaches {
		result.Attaches[index] = corenetwork.ZoneRemovalImpactAttach{
			ID: attach.ID, Name: attach.Name, EnvironmentID: attach.EnvironmentID,
			ServiceID: attach.ServiceID, Database: attach.Database, Status: attach.Status,
		}
	}
	for index, target := range impact.Services {
		result.Services[index] = corenetwork.ZoneRemovalImpactService{
			ID: target.ID, Name: target.Name, EnvironmentID: target.EnvironmentID,
		}
	}
	for index, database := range impact.Databases {
		result.Databases[index] = corenetwork.ZoneRemovalImpactDatabase{
			AttachID: database.AttachID, Name: database.Name,
		}
	}
	return result
}

func cloneIdempotencyResponse(response etcd.IdempotencyResponse) etcd.IdempotencyResponse {
	response.Body = append([]byte(nil), response.Body...)
	return response
}
