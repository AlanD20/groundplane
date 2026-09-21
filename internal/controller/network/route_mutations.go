package network

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

const (
	routeCreationRoute           = "/routes"
	routeEditRoute               = "/routes/{id}"
	maximumRouteMutationAttempts = 3
)

type routeMutationRepository interface {
	GetEnvironment(context.Context, string) (etcdstore.Versioned[hierarchyrecord.EnvironmentRecord], error)
	GetProject(context.Context, string) (etcdstore.Versioned[hierarchyrecord.ProjectRecord], error)
	GetService(context.Context, string) (etcdstore.Versioned[servicerecord.ServiceRecord], error)
	GetRoute(context.Context, string) (etcdstore.Versioned[routerecord.Record], error)
	BeginRouteMutationWithTask(
		context.Context,
		etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
		etcdstore.Versioned[hierarchyrecord.ProjectRecord],
		etcdstore.Versioned[servicerecord.ServiceRecord],
		*etcdstore.Versioned[routerecord.Record],
		routerecord.Record,
		etcd.RouteMutationIntent,
		etcd.TaskRecord,
		idempotencyrecord.IdempotencyMarker,
	) (etcd.IdempotencyTransactionResult, error)
}

type routeMutationProjectionRepository interface {
	GetEnvironmentAppliedComposeProjection(
		context.Context, string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
}

type routeMutationDesiredProjectionRepository interface {
	GetEnvironmentComposeProjection(
		context.Context, string,
	) (etcdstore.Versioned[etcd.EnvironmentComposeProjection], bool, error)
}

type routeMutationTaskPlanner interface {
	PrepareRouteMutationTask(
		context.Context,
		etcd.TaskRecord,
		etcd.RouteMutationIntent,
		etcd.RouteMutationProcedureIDs,
	) (etcd.RouteMutationTaskPreparation, error)
}

type routeMutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type routeMutationService struct {
	repository  routeMutationRepository
	idempotency routeMutationIdempotency
	planner     routeMutationTaskPlanner
	deletions   *routeRemovalService
	now         func() time.Time
}

func newRouteMutationService(
	repository routeMutationRepository,
	idempotency routeMutationIdempotency,
) (*routeMutationService, error) {
	return newRouteMutationServiceWithPlanner(repository, idempotency, nil)
}

func newRouteMutationServiceWithPlanner(
	repository routeMutationRepository,
	idempotency routeMutationIdempotency,
	planner routeMutationTaskPlanner,
) (*routeMutationService, error) {
	if repository == nil || idempotency == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation service is not configured")
	}
	return &routeMutationService{
		repository: repository, idempotency: idempotency, planner: planner, now: time.Now,
	}, nil
}

func (service *routeMutationService) RemoveRoute(
	ctx context.Context,
	routeID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	if service == nil || service.deletions == nil {
		return idempotencyrecord.IdempotencyResponse{}, errs.New(errs.KindInternal, "Route deletion service is not configured")
	}
	return service.deletions.RemoveRoute(ctx, routeID, idempotencyKey)
}
