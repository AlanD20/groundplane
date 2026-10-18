package network

import (
	"context"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
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
	GetEnvironmentComposeProjection(
		context.Context, string,
	) (etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection], bool, error)
}

type routeMutationEvidence struct {
	candidate requestidempotency.ProtectedEvidence
	durable   idempotencyrecord.ProtectedIntentRecord
}

type routeMutationService struct {
	repository  routeMutationRepository
	idempotency routeMutationIdempotency
	publisher   *RouteWorkPublisher
	now         func() time.Time
}

func newRouteMutationService(
	repository routeMutationRepository,
	idempotency routeMutationIdempotency,
	publisher *RouteWorkPublisher,
) (*routeMutationService, error) {
	if repository == nil || idempotency == nil || publisher == nil {
		return nil, errs.New(errs.KindInternal, "Route mutation service is not configured")
	}
	return &routeMutationService{
		repository: repository, idempotency: idempotency, publisher: publisher, now: time.Now,
	}, nil
}
