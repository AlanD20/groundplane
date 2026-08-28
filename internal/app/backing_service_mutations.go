package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingServiceLifecycle interface {
	StartService(context.Context, string, string) (etcd.IdempotencyResponse, error)
	StopService(context.Context, string, string) (etcd.IdempotencyResponse, error)
	DestroyService(context.Context, string, string) (etcd.IdempotencyResponse, error)
}

type backingServiceMutationService struct {
	backingServices *backingServiceReadService
	lifecycle       backingServiceLifecycle
	creations       *backingServiceCreationService
}

func newBackingServiceMutationService(
	backingServices *backingServiceReadService,
	lifecycle backingServiceLifecycle,
	creations *backingServiceCreationService,
) (*backingServiceMutationService, error) {
	if backingServices == nil || lifecycle == nil || creations == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutation dependencies are not configured")
	}
	return &backingServiceMutationService{backingServices: backingServices, lifecycle: lifecycle, creations: creations}, nil
}

func (service *backingServiceMutationService) CreateBackingService(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	return service.creations.CreateBackingService(ctx, input, idempotencyKey)
}

func (service *backingServiceMutationService) StartBackingService(
	ctx context.Context,
	projectID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	backing, err := service.resolve(ctx, projectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.lifecycle.StartService(ctx, backing.ServiceID, idempotencyKey)
}

func (service *backingServiceMutationService) StopBackingService(
	ctx context.Context,
	projectID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	backing, err := service.resolve(ctx, projectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.lifecycle.StopService(ctx, backing.ServiceID, idempotencyKey)
}

func (service *backingServiceMutationService) DestroyBackingService(
	ctx context.Context,
	projectID string,
	idempotencyKey string,
) (etcd.IdempotencyResponse, error) {
	backing, err := service.resolve(ctx, projectID)
	if err != nil {
		return etcd.IdempotencyResponse{}, err
	}
	return service.lifecycle.DestroyService(ctx, backing.ServiceID, idempotencyKey)
}

func (service *backingServiceMutationService) resolve(
	ctx context.Context,
	projectID string,
) (etcd.BackingServiceRecord, error) {
	if service == nil || service.backingServices == nil || service.lifecycle == nil {
		return etcd.BackingServiceRecord{}, errs.New(
			errs.KindInternal,
			"Backing-service mutation service is not configured",
		)
	}
	stored, err := service.backingServices.GetBackingService(ctx, projectID)
	if err != nil {
		return etcd.BackingServiceRecord{}, err
	}
	return stored.Record, nil
}
