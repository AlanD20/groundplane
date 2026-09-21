package backingservices

import (
	"context"
	backingservices "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"

	apiTypes "github.com/AlanD20/groundplane/pkg/api"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingServiceLifecycle interface {
	StartService(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
	StopService(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
	DestroyService(context.Context, string, string) (idempotencyrecord.IdempotencyResponse, error)
}

type MutationService struct {
	backingServices *ReadService
	lifecycle       backingServiceLifecycle
	creations       *CreationService
}

func NewMutationService(
	backingServices *ReadService,
	lifecycle backingServiceLifecycle,
	creations *CreationService,
) (*MutationService, error) {
	if backingServices == nil || lifecycle == nil || creations == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service mutation dependencies are not configured")
	}
	return &MutationService{
		backingServices: backingServices,
		lifecycle:       lifecycle,
		creations:       creations,
	}, nil
}

func (service *MutationService) CreateBackingService(
	ctx context.Context,
	input apiTypes.BackingServiceCreate,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	return service.creations.CreateBackingService(ctx, input, idempotencyKey)
}

func (service *MutationService) StartBackingService(
	ctx context.Context,
	projectID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	backing, err := service.resolve(ctx, projectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.lifecycle.StartService(ctx, backing.ServiceID, idempotencyKey)
}

func (service *MutationService) StopBackingService(
	ctx context.Context,
	projectID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	backing, err := service.resolve(ctx, projectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.lifecycle.StopService(ctx, backing.ServiceID, idempotencyKey)
}

func (service *MutationService) DestroyBackingService(
	ctx context.Context,
	projectID string,
	idempotencyKey string,
) (idempotencyrecord.IdempotencyResponse, error) {
	backing, err := service.resolve(ctx, projectID)
	if err != nil {
		return idempotencyrecord.IdempotencyResponse{}, err
	}
	return service.lifecycle.DestroyService(ctx, backing.ServiceID, idempotencyKey)
}

func (service *MutationService) resolve(
	ctx context.Context,
	projectID string,
) (backingservices.Record, error) {
	if service == nil || service.backingServices == nil || service.lifecycle == nil {
		return backingservices.Record{}, errs.New(
			errs.KindInternal,
			"Backing-service mutation service is not configured",
		)
	}
	stored, err := service.backingServices.GetBackingService(ctx, projectID)
	if err != nil {
		return backingservices.Record{}, err
	}
	return stored.Record, nil
}
