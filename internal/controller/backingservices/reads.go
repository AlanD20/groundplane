package backingservices

import (
	"context"
	backingservices "github.com/AlanD20/groundplane/internal/infra/etcd/backingservices"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/common/ids"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingServiceReadRepository interface {
	GetBackingService(context.Context, string) (etcdstore.Versioned[backingservices.Record], error)
	ListBackingServices(context.Context, etcdstore.PageRequest) (etcdstore.Page[backingservices.Record], error)
}

type ReadService struct {
	repository backingServiceReadRepository
}

func NewReadService(repository backingServiceReadRepository) (*ReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Backing-service read repository is not configured")
	}
	return &ReadService{repository: repository}, nil
}

func (service *ReadService) GetBackingService(
	ctx context.Context,
	projectID string,
) (etcdstore.Versioned[backingservices.Record], error) {
	if ctx == nil {
		return etcdstore.Versioned[backingservices.Record]{}, errs.New(
			errs.KindInternal,
			"Backing-service read context is required",
		)
	}
	if ids.Validate(ids.KindProject, projectID) != nil {
		return etcdstore.Versioned[backingservices.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Backing-service read requires a stable Project id",
		)
	}
	return service.repository.GetBackingService(ctx, projectID)
}

func (service *ReadService) ListBackingServices(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[backingservices.Record], error) {
	if ctx == nil {
		return etcdstore.Page[backingservices.Record]{}, errs.New(
			errs.KindInternal,
			"Backing-service list context is required",
		)
	}
	if request.Limit < 0 {
		return etcdstore.Page[backingservices.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Backing-service list limit must be a positive integer",
		)
	}
	return service.repository.ListBackingServices(ctx, request)
}
