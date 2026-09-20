package backingservices

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type backingServiceReadRepository interface {
	GetBackingService(context.Context, string) (etcd.Versioned[etcd.BackingServiceRecord], error)
	ListBackingServices(context.Context, etcd.PageRequest) (etcd.Page[etcd.BackingServiceRecord], error)
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
) (etcd.Versioned[etcd.BackingServiceRecord], error) {
	if ctx == nil {
		return etcd.Versioned[etcd.BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service read context is required",
		)
	}
	if ids.Validate(ids.KindProject, projectID) != nil {
		return etcd.Versioned[etcd.BackingServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Backing-service read requires a stable Project id",
		)
	}
	return service.repository.GetBackingService(ctx, projectID)
}

func (service *ReadService) ListBackingServices(
	ctx context.Context,
	request etcd.PageRequest,
) (etcd.Page[etcd.BackingServiceRecord], error) {
	if ctx == nil {
		return etcd.Page[etcd.BackingServiceRecord]{}, errs.New(
			errs.KindInternal,
			"Backing-service list context is required",
		)
	}
	if request.Limit < 0 {
		return etcd.Page[etcd.BackingServiceRecord]{}, errs.New(
			errs.KindValidationFailed,
			"Backing-service list limit must be a positive integer",
		)
	}
	return service.repository.ListBackingServices(ctx, request)
}
