package app

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type volumeReadRepository interface {
	ListEnvironmentVolumes(context.Context, string, etcd.PageRequest) (etcd.Page[etcd.VolumeRecord], error)
}

type volumeReadService struct {
	repository volumeReadRepository
}

func newVolumeReadService(repository volumeReadRepository) (*volumeReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "volume read repository is required")
	}
	return &volumeReadService{repository: repository}, nil
}

func (service *volumeReadService) ListVolumes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.VolumeRecord], error) {
	return service.repository.ListEnvironmentVolumes(ctx, environmentID, request)
}

type durableVolumeReadRepository struct {
	volumes *etcd.VolumeRepository
}

func newDurableVolumeReadRepository(
	volumes *etcd.VolumeRepository,
) (*durableVolumeReadRepository, error) {
	if volumes == nil {
		return nil, errs.New(errs.KindInternal, "volume read repository is required")
	}
	return &durableVolumeReadRepository{volumes: volumes}, nil
}

func (repository *durableVolumeReadRepository) ListEnvironmentVolumes(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[etcd.VolumeRecord], error) {
	return repository.volumes.ListEnvironmentVolumes(ctx, environmentID, request)
}
