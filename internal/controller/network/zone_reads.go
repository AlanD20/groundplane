package network

import (
	"context"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type zoneReadRepository interface {
	GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error)
	GetZone(context.Context, string) (etcd.Versioned[zonerecord.Record], error)
	ListZones(context.Context, string, etcd.PageRequest) (etcd.Page[zonerecord.Record], error)
}

type zoneReadService struct {
	repository zoneReadRepository
}

func newZoneReadService(repository zoneReadRepository) (*zoneReadService, error) {
	if repository == nil {
		return nil, errs.New(errs.KindInternal, "Zone read repository is not configured")
	}
	return &zoneReadService{repository: repository}, nil
}

func (service *zoneReadService) ListZones(
	ctx context.Context,
	environmentID string,
	request etcd.PageRequest,
) (etcd.Page[zonerecord.Record], error) {
	if ctx == nil {
		return etcd.Page[zonerecord.Record]{}, errs.New(errs.KindInternal, "Zone list context is required")
	}
	if ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return etcd.Page[zonerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Zone list requires a stable Environment id",
		)
	}
	if request.Limit < 0 {
		return etcd.Page[zonerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Zone list limit must be a positive integer",
		)
	}
	if _, err := service.repository.GetEnvironment(ctx, environmentID); err != nil {
		return etcd.Page[zonerecord.Record]{}, err
	}
	return service.repository.ListZones(ctx, environmentID, request)
}

func (service *zoneReadService) GetZone(
	ctx context.Context,
	zoneID string,
) (etcd.Versioned[zonerecord.Record], error) {
	if ctx == nil {
		return etcd.Versioned[zonerecord.Record]{}, errs.New(errs.KindInternal, "Zone read context is required")
	}
	if ids.Validate(ids.KindNetwork, zoneID) != nil {
		return etcd.Versioned[zonerecord.Record]{}, errs.New(
			errs.KindValidationFailed,
			"Zone read requires a stable Zone id",
		)
	}
	return service.repository.GetZone(ctx, zoneID)
}
