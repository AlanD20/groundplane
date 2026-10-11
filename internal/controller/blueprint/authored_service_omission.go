package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredServiceOmissionRepository interface {
	TrySettleBlueprintServiceOmission(context.Context, etcd.TaskRecord, blueprintunits.Unit) (bool, error)
}

func (service *Service) TrySettleServiceOmission(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit,
) (bool, error) {
	if service == nil || ctx == nil {
		return false, errs.New(errs.KindInternal, "Blueprint Service omission settler is not configured")
	}
	settler, ok := service.repository.(authoredServiceOmissionRepository)
	if !ok {
		return false, errs.New(errs.KindInternal, "Blueprint Service omission repository is not configured")
	}
	return settler.TrySettleBlueprintServiceOmission(ctx, parent, unit)
}
