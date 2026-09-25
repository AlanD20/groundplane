package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredRouteSettlementRepository interface {
	SettleBlueprintRoute(context.Context, etcd.TaskRecord, blueprintunits.Unit) error
}

func (service *Service) SettleRoute(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit,
) error {
	if service == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Blueprint Route settler is not configured")
	}
	settler, ok := service.repository.(authoredRouteSettlementRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint Route settlement repository is not configured")
	}
	return settler.SettleBlueprintRoute(ctx, parent, unit)
}
