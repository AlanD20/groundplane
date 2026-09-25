package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredDormantServiceRepository interface {
	TrySettleBlueprintDormantService(
		context.Context, etcd.TaskRecord, blueprintunits.Unit,
	) (bool, error)
}

func (service *Service) TrySettleDormantService(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (bool, error) {
	if service == nil || ctx == nil {
		return false, errs.New(errs.KindInternal, "Blueprint dormant Service settler is not configured")
	}
	settler, ok := service.repository.(authoredDormantServiceRepository)
	if !ok {
		return false, errs.New(errs.KindInternal, "Blueprint dormant Service repository is not configured")
	}
	return settler.TrySettleBlueprintDormantService(ctx, parent, unit)
}
