package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredScriptSettlementRepository interface {
	PrepareBlueprintScriptSettlement(
		context.Context,
		workqueue.View,
		blueprintunits.Unit,
		time.Time,
	) (workqueue.Change, error)
}

func (service *Service) prepareScriptSettlement(
	ctx context.Context,
	work workqueue.View,
	unit blueprintunits.Unit,
	at time.Time,
) (workqueue.Change, error) {
	if service == nil || ctx == nil {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Script settler is not configured")
	}
	settler, ok := service.repository.(authoredScriptSettlementRepository)
	if !ok {
		return workqueue.Change{}, errs.New(
			errs.KindInternal,
			"Blueprint Script settlement repository is not configured",
		)
	}
	return settler.PrepareBlueprintScriptSettlement(ctx, work, unit, at)
}
