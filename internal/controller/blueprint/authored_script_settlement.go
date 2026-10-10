package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredScriptSettlementRepository interface {
	SettleBlueprintScript(context.Context, etcd.TaskRecord, blueprintunits.Unit) error
}

func (service *Service) SettleScript(ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit) error {
	if service == nil || ctx == nil {
		return errs.New(errs.KindInternal, "Blueprint Script settler is not configured")
	}
	settler, ok := service.repository.(authoredScriptSettlementRepository)
	if !ok {
		return errs.New(errs.KindInternal, "Blueprint Script settlement repository is not configured")
	}
	return settler.SettleBlueprintScript(ctx, parent, unit)
}
