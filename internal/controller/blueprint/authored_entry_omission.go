package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredEntryOmissionRepository interface {
	TrySettleBlueprintEntryOmission(context.Context, etcd.TaskRecord, blueprintunits.Unit) (bool, error)
}

func (service *Service) TrySettleEntryOmission(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit,
) (bool, error) {
	if service == nil || ctx == nil {
		return false, errs.New(errs.KindInternal, "Blueprint Entry omission settler is not configured")
	}
	settler, ok := service.repository.(authoredEntryOmissionRepository)
	if !ok {
		return false, errs.New(errs.KindInternal, "Blueprint Entry omission repository is not configured")
	}
	return settler.TrySettleBlueprintEntryOmission(ctx, parent, unit)
}
