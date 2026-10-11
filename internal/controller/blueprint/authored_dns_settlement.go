package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredDNSSettlementRepository interface {
	TrySettleBlueprintDNS(context.Context, etcd.TaskRecord, blueprintunits.Unit) (bool, error)
}

// DNS uses the platform resolver's real Agent Task, not a separate unit child.
// False means its acknowledgement is pending, never permission for a no-op.
func (service *Service) TrySettleDNS(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
) (bool, error) {
	if service == nil || ctx == nil {
		return false, errs.New(errs.KindInternal, "Blueprint DNS settler is not configured")
	}
	settler, ok := service.repository.(authoredDNSSettlementRepository)
	if !ok {
		return false, errs.New(errs.KindInternal, "Blueprint DNS settlement repository is not configured")
	}
	return settler.TrySettleBlueprintDNS(ctx, parent, unit)
}
