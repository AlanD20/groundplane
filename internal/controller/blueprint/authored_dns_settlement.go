package blueprint

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredDNSSettlementRepository interface {
	PrepareBlueprintDNS(context.Context, workqueue.View, blueprintunits.Unit, blueprintunits.Snapshot, time.Time) (etcd.BlueprintDNSPreparation, error)
}

// DNS settlement joins the scheduler transaction only after the shared resolver
// acknowledges the captured record. Waiting never creates an Agent no-op.
func (service *Service) PrepareDNSSettlement(
	ctx context.Context,
	work workqueue.View,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
	at time.Time,
) (etcd.BlueprintDNSPreparation, error) {
	if service == nil || ctx == nil {
		return etcd.BlueprintDNSPreparation{}, errs.New(errs.KindInternal, "Blueprint DNS settler is not configured")
	}
	settler, ok := service.repository.(authoredDNSSettlementRepository)
	if !ok {
		return etcd.BlueprintDNSPreparation{}, errs.New(errs.KindInternal, "Blueprint DNS settlement repository is not configured")
	}
	return settler.PrepareBlueprintDNS(ctx, work, unit, snapshot, at)
}
