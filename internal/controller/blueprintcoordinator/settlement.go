package blueprintcoordinator

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type settlementDisposition uint8

const (
	settlementNeedsAgent settlementDisposition = iota
	settlementWaiting
	settlementComplete
)

// tryControllerSettlement distinguishes waiting from needing an Agent child.
// Persistence-only effects and their receipts remain one atomic publication;
// permanent failures use the parent's stop gate rather than retrying forever.
func (runner *Runner) tryControllerSettlement(
	ctx context.Context, parent etcd.TaskRecord, unit blueprintunits.Unit, snapshot blueprintunits.Snapshot,
) (settlementDisposition, error) {
	switch unit.Target.Kind {
	case ids.KindScript:
		settler, ok := runner.planner.(ScriptSettler)
		if !ok {
			return settlementNeedsAgent, errs.New(errs.KindInternal, "Blueprint Script settler is not configured")
		}
		return settlementComplete, settler.SettleScript(ctx, parent, unit)
	case ids.KindEnvEntry:
		if unit.Removal {
			settler, ok := runner.planner.(EntryOmissionSettler)
			if !ok {
				return settlementNeedsAgent, errs.New(
					errs.KindInternal,
					"Blueprint Entry omission settler is not configured",
				)
			}
			return controllerSettlementResult(settler.TrySettleEntryOmission(ctx, parent, unit))
		}
		settler, ok := runner.planner.(EntrySettler)
		if !ok {
			return settlementNeedsAgent, errs.New(errs.KindInternal, "Blueprint Entry settler is not configured")
		}
		return settlementComplete, settler.SettleEntry(ctx, parent, unit, snapshot)
	case ids.KindRoute:
		settler, ok := runner.planner.(RouteSettler)
		if !ok {
			return settlementNeedsAgent, errs.New(errs.KindInternal, "Blueprint Route settler is not configured")
		}
		if unit.Removal {
			return settlementComplete, settler.SettleRouteOmission(ctx, parent, unit)
		}
		return settlementComplete, settler.SettleRoute(ctx, parent, unit)
	case ids.KindDNSRecord:
		settler, ok := runner.planner.(DNSSettler)
		if !ok {
			return settlementWaiting, errs.New(errs.KindInternal, "Blueprint DNS settler is not configured")
		}
		settled, err := settler.TrySettleDNS(ctx, parent, unit)
		if settled {
			return settlementComplete, err
		}
		return settlementWaiting, err
	case ids.KindService:
		if unit.Removal {
			settler, ok := runner.planner.(ServiceOmissionSettler)
			if !ok {
				return settlementNeedsAgent, errs.New(
					errs.KindInternal,
					"Blueprint Service omission settler is not configured",
				)
			}
			return controllerSettlementResult(settler.TrySettleServiceOmission(ctx, parent, unit))
		}
		settler, ok := runner.planner.(ServiceConfigurationSettler)
		if !ok {
			return settlementNeedsAgent, errs.New(
				errs.KindInternal,
				"Blueprint Service configuration settler is not configured",
			)
		}
		return controllerSettlementResult(settler.TrySettleServiceConfiguration(ctx, parent, unit))
	case ids.KindComponent:
		settler, ok := runner.planner.(DisabledComponentSettler)
		if !ok {
			return settlementNeedsAgent, errs.New(
				errs.KindInternal,
				"Blueprint disabled Component settler is not configured",
			)
		}
		return controllerSettlementResult(settler.TrySettleDisabledComponent(ctx, parent, unit, snapshot))
	default:
		return settlementNeedsAgent, nil
	}
}

func controllerSettlementResult(settled bool, err error) (settlementDisposition, error) {
	if settled {
		return settlementComplete, err
	}
	return settlementNeedsAgent, err
}
