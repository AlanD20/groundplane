package blueprintcoordinator

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ServiceRemovalReader interface {
	GetServiceRemovalRuntime(context.Context, string, string, int64) (keyvalue.Versioned[serviceruntimerecord.Record], error)
	GetServiceRemovalPredecessor(context.Context, string, int64) (workqueue.ClaimSnapshot, workexecution.Placement, error)
}

func (planner *ServicePlanner) prepareRemoval(ctx context.Context, task workexecution.Task, unit blueprintunits.Unit, baseline blueprintunits.Snapshot) (PreparedUnit, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply || task.PublicationComplete ||
		task.Scope.EnvironmentID != baseline.EnvironmentID || baseline.ReadRevision <= 0 ||
		blueprintunits.ValidateUnit(unit) != nil || unit.Target.Kind != ids.KindService || !unit.Removal ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return PreparedUnit{}, errs.New(errs.KindValidationFailed, "Blueprint Service removal requires one captured target")
	}
	runtime, err := planner.removals.GetServiceRemovalRuntime(ctx, task.Scope.EnvironmentID, unit.Target.ID, baseline.ReadRevision)
	if err != nil {
		return PreparedUnit{}, err
	}
	if runtime.ReadRevision != baseline.ReadRevision || runtime.Revision < 0 || runtime.Revision > baseline.ReadRevision {
		return PreparedUnit{}, errs.New(errs.KindStateConflict, "Blueprint Service runtime differs from the planning snapshot")
	}
	selected := PreparedUnit{Unit: unit, Operation: workexecution.ServiceRemoval,
		Placement:     workexecution.Placement{Executor: workexecution.Controller},
		RemovalSource: &blueprintunits.ServiceRemovalSource{ServiceID: unit.Target.ID, RuntimeRevision: runtime.Revision}}
	claim, predecessorPlacement, err := planner.removals.GetServiceRemovalPredecessor(ctx, unit.Target.ID, baseline.ReadRevision)
	if err != nil {
		return PreparedUnit{}, err
	}
	selected.RemovalSource.ClaimRevision = claim.Revision
	selected.RemovalSource.Predecessor = claim.Owner
	if claim.Owner != nil && predecessorPlacement.Executor == workexecution.Agent {
		selected.Placement = predecessorPlacement
		selected.EffectClaims = []workexecution.Claim{{ScopeID: task.Scope.EnvironmentID}, {HostID: predecessorPlacement.HostID, ScopeID: task.Scope.EnvironmentID}}
	}
	if runtime.Revision == 0 {
		// Missing runtime is insufficient after unknown effects. Initial absence
		// is positive birth evidence, rechecked when the input is published.
		if _, err := blueprintunits.RequireInitialServiceAbsence(baseline, unit.Target.ID); err != nil && claim.Owner == nil {
			return PreparedUnit{}, err
		}
		return selected, nil
	}
	if serviceruntimerecord.Validate(runtime.Record) != nil || runtime.Record.EnvironmentID != baseline.EnvironmentID || runtime.Record.Runtime.ServiceID != unit.Target.ID {
		return PreparedUnit{}, errs.New(errs.KindStateConflict, "Blueprint Service removal runtime belongs to another resource")
	}
	selected.RemovalSource.RuntimeSHA256, err = serviceruntimerecord.Fingerprint(runtime.Record)
	if err != nil {
		return PreparedUnit{}, err
	}
	hostID := runtime.Record.Source.Executor.HostID
	if claim.Owner != nil && predecessorPlacement.Executor == workexecution.Agent && predecessorPlacement.HostID != hostID {
		return PreparedUnit{}, errs.New(errs.KindStateConflict, "Service removal predecessor belongs to a different host")
	}
	selected.Placement = workexecution.Placement{Executor: workexecution.Agent, HostID: hostID}
	selected.EffectClaims = []workexecution.Claim{{ScopeID: task.Scope.EnvironmentID}, {HostID: hostID, ScopeID: task.Scope.EnvironmentID}}
	return selected, nil
}
