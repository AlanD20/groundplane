package blueprintcoordinator

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type ServicePlanner struct{ hosts hostbindings.Reader }
type NetworkPlanner struct{ hosts hostbindings.Reader }
type AttachPlanner struct{ hosts hostbindings.Reader }
type ComponentPlanner struct{ hosts hostbindings.Reader }

func NewServicePlanner(hosts hostbindings.Reader) (*ServicePlanner, error) {
	if hosts == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Service planner requires host ownership storage")
	}
	return &ServicePlanner{hosts: hosts}, nil
}

func NewNetworkPlanner(hosts hostbindings.Reader) (*NetworkPlanner, error) {
	if hosts == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Network planner requires host ownership storage")
	}
	return &NetworkPlanner{hosts: hosts}, nil
}

func NewAttachPlanner(hosts hostbindings.Reader) (*AttachPlanner, error) {
	if hosts == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Attach planner requires host ownership storage")
	}
	return &AttachPlanner{hosts: hosts}, nil
}

func NewComponentPlanner(hosts hostbindings.Reader) (*ComponentPlanner, error) {
	if hosts == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Component planner requires host ownership storage")
	}
	return &ComponentPlanner{hosts: hosts}, nil
}

// A Service's deployment strategy belongs to its captured Release input, not
// admission. Removal instead selects the typed resource-removal operation.
func (planner *ServicePlanner) PrepareBlueprintWork(ctx context.Context, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot,
) (PreparedUnit, error) {
	operation := workexecution.Release
	if unit.Removal {
		operation = workexecution.Hierarchy
	}
	return prepareHostResource(ctx, planner.hosts, task, unit, baseline, ids.KindService, operation)
}

func (planner *NetworkPlanner) PrepareBlueprintWork(ctx context.Context, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot,
) (PreparedUnit, error) {
	return prepareHostResource(ctx, planner.hosts, task, unit, baseline, ids.KindNetwork, workexecution.Network)
}

func (planner *AttachPlanner) PrepareBlueprintWork(ctx context.Context, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot,
) (PreparedUnit, error) {
	operation := workexecution.Attach
	if unit.Removal {
		operation = workexecution.Detach
	}
	return prepareHostResource(ctx, planner.hosts, task, unit, baseline, ids.KindAttach, operation)
}

func (planner *ComponentPlanner) PrepareBlueprintWork(ctx context.Context, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot,
) (PreparedUnit, error) {
	return prepareHostResource(ctx, planner.hosts, task, unit, baseline, ids.KindComponent, workexecution.Component)
}

func prepareHostResource(ctx context.Context, hosts hostbindings.Reader, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot, kind ids.Kind,
	operation workexecution.Operation,
) (PreparedUnit, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply || task.PublicationComplete ||
		task.Scope.EnvironmentID != baseline.EnvironmentID || baseline.ReadRevision <= 0 ||
		blueprintunits.ValidateUnit(unit) != nil || unit.Target.Kind != kind {
		return PreparedUnit{}, errs.New(errs.KindValidationFailed, "Blueprint resource admission requires captured intent and ownership")
	}
	host, err := hostbindings.ReadLocal(ctx, hosts, baseline.ReadRevision)
	if err != nil {
		return PreparedUnit{}, err
	}
	selected := PreparedUnit{Unit: unit, Operation: operation,
		Placement: workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID}}
	for _, target := range unit.Writes {
		selected.EffectClaims = append(selected.EffectClaims,
			workexecution.Claim{HostID: host.Binding.HostID, ScopeID: target.ID})
	}
	// These operations can publish the shared Environment Compose files.
	// Match direct-operation claims; a second resource identity must not
	// allow two executors to overwrite the same configuration directory.
	selected.EffectClaims = append(selected.EffectClaims,
		workexecution.Claim{ScopeID: task.Scope.EnvironmentID},
		workexecution.Claim{HostID: host.Binding.HostID, ScopeID: task.Scope.EnvironmentID})
	return selected, nil
}
