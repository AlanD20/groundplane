package blueprintcoordinator

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hostbindings"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// VolumePlanner selects a host for captured storage intent. Executable bytes
// are prepared later under the published graph; admission does not execute a
// helper or use a historical child Task to infer whether storage exists.
type VolumePlanner struct {
	hosts hostbindings.Reader
}

func NewVolumePlanner(hosts hostbindings.Reader) (*VolumePlanner, error) {
	if hosts == nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Volume planner requires host ownership storage")
	}
	return &VolumePlanner{hosts: hosts}, nil
}

func (planner *VolumePlanner) PrepareBlueprintWork(ctx context.Context, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot,
) (PreparedUnit, error) {
	if task.Validate() != nil || task.Action != workexecution.ActionApply || task.PublicationComplete ||
		task.Scope.EnvironmentID != baseline.EnvironmentID || baseline.ReadRevision <= 0 ||
		blueprintunits.ValidateUnit(unit) != nil || unit.Target.Kind != ids.KindVolume ||
		len(unit.Writes) != 1 || unit.Writes[0] != unit.Target {
		return PreparedUnit{}, errs.New(errs.KindValidationFailed, "Blueprint Volume admission requires captured storage intent")
	}
	if unit.Removal {
		return PreparedUnit{}, errs.New(errs.KindResourceInUse, "remove the persistent Volume separately before applying its omission")
	}
	host, err := hostbindings.ReadLocal(ctx, planner.hosts, baseline.ReadRevision)
	if err != nil {
		return PreparedUnit{}, err
	}
	return PreparedUnit{Unit: unit, Operation: workexecution.Volume,
		Placement:    workexecution.Placement{Executor: workexecution.Agent, HostID: host.Binding.HostID},
		EffectClaims: []workexecution.Claim{{HostID: host.Binding.HostID, ScopeID: unit.Target.ID}},
	}, nil
}
