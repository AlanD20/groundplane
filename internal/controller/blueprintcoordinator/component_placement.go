package blueprintcoordinator

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Placement is captured once. Settlement rechecks absence and fences changes;
// a later runtime change cannot turn Controller metadata work into host effects.
func (planner *ComponentPlanner) metadataOnly(ctx context.Context, task workexecution.Task,
	unit blueprintunits.Unit, baseline blueprintunits.Snapshot, desired map[string]core.ComponentSpec,
) (bool, error) {
	key := componentrecord.RecordKey(unit.Target.ID)
	read, err := planner.hosts.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: baseline.ReadRevision},
	)
	if read != nil {
		defer keyvalue.ClearValues(read.Values)
	}
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != baseline.ReadRevision || len(read.Values) != 1 {
		return false, errs.New(errs.KindInternal, "Component placement snapshot is incomplete")
	}
	value := read.Values[0]
	if value == nil {
		return false, nil
	}
	if value.Key != key || value.ModRevision <= 0 {
		return false, errs.New(errs.KindInternal, "Component placement record is invalid")
	}
	current, err := componentrecord.DecodeRecord(value.Value)
	if err != nil {
		return false, err
	}
	if current.Desired.ID != unit.Target.ID || current.Desired.Owner != core.ComponentOwnerEnvironment ||
		current.Desired.OwnerID != task.Scope.EnvironmentID {
		return false, errs.New(errs.KindStateConflict, "Component placement belongs to another Environment")
	}
	capability, supported := current.Desired.Kind.EnvironmentCapability()
	if !supported {
		return false, errs.New(errs.KindStateConflict, "Component is not an Environment singleton")
	}
	spec, found := desired[string(capability)]
	if found == unit.Removal {
		return false, errs.New(errs.KindStateConflict, "Component placement differs from authored intent")
	}
	if found && spec.Enabled || current.Desired.Enabled || len(current.Runtime.GeneratedServices) != 0 ||
		current.Runtime.PinnedIPv4 != "" ||
		current.Runtime.Healthy {
		return false, nil
	}
	for _, execution := range baseline.Executions {
		if execution.Record.Unit.Target == unit.Target {
			return false, nil
		}
	}
	runtime, present, err := componentplanning.ReadRuntime(ctx, planner.hosts, unit.Target.ID, baseline.ReadRevision)
	if err != nil {
		return false, err
	}
	if present {
		acknowledged := runtime.Record.Component
		acknowledged.Runtime.Healthy = current.Runtime.Healthy
		if runtime.Record.EnvironmentID != task.Scope.EnvironmentID ||
			!componentrecord.EqualRecord(acknowledged, current) {
			return false, nil
		}
	}
	for _, applied := range baseline.Applied {
		receipt := applied.Record
		if receipt.Target != unit.Target {
			continue
		}
		if receipt.State != blueprintunits.Applied && receipt.State != blueprintunits.Absent {
			return false, nil
		}
		if receipt.ControllerOperationID != "" {
			return blueprintunits.ControllerDisabledComponentPublicationMatches(
				applied,
				value.ModRevision,
				runtime.Revision,
			), nil
		}
		if receipt.State == blueprintunits.Absent {
			return !present || !runtime.Record.Component.Desired.Enabled, nil
		}
		return present && receipt.Execution != nil && *receipt.Execution == runtime.Record.Execution &&
			receipt.ParentTaskID == runtime.Record.RevisionID, nil
	}
	return false, nil
}
