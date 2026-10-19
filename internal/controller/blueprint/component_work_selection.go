package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentWorkSelection struct {
	Source     authoredRuntimeProjectionSource
	Current    etcd.BlueprintRuntimeComponent
	Capability core.ComponentCapability
	Spec       core.ComponentSpec
	Candidate  core.Component
}

// selectComponentWork uses the accepted Apply input and a fixed runtime read.
// Both Controller-only metadata and Agent execution use this selection; neither
// can silently substitute a later desired Component definition.
func (service *Service) selectComponentWork(ctx context.Context, work workqueue.View,
	unit blueprintunits.Unit, snapshot blueprintunits.Snapshot,
) (componentWorkSelection, error) {
	task, item := work.Task.Task, work.Item.Item
	if item.Validate() != nil || item.TaskID != task.ID || item.Operation != workexecution.Component ||
		item.TargetID != unit.Target.ID || unit.Target.Kind != ids.KindComponent || blueprintunits.ValidateUnit(unit) != nil ||
		work.ReadRevision != snapshot.ReadRevision || work.Task.Revision <= 0 || work.Item.Revision <= 0 ||
		work.Task.Revision > work.ReadRevision || work.Item.Revision > work.ReadRevision ||
		item.AttemptNumber != 0 || item.ForwardRevoked || item.ActiveAttemptID != "" ||
		validateAuthoredRuntimeProjectionParent(task, snapshot) != nil {
		return componentWorkSelection{}, errs.New(
			errs.KindStateConflict,
			"Component selection requires its captured Apply work",
		)
	}
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, task)
	if err != nil {
		return componentWorkSelection{}, err
	}
	capability, spec, found := authoredComponentSpec(
		source.identities.Record.Components,
		source.desired.Record.Input.Components,
		unit.Target.ID,
	)
	if found == unit.Removal {
		return componentWorkSelection{}, errs.New(
			errs.KindStateConflict,
			"Component membership differs from the captured Apply",
		)
	}
	current, err := service.repository.ReadEnvironmentBlueprintRuntimeComponents(
		ctx,
		task.Scope.EnvironmentID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return componentWorkSelection{}, err
	}
	for _, selected := range current {
		if selected.Component.Record.Desired.ID != unit.Target.ID {
			continue
		}
		if selected.Component.Revision <= 0 || selected.Component.ReadRevision != snapshot.ReadRevision {
			return componentWorkSelection{}, errs.New(errs.KindStateConflict, "Component runtime snapshot changed")
		}
		if unit.Removal {
			capability, spec, err = omittedComponentSelection(selected.Component.Record)
			if err != nil {
				return componentWorkSelection{}, err
			}
		}
		active, err := componentrecord.ProjectRecord(selected.Component.Record)
		if err != nil {
			return componentWorkSelection{}, err
		}
		changes, err := taskplanning.ReconcileBlueprintComponent(capability, spec, active, func(kind ids.Kind) string {
			return ids.DeriveAt(kind, item.CreatedAt, item.ID, "component-candidate")
		})
		if err != nil {
			return componentWorkSelection{}, err
		}
		if len(changes.Effective) != 1 || changes.Effective[0].ID != item.TargetID {
			return componentWorkSelection{}, errs.New(
				errs.KindStateConflict,
				"Component selection must preserve its singleton identity",
			)
		}
		return componentWorkSelection{
			Source:     source,
			Current:    selected,
			Capability: capability,
			Spec:       spec,
			Candidate:  changes.Effective[0],
		}, nil
	}
	return componentWorkSelection{}, errs.New(errs.KindStateConflict, "Component singleton is unavailable")
}
