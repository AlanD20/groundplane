package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type disabledComponentPreparation struct {
	Selection etcd.BlueprintDisabledComponentSelection
	Proxy     *componentplanning.ProxyServicePreflight
	Sources   workqueue.Change
}

// Preparation returns metadata and source-retention writes, not an independent
// commit. The Component owner must join these with native work settlement.
func (service *Service) prepareDisabledComponentSelection(
	ctx context.Context,
	work workqueue.View,
	services []workqueue.View,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
) (*disabledComponentPreparation, error) {
	parent := work.Task.Task
	item := work.Item.Item
	if service == nil || ctx == nil || unit.Target.Kind != ids.KindComponent ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.ReadRevision != snapshot.ReadRevision ||
		work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		item.Validate() != nil || item.TaskID != parent.ID || item.TargetID != unit.Target.ID ||
		item.Operation != workexecution.Component || item.Placement.Executor != workexecution.Controller ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.ForwardRevoked ||
		(item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		blueprintunits.ValidateUnit(unit) != nil ||
		validateAuthoredRuntimeProjectionParent(parent, snapshot) != nil {
		return nil, errs.New(errs.KindValidationFailed, "Blueprint disabled Component request is invalid")
	}
	selected, err := service.selectComponentWork(ctx, work, unit, snapshot)
	if err != nil {
		return nil, err
	}
	source, capability, spec := selected.Source, selected.Capability, selected.Spec
	current := &selected.Current
	record := current.Component.Record
	if !unit.Removal && spec.Enabled {
		return nil, nil
	}
	if !authoredDisabledComponentAcknowledged(snapshot, *current) {
		return nil, nil
	}
	if record.Desired.Enabled || len(record.Runtime.GeneratedServices) != 0 ||
		record.Runtime.PinnedIPv4 != "" || record.Runtime.Healthy {
		return nil, nil
	}
	effective, err := componentrecord.NewRecord(selected.Candidate)
	if err != nil {
		return nil, err
	}
	if unit.Removal && (effective.Desired.Enabled || len(effective.Runtime.GeneratedServices) != 0 ||
		effective.Runtime.PinnedIPv4 != "" || !core.EqualComponentConfig(effective.Desired.Config, core.ComponentConfig{})) {
		return nil, errs.New(errs.KindStateConflict, "omitted Component did not resolve to disabled absence")
	}
	var identityFound bool
	selection := etcd.BlueprintDisabledComponentSelection{
		Capability: capability, Spec: spec, Component: record,
	}
	if unit.Removal {
		selection.Identity = projectionrecord.OwnedIdentity{ID: unit.Target.ID, Name: string(capability)}
		identityFound = true
	}
	for _, identity := range source.identities.Record.Components {
		if identity.ID == unit.Target.ID && identity.Name == string(capability) {
			selection.Identity = identity
			identityFound = true
			break
		}
	}
	if !identityFound {
		return nil, errs.New(errs.KindStateConflict, "Blueprint Component identity changed")
	}
	result := &disabledComponentPreparation{Selection: selection}
	if record.Desired.Kind != spec.Implementation {
		result.Selection.ProxyAuthority, err = service.prepareDisabledProxyServiceCapture(
			ctx,
			parent,
			snapshot,
			source,
			*current,
			spec.Implementation,
		)
		if err != nil {
			return nil, err
		}
		result.Proxy, result.Sources, err = service.prepareComponentProxyRollouts(
			ctx,
			work,
			services,
			snapshot,
			source,
			effective,
			result.Selection.ProxyAuthority,
		)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}

func authoredDisabledComponentAcknowledged(
	snapshot blueprintunits.Snapshot,
	authority etcd.BlueprintRuntimeComponent,
) bool {
	if authority.Component.Record.Desired.Enabled {
		return false
	}
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.ID != authority.Component.Record.Desired.ID ||
			applied.Record.Target.Kind != ids.KindComponent {
			continue
		}
		if applied.Record.State == blueprintunits.Absent && applied.Record.ControllerOperationID == "" {
			return !authority.RuntimePresent || !authority.Runtime.Record.Component.Desired.Enabled
		}
		return (applied.Record.State == blueprintunits.Absent || applied.Record.State == blueprintunits.Applied) &&
			authoredRuntimeComponentReceiptMatches(applied, authority)
	}
	return false
}
