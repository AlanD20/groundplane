package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type authoredDisabledComponentSettlementRepository interface {
	SettleBlueprintDisabledComponent(
		context.Context,
		etcd.TaskRecord,
		blueprintunits.Unit,
		etcd.BlueprintDisabledComponentSelection,
	) error
}

// TrySettleDisabledComponent settles disabled singleton settings or omission
// only when exact retained authority proves no workload remains. Metadata edits
// have no Agent effect; other cases use protected Component execution.
func (service *Service) TrySettleDisabledComponent(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
) (bool, error) {
	if service == nil || ctx == nil || unit.Target.Kind != ids.KindComponent ||
		validateAuthoredRuntimeProjectionParent(parent, snapshot) != nil {
		return false, errs.New(errs.KindValidationFailed, "Blueprint disabled Component request is invalid")
	}
	settler, ok := service.repository.(authoredDisabledComponentSettlementRepository)
	if !ok {
		return false, errs.New(errs.KindInternal, "Blueprint disabled Component repository is not configured")
	}
	source, err := service.loadAuthoredRuntimeProjectionSource(ctx, parent)
	if err != nil {
		return false, err
	}
	capability, spec, found := authoredComponentSpec(
		source.identities.Record.Components, source.desired.Record.Input.Components, unit.Target.ID,
	)
	if !found && !unit.Removal || found && unit.Removal {
		return false, errs.New(errs.KindStateConflict, "Blueprint Component is absent from the sealed input")
	}
	if !unit.Removal && spec.Enabled {
		return false, nil
	}
	authorities, err := service.repository.ReadEnvironmentBlueprintRuntimeComponents(
		ctx, parent.Owner.EnvironmentID, snapshot.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	var current *etcd.BlueprintRuntimeComponent
	for index := range authorities {
		if authorities[index].Component.Record.Desired.ID == unit.Target.ID {
			current = &authorities[index]
			break
		}
	}
	if current == nil {
		return false, errs.New(errs.KindStateConflict, "Blueprint Component singleton is unavailable")
	}
	record := current.Component.Record
	if unit.Removal {
		capability, spec, err = omittedComponentSelection(record)
		if err != nil {
			return false, err
		}
	}
	if !authoredDisabledComponentAcknowledged(snapshot, *current) {
		return false, nil
	}
	if record.Desired.Enabled || len(record.Runtime.GeneratedServices) != 0 ||
		record.Runtime.PinnedIPv4 != "" || record.Runtime.Healthy {
		return false, nil
	}
	projected, err := componentrecord.ProjectRecord(record)
	if err != nil {
		return false, err
	}
	changes, err := taskplanning.ReconcileBlueprintComponent(
		capability, spec, projected, func(ids.Kind) string { return "" },
	)
	if err != nil {
		return false, err
	}
	if len(changes.Effective) != 1 {
		return false, nil
	}
	effective, err := componentrecord.NewRecord(changes.Effective[0])
	if err != nil {
		return false, err
	}
	if unit.Removal && (effective.Desired.Enabled || len(effective.Runtime.GeneratedServices) != 0 ||
		effective.Runtime.PinnedIPv4 != "" || !core.EqualComponentConfig(effective.Desired.Config, core.ComponentConfig{})) {
		return false, errs.New(errs.KindStateConflict, "omitted Component did not resolve to disabled absence")
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
		return false, errs.New(errs.KindStateConflict, "Blueprint Component identity changed")
	}
	if err := settler.SettleBlueprintDisabledComponent(ctx, parent, unit, selection); err != nil {
		return false, err
	}
	return true, nil
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
