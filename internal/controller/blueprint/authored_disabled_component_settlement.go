package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
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

// TrySettleDisabledComponent acknowledges only an explicitly authored
// singleton whose normalized disabled configuration already matches current
// state. Any mutation or possible prior effect remains on the protected Agent
// Component path.
func (service *Service) TrySettleDisabledComponent(
	ctx context.Context,
	parent etcd.TaskRecord,
	unit blueprintunits.Unit,
	snapshot blueprintunits.Snapshot,
) (bool, error) {
	if service == nil || ctx == nil || unit.Target.Kind != ids.KindComponent || unit.Removal ||
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
	if !found {
		return false, errs.New(errs.KindStateConflict, "Blueprint Component is absent from the sealed input")
	}
	if spec.Enabled || authoredRuntimeComponentEffectPossible(snapshot, unit.Target.ID) {
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
	if len(changes.Candidates) != 0 || len(changes.Effective) != 1 {
		return false, nil
	}
	effective, err := componentrecord.NewRecord(changes.Effective[0])
	if err != nil {
		return false, err
	}
	if !componentrecord.EqualRecord(effective, record) {
		return false, nil
	}
	var identityFound bool
	selection := etcd.BlueprintDisabledComponentSelection{
		Capability: capability, Spec: spec, Component: record,
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
