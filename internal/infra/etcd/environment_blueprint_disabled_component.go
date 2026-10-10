package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintDisabledComponentSelection is the exact pure reconciliation result
// for one explicitly authored disabled Component. Persistence rechecks it
// against the immutable authored records and current singleton before receipt.
type BlueprintDisabledComponentSelection struct {
	Capability core.ComponentCapability
	Spec       core.ComponentSpec
	Identity   projectionrecord.OwnedIdentity
	Component  componentrecord.Record
}

// SettleBlueprintDisabledComponent acknowledges an explicitly authored
// disabled singleton without inventing an Agent Task or runtime identity. It
// accepts only prior proven absence and fences runtime presence or absence.
func (repository *EnvironmentBlueprintRepository) SettleBlueprintDisabledComponent(
	ctx context.Context,
	parent TaskRecord,
	unit blueprintunits.Unit,
	selection BlueprintDisabledComponentSelection,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		unit.Target.Kind != ids.KindComponent || unit.Removal ||
		selection.Identity.ID != unit.Target.ID || selection.Identity.Name != string(selection.Capability) ||
		selection.Spec.Enabled || componentrecord.ValidateRecord(selection.Component) != nil ||
		selection.Component.Desired.ID != unit.Target.ID ||
		selection.Component.Desired.Owner != core.ComponentOwnerEnvironment ||
		selection.Component.Desired.OwnerID != parent.Owner.EnvironmentID ||
		selection.Component.Desired.Kind != selection.Spec.Implementation ||
		selection.Component.Desired.Enabled || len(selection.Component.Runtime.GeneratedServices) != 0 ||
		selection.Component.Runtime.PinnedIPv4 != "" || selection.Component.Runtime.Healthy {
		return errs.New(errs.KindValidationFailed, "Blueprint disabled Component settlement identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return err
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != parent.ID {
		return errs.New(errs.KindStateConflict, "Blueprint disabled Component parent is no longer current")
	}
	if !blueprintDisabledComponentPriorAbsence(snapshot, unit.Target) {
		return errs.New(errs.KindStateConflict, "Blueprint Component may retain an earlier effect")
	}
	parentKey := taskjournal.TaskStorageKey(parent.ID)
	claimKey := taskjournal.BlueprintParentClaimKey(parent.ID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(parent.Owner.EnvironmentID, parent.ID)
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(parent.Owner.EnvironmentID, parent.ID)
	componentKey := componentrecord.RecordKey(unit.Target.ID)
	activeKey := environmentchanges.ComponentTaskActiveEnvironmentKey(parent.Owner.EnvironmentID)
	keys := []string{parentKey, claimKey, rootKey, identitiesKey, componentKey, activeKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: keys, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint disabled Component evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return errs.New(errs.KindInternal, "Blueprint disabled Component evidence is corrupt")
		}
	}
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[3] == nil || read.Values[4] == nil || read.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint disabled Component authority is unavailable")
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	if parentErr != nil || claimErr != nil || claimID != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil || storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner ||
		storedParent.PlanID != parent.PlanID || storedParent.PlanHash != parent.PlanHash ||
		storedParent.RenderGeneration != parent.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint disabled Component parent claim changed")
	}
	desired, found, err := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, parent.Owner.EnvironmentID, parent.ID, snapshot.ReadRevision,
	)
	if err != nil {
		return err
	}
	owned, err := projectionrecord.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if err != nil {
		return err
	}
	current, err := componentrecord.DecodeRecord(read.Values[4].Value)
	if err != nil {
		return err
	}
	if !found || desired.Revision != read.Values[2].ModRevision ||
		desired.Record.EnvironmentID != parent.Owner.EnvironmentID || desired.Record.RevisionID != parent.ID ||
		desired.Record.RenderGeneration != uint64(parent.RenderGeneration) ||
		owned.EnvironmentID != parent.Owner.EnvironmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != desired.Record.RenderGeneration ||
		!blueprintDisabledComponentSelectionMatches(desired.Record, owned, selection) ||
		!componentrecord.EqualRecord(current, selection.Component) {
		return errs.New(errs.KindStateConflict, "Blueprint disabled Component selection changed")
	}
	runtime, runtimePresent, err := componentplanning.ReadRuntime(
		ctx, repository.store, unit.Target.ID, snapshot.ReadRevision,
	)
	if err != nil {
		return err
	}
	if runtime.ReadRevision != snapshot.ReadRevision {
		return errs.New(errs.KindStateConflict, "Blueprint disabled Component runtime snapshot changed")
	}
	if runtimePresent {
		acknowledged := runtime.Record.Component
		acknowledged.Runtime.Healthy = current.Runtime.Healthy
		if runtime.Record.EnvironmentID != parent.Owner.EnvironmentID ||
			!componentrecord.EqualRecord(acknowledged, current) {
			return errs.New(errs.KindStateConflict, "Blueprint disabled Component runtime changed")
		}
	}
	runtimeRevision := int64(0)
	if runtimePresent {
		runtimeRevision = runtime.Revision
	}
	receipt, err := blueprintunits.PrepareControllerDisabledComponentSettlement(
		snapshot, unit, ids.New(ids.KindOperation), read.Values[4].ModRevision, runtimeRevision,
	)
	if err != nil {
		return err
	}
	defer receipt.Clear()
	conditions := append(receipt.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)},
		keyvalue.Condition{Key: rootKey, ModRevision: read.Values[2].ModRevision},
		keyvalue.Condition{Key: identitiesKey, ModRevision: read.Values[3].ModRevision},
		keyvalue.Condition{Key: componentKey, ModRevision: read.Values[4].ModRevision},
		keyvalue.Condition{Key: activeKey},
	)
	runtimeCondition := keyvalue.Condition{Key: componentplanning.RuntimeKey(unit.Target.ID)}
	if runtimePresent {
		runtimeCondition.ModRevision = runtime.Revision
	}
	conditions = append(conditions, runtimeCondition)
	result, err := repository.store.Transact(ctx, conditions, receipt.Mutations())
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint disabled Component settlement raced")
	}
	return nil
}

func blueprintDisabledComponentPriorAbsence(
	snapshot blueprintunits.Snapshot,
	target blueprintunits.ResourceKey,
) bool {
	for _, applied := range snapshot.Applied {
		if applied.Record.Target == target {
			return applied.Record.State == blueprintunits.Absent
		}
	}
	return false
}

func blueprintDisabledComponentSelectionMatches(
	desired projectionrecord.EnvironmentDesiredInput,
	owned projectionrecord.EnvironmentOwnedIdentities,
	selection BlueprintDisabledComponentSelection,
) bool {
	spec, found := desired.Input.Components[string(selection.Capability)]
	if !found || !equalBlueprintComponentSpec(spec, selection.Spec) {
		return false
	}
	for _, identity := range owned.Components {
		if identity == selection.Identity {
			return true
		}
	}
	return false
}

func equalBlueprintComponentSpec(left, right core.ComponentSpec) bool {
	return left.Implementation == right.Implementation && left.Enabled == right.Enabled &&
		left.Settings.Alias == right.Settings.Alias && left.Settings.SecretID == right.Settings.SecretID &&
		(left.Settings.ZoneIDs == nil) == (right.Settings.ZoneIDs == nil) &&
		slices.Equal(left.Settings.ZoneIDs, right.Settings.ZoneIDs) &&
		left.ImplementationConfig.CaddyfileTemplate == right.ImplementationConfig.CaddyfileTemplate
}
