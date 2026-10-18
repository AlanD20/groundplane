package etcd

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/serviceproxy"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintDisabledComponentSelection is the exact pure reconciliation result
// for one disabled Component, including an omitted singleton. Persistence rechecks it
// against the immutable authored records and current singleton before receipt.
type BlueprintDisabledComponentSelection struct {
	Capability     core.ComponentCapability
	Spec           core.ComponentSpec
	Identity       projectionrecord.OwnedIdentity
	Component      componentrecord.Record
	ProxyAuthority componentplanning.FrozenProjectionAuthority
}

// PrepareBlueprintDisabledComponent returns disabled singleton settings with
// their Controller receipt. Exact retained absence proof is required; uncertain
// host effects never authorize this metadata-only operation.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintDisabledComponent(
	ctx context.Context,
	work workqueue.View,
	unit blueprintunits.Unit,
	selection BlueprintDisabledComponentSelection,
	preflight *componentplanning.ProxyServicePreflight,
	sourceChange workqueue.Change,
	at time.Time,
) (workqueue.Change, error) {
	parent, item := work.Task.Task, work.Item.Item
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		parent.Validate() != nil || item.Validate() != nil || parent.Action != workexecution.ActionApply ||
		!parent.PublicationComplete || parent.Stop != nil || parent.FinishedAt != nil || parent.CollectionStartedAt != nil ||
		work.Task.Revision <= 0 || work.Item.Revision <= 0 || work.ReadRevision < work.Task.Revision || work.ReadRevision < work.Item.Revision ||
		item.TaskID != parent.ID || item.TargetID != unit.Target.ID || item.Operation != workexecution.Component ||
		item.Placement.Executor != workexecution.Controller || item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.ForwardRevoked || (item.Phase != workexecution.Waiting && item.Phase != workexecution.Ready) ||
		unit.Target.Kind != ids.KindComponent ||
		selection.Identity.ID != unit.Target.ID || selection.Identity.Name != string(selection.Capability) ||
		selection.Spec.Enabled || componentrecord.ValidateRecord(selection.Component) != nil ||
		selection.Component.Desired.ID != unit.Target.ID ||
		selection.Component.Desired.Owner != core.ComponentOwnerEnvironment ||
		selection.Component.Desired.OwnerID != parent.Scope.EnvironmentID ||
		!selection.Component.Desired.Kind.SharesEnvironmentCapability(selection.Spec.Implementation) ||
		selection.Spec.Validate(selection.Capability) != nil ||
		selection.Component.Desired.Enabled || len(selection.Component.Runtime.GeneratedServices) != 0 ||
		selection.Component.Runtime.PinnedIPv4 != "" || selection.Component.Runtime.Healthy {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint disabled Component settlement identity is invalid")
	}
	if selection.Capability != core.ComponentCapabilityHTTPRouter && len(unit.Writes) != 1 {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "only the router capability may claim Service forwarding resources")
	}
	engineChange := selection.Component.Desired.Kind != selection.Spec.Implementation
	if engineChange != (preflight != nil) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "disabled proxy selection requires the exact affected-Service capture")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, parent.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint disabled Component parent is no longer current")
	}
	parentKey := workqueue.TaskKey(parent.ID)
	itemKey := workqueue.ItemKey(item.ID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(parent.Scope.EnvironmentID, parent.ID)
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(parent.Scope.EnvironmentID, parent.ID)
	componentKey := componentrecord.RecordKey(unit.Target.ID)
	keys := []string{parentKey, itemKey, rootKey, identitiesKey, componentKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: keys, Revision: snapshot.ReadRevision,
	})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint disabled Component evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint disabled Component evidence is corrupt")
		}
	}
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil ||
		read.Values[3] == nil || read.Values[4] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint disabled Component authority is unavailable")
	}
	if read.Values[0].ModRevision != work.Task.Revision || read.Values[1].ModRevision != work.Item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Component work changed")
	}
	desired, found, err := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, parent.Scope.EnvironmentID, parent.ID, snapshot.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	owned, err := projectionrecord.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	current, err := componentrecord.DecodeRecord(read.Values[4].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || desired.Revision != read.Values[2].ModRevision ||
		desired.Record.EnvironmentID != parent.Scope.EnvironmentID || desired.Record.RevisionID != parent.ID ||
		owned.EnvironmentID != parent.Scope.EnvironmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != desired.Record.RenderGeneration ||
		!blueprintDisabledComponentSelectionMatches(desired.Record, owned, selection, unit.Removal) ||
		!componentrecord.EqualRecord(current, selection.Component) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint disabled Component selection changed")
	}
	runtime, runtimePresent, err := componentplanning.ReadRuntime(
		ctx, repository.store, unit.Target.ID, snapshot.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if runtime.ReadRevision != snapshot.ReadRevision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint disabled Component runtime snapshot changed")
	}
	if runtimePresent {
		acknowledged := runtime.Record.Component
		acknowledged.Runtime.Healthy = current.Runtime.Healthy
		if runtime.Record.EnvironmentID != parent.Scope.EnvironmentID ||
			!componentrecord.EqualRecord(acknowledged, current) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint disabled Component runtime changed")
		}
	}
	if !blueprintDisabledComponentAbsenceProven(
		snapshot,
		unit.Target,
		current,
		read.Values[4].ModRevision,
		runtime,
		runtimePresent,
	) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "disabled Component may retain an unacknowledged effect")
	}
	candidate := current
	candidate.Desired.Kind = selection.Spec.Implementation
	candidate.Desired.Config = core.ComponentConfigForSpec(selection.Spec)
	if err := componentrecord.ValidateRecord(candidate); err != nil {
		return workqueue.Change{}, err
	}
	retireRuntime := runtimePresent &&
		(unit.Removal || current.Desired.Kind != candidate.Desired.Kind ||
			!core.EqualComponentConfig(current.Desired.Config, candidate.Desired.Config))
	runtimeRevision := int64(0)
	if runtimePresent && !retireRuntime {
		runtimeRevision = runtime.Revision
	}
	operationID, err := componentplanning.ControllerProxyOperationID(parent, item)
	if err != nil {
		return workqueue.Change{}, err
	}
	runtimeCondition := keyvalue.Condition{Key: componentplanning.RuntimeKey(unit.Target.ID)}
	if runtimePresent {
		runtimeCondition.ModRevision = runtime.Revision
	}
	proxyPublication := componentplanning.ControllerProxyPublication{}
	if engineChange {
		proxyPublication, err = componentplanning.PrepareControllerProxySelection(ctx, repository.store,
			work, *preflight, sourceChange, runtimeCondition, snapshot.HeadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer proxyPublication.Clear()
		if err := proxyPublication.Validate(parent.Scope.EnvironmentID, parent.ID, unit.Target.ID,
			serviceproxy.Implementation(candidate.Desired.Kind)); err != nil {
			return workqueue.Change{}, err
		}
	}
	receipt, err := blueprintunits.PrepareControllerDisabledComponentSettlement(
		snapshot, unit, operationID, read.Values[4].ModRevision, runtimeRevision, engineChange,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer receipt.Clear()
	conditions := append(receipt.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: itemKey, ModRevision: read.Values[1].ModRevision},
		keyvalue.Condition{Key: rootKey, ModRevision: read.Values[2].ModRevision},
		keyvalue.Condition{Key: identitiesKey, ModRevision: read.Values[3].ModRevision},
		keyvalue.Condition{Key: componentKey, ModRevision: read.Values[4].ModRevision},
	)
	conditions = append(conditions, runtimeCondition)
	conditions = append(conditions, proxyPublication.Conditions()...)
	secretConditions, secretMutations, err := componentplanning.PrepareDisabledComponentSecretReplacement(
		ctx, repository.store, parent.Scope.ProjectID, current, candidate, snapshot.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(secretMutations)
	conditions = append(conditions, secretConditions...)
	value, err := componentrecord.EncodeRecord(candidate)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(value)
	mutations := append(receipt.Mutations(), secretMutations...)
	mutations = append(mutations, proxyPublication.Mutations()...)
	mutations = append(
		mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: componentKey, Value: value},
		componentrecord.WriteFenceMutation(parent.ID),
	)
	// Rewrite even unchanged metadata: its publication revision must equal the
	// receipt revision. Retire only the proven-empty runtime when its metadata
	// no longer matches; no Agent acknowledgement is fabricated.
	if retireRuntime {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: runtimeCondition.Key})
	}
	// Prepared owners clear their buffers on return; the caller owns the
	// returned bytes and joins them with the native work's terminal change.
	mutations = slices.Clone(mutations)
	for index := range mutations {
		mutations[index].Value = slices.Clone(mutations[index].Value)
	}
	settled, err := blueprintunits.PrepareComponentDefinitionSettlement(work, unit,
		read.Values[4].ModRevision, runtimeRevision, at)
	if err != nil {
		keyvalue.ClearMutationValues(mutations)
		return workqueue.Change{}, err
	}
	settled.Conditions = append(settled.Conditions, conditions...)
	settled.Mutations = append(settled.Mutations, mutations...)
	return settled, nil
}

func blueprintDisabledComponentSelectionMatches(
	desired projectionrecord.EnvironmentDesiredInput,
	owned projectionrecord.EnvironmentOwnedIdentities,
	selection BlueprintDisabledComponentSelection,
	removal bool,
) bool {
	spec, found := desired.Input.Components[string(selection.Capability)]
	if removal {
		if found ||
			!equalBlueprintComponentSpec(
				selection.Spec,
				core.ComponentSpec{Implementation: selection.Component.Desired.Kind},
			) {
			return false
		}
		for _, identity := range owned.Components {
			if identity.ID == selection.Identity.ID || identity.Name == string(selection.Capability) {
				return false
			}
		}
		return true
	}
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

func blueprintDisabledComponentAbsenceProven(
	snapshot blueprintunits.Snapshot,
	target blueprintunits.ResourceKey,
	current componentrecord.Record,
	componentRevision int64,
	runtime keyvalue.Versioned[componentplanning.Runtime],
	runtimePresent bool,
) bool {
	for _, applied := range snapshot.Applied {
		receipt := applied.Record
		if receipt.Target != target {
			continue
		}
		if current.Desired.Enabled ||
			receipt.State != blueprintunits.Applied && receipt.State != blueprintunits.Absent {
			return false
		}
		if receipt.ControllerOperationID != "" {
			return blueprintunits.ControllerDisabledComponentPublicationMatches(
				applied,
				componentRevision,
				runtime.Revision,
			)
		}
		if receipt.State == blueprintunits.Absent {
			return !runtimePresent || !runtime.Record.Component.Desired.Enabled
		}
		return runtimePresent && receipt.Execution != nil && *receipt.Execution == runtime.Record.Execution &&
			receipt.ParentTaskID == runtime.Record.RevisionID
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
