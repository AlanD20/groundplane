package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/components"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/routepersistence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// SettleBlueprintRouteOmission releases Route metadata only after the exact
// router unit has acknowledged withdrawal. Desired absence is not host proof.
func (repository *EnvironmentBlueprintRepository) SettleBlueprintRouteOmission(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		unit.Target.Kind != ids.KindRoute || !unit.Removal {
		return errs.New(errs.KindValidationFailed, "Blueprint Route omission identity is invalid")
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
		return errs.New(errs.KindStateConflict, "Blueprint Route omission has been superseded")
	}
	environmentID := parent.Owner.EnvironmentID
	keys := []string{
		taskjournal.TaskStorageKey(parent.ID), taskjournal.BlueprintParentClaimKey(parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
		taskjournal.TaskMaterializationWriterKey(environmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint Route omission snapshot is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys)+2)
	for index, value := range read.Values {
		if index < 4 && value == nil || value != nil && value.Key != keys[index] {
			return errs.New(errs.KindStateConflict, "Blueprint Route omission authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	if read.Values[4] != nil || read.Values[5] != nil {
		return errs.New(errs.KindResourceInUse, "Blueprint Route withdrawal overlaps an active configuration writer")
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	desired, found, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		environmentID,
		parent.ID,
		snapshot.ReadRevision,
	)
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if parentErr != nil || claimErr != nil || desiredErr != nil || !found || ownedErr != nil ||
		claimID != parent.ID || storedParent.ID != parent.ID || storedParent.Owner != parent.Owner ||
		storedParent.PlanID != parent.PlanID || storedParent.PlanHash != parent.PlanHash ||
		storedParent.RenderGeneration != parent.RenderGeneration || storedParent.Status != taskjournal.TaskStatusRunning ||
		validateBlueprintParentClaimTask(storedParent) != nil || desired.Revision != read.Values[2].ModRevision ||
		desired.Record.EnvironmentID != environmentID || desired.Record.RevisionID != parent.ID ||
		desired.Record.RenderGeneration != uint64(parent.RenderGeneration) || owned.EnvironmentID != environmentID ||
		owned.RevisionID != parent.ID || owned.RenderGeneration != desired.Record.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint Route omission revision changed")
	}
	for _, identity := range owned.Routes {
		if identity.ID == unit.Target.ID {
			return errs.New(errs.KindStateConflict, "Blueprint Route is still declared")
		}
	}
	authorities, err := repository.ReadEnvironmentBlueprintRuntimeComponents(ctx, environmentID, snapshot.ReadRevision)
	if err != nil {
		return err
	}
	var router *BlueprintRuntimeComponent
	for index := range authorities {
		if authorities[index].Component.Record.Desired.Kind == core.ComponentKindIngressCaddy {
			router = &authorities[index]
		}
	}
	if router == nil || !blueprintRouteWithdrawalAcknowledged(snapshot, owned, *router) {
		return errs.New(errs.KindStateConflict, "router has not acknowledged the Blueprint without this Route")
	}
	conditions = append(
		conditions,
		keyvalue.Condition{
			Key:         components.RecordKey(router.Component.Record.Desired.ID),
			ModRevision: router.Component.Revision,
		},
		keyvalue.Condition{
			Key:         componentplanning.RuntimeKey(router.Component.Record.Desired.ID),
			ModRevision: router.Runtime.Revision,
		},
	)
	retirementConditions, mutations, err := routepersistence.PrepareRetirement(
		ctx,
		repository.store,
		environmentID,
		unit.Target.ID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return err
	}
	receipt, err := blueprintunits.PrepareControllerRouteSettlement(
		snapshot,
		unit,
		ids.New(ids.KindOperation),
		read.Values[2].ModRevision,
	)
	if err != nil {
		return err
	}
	defer receipt.Clear()
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, snapshot.ReadRevision)
	if err != nil {
		return err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return err
	}
	defer clear(epoch.Value)
	conditions = append(conditions, retirementConditions...)
	conditions = append(conditions, receipt.Conditions()...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions, keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)})
	mutations = append(mutations, receipt.Mutations()...)
	mutations = append(mutations, epoch)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Route withdrawal raced")
	}
	return nil
}

func blueprintRouteWithdrawalAcknowledged(
	snapshot blueprintunits.Snapshot,
	owned environmentprojection.EnvironmentOwnedIdentities,
	router BlueprintRuntimeComponent,
) bool {
	id := router.Component.Record.Desired.ID
	var applied *keyvalue.Versioned[blueprintunits.AppliedRecord]
	for index := range snapshot.Applied {
		if snapshot.Applied[index].Record.Target == (blueprintunits.ResourceKey{Kind: ids.KindComponent, ID: id}) {
			applied = &snapshot.Applied[index]
		}
	}
	var receipt *blueprintunits.AppliedRecord
	if applied != nil {
		receipt = &applied.Record
	}
	if !router.Component.Record.Desired.Enabled {
		if len(router.Component.Record.Runtime.GeneratedServices) != 0 ||
			router.Component.Record.Runtime.PinnedIPv4 != "" {
			return false
		}
		if receipt == nil {
			return !router.RuntimePresent
		}
		if receipt.State == blueprintunits.Absent {
			if receipt.ControllerOperationID != "" {
				return blueprintunits.ControllerDisabledComponentPublicationMatches(
					*applied,
					router.Component.Revision,
					router.Runtime.Revision,
				)
			}
			return !router.RuntimePresent || !router.Runtime.Record.Component.Desired.Enabled
		}
	}
	if receipt == nil || snapshot.Desired == nil || receipt.State != blueprintunits.Applied {
		return false
	}
	selected := false
	for _, identity := range owned.Components {
		selected = selected || identity.ID == id
	}
	if !selected {
		return false
	}
	unitMatches := false
	for _, candidate := range snapshot.Desired.Record.Units {
		unitMatches = unitMatches ||
			candidate.Target == receipt.Target && !candidate.Removal && candidate.Fingerprint == receipt.Fingerprint
	}
	if !unitMatches {
		return false
	}
	if receipt.ControllerOperationID != "" {
		return !router.Component.Record.Desired.Enabled &&
			blueprintunits.ControllerDisabledComponentPublicationMatches(
				*applied,
				router.Component.Revision,
				router.Runtime.Revision,
			)
	}
	return router.RuntimePresent && receipt.SourceTaskID == router.Runtime.Record.TaskID &&
		receipt.SourcePlanID == router.Runtime.Record.PlanID && receipt.ParentTaskID == router.Runtime.Record.RevisionID &&
		router.Runtime.Record.RevisionID == snapshot.HeadTaskID
}
