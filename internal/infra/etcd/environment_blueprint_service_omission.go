package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnsrecords"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalreferences"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TrySettleBlueprintServiceOmission handles only an unrendered declaration
// whose positive birth proof still owns absence. A missing projection alone
// is never sufficient: any executed or uncertain effect keeps Agent cleanup.
func (repository *EnvironmentBlueprintRepository) TrySettleBlueprintServiceOmission(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
) (bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		unit.Target.Kind != ids.KindService || !unit.Removal {
		return false, errs.New(errs.KindValidationFailed, "Blueprint Service omission identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return false, err
	}
	snapshot, err := ledger.Load(ctx, parent.Owner.EnvironmentID)
	if err != nil {
		return false, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission has been superseded")
	}
	environmentID, serviceID := parent.Owner.EnvironmentID, unit.Target.ID
	ownedKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID)
	ownedRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{ownedKey}, Revision: snapshot.ReadRevision},
	)
	if err != nil {
		return false, err
	}
	if ownedRead == nil || ownedRead.ReadRevision != snapshot.ReadRevision || len(ownedRead.Values) != 1 ||
		ownedRead.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission identities are unavailable")
	}
	defer keyvalue.ClearValues(ownedRead.Values)
	owned, err := environmentprojection.DecodeEnvironmentOwnedIdentities(ownedRead.Values[0].Value)
	if err != nil || owned.EnvironmentID != environmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != uint64(parent.RenderGeneration) {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission identities changed")
	}
	for _, identity := range owned.Services {
		if identity.ID == serviceID {
			return false, errs.New(errs.KindStateConflict, "Blueprint Service is still declared")
		}
	}
	declarationID := ""
	for _, omission := range owned.PendingOmissions {
		if omission.Kind == ids.KindService && omission.ID == serviceID {
			declarationID = omission.SourceRevisionID
			break
		}
	}
	if declarationID == "" {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission has no retained declaration")
	}
	keys := []string{
		taskjournal.TaskStorageKey(parent.ID), taskjournal.BlueprintParentClaimKey(parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, declarationID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, declarationID),
		blueprints.EnvironmentBlueprintEffectiveProjectionKey(environmentID, declarationID),
		environmentprojection.EnvironmentComposeProjectionStorageKey(environmentID),
		services.ServiceRuntimeKey(serviceID), serviceruntimerecord.Key(serviceID),
		services.ServiceLifecycleActiveKey(serviceID),
		deletions.TombstoneKey(string(deletions.DeletionTargetService), serviceID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return false, errs.New(errs.KindInternal, "Blueprint Service omission evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := []keyvalue.Condition{{Key: ownedKey, ModRevision: ownedRead.Values[0].ModRevision}}
	for index, value := range read.Values {
		if index < 5 && value == nil || value != nil && value.Key != keys[index] {
			return false, errs.New(errs.KindStateConflict, "Blueprint Service omission authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	if parentErr != nil || claimErr != nil || claimID != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil || storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner || storedParent.PlanID != parent.PlanID ||
		storedParent.PlanHash != parent.PlanHash || storedParent.RenderGeneration != parent.RenderGeneration {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission parent claim changed")
	}
	if read.Values[5] != nil || read.Values[7] != nil || read.Values[8] != nil {
		return false, nil
	}
	for _, value := range read.Values[9:] {
		if value != nil {
			return false, errs.New(
				errs.KindResourceInUse,
				"Blueprint Service omission overlaps an active lifecycle operation",
			)
		}
	}
	declared, found, err := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		environmentID,
		declarationID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	if !found || declared.Revision != read.Values[3].ModRevision {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service retained declaration changed")
	}
	declarationOwned, err := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[4].Value)
	if err != nil || declarationOwned.EnvironmentID != environmentID || declarationOwned.RevisionID != declarationID ||
		declarationOwned.RenderGeneration != declared.Record.RenderGeneration {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service retained identities changed")
	}
	var identity environmentprojection.OwnedIdentity
	for _, selected := range declarationOwned.Services {
		if selected.ID == serviceID {
			identity = selected
			break
		}
	}
	if identity.ID == "" {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service is absent from its retained declaration")
	}
	if err := requireServiceDeclared(declared.Record, identity.Name); err != nil {
		return false, err
	}
	receipt, eligible, err := blueprintunits.TryPrepareControllerServiceDeclarationRemoval(
		snapshot, unit, ids.New(ids.KindOperation), declarationID, declared.Revision, identity.BirthRevisionID,
	)
	if err != nil || !eligible {
		return false, err
	}
	defer receipt.Clear()
	current, currentFound, err := blueprints.ReadDesiredInputRevision(
		ctx,
		repository.store,
		environmentID,
		parent.ID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	if !currentFound || current.Revision != read.Values[2].ModRevision ||
		current.Record.RenderGeneration != uint64(parent.RenderGeneration) {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission desired input changed")
	}
	if err := requireServiceDeclarationUnreferenced(current.Record, identity.Name); err != nil {
		return false, err
	}
	if read.Values[6] != nil {
		applied, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(read.Values[6].Value)
		if err != nil || applied.EnvironmentID != environmentID {
			return false, errs.New(errs.KindInternal, "Blueprint Service applied projection is corrupt")
		}
		if projectionContainsService(applied, serviceID) {
			return false, nil
		}
		if err := requireServiceAppliedUnreferenced(applied, serviceID, identity.Name); err != nil {
			return false, err
		}
	}
	references, err := serviceremovalreferences.PrepareDeclarationRemoval(
		ctx,
		repository.store,
		environmentID,
		serviceID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	scripts, err := scriptsourceevidence.PrepareServiceScriptAbsence(
		ctx,
		repository.store,
		serviceID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	dns, err := dnsrecords.NewReader(repository.store).RequireDNSUnreferenced(ctx, serviceID, "", snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	addresses, err := networkreservations.PrepareProxyAddressRelease(
		ctx,
		repository.store,
		serviceID,
		snapshot.ReadRevision,
	)
	if err != nil {
		return false, err
	}
	defer addresses.Clear()
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return false, err
	}
	defer clear(epoch.Value)
	conditions = append(conditions, receipt.Conditions()...)
	conditions = append(conditions, references.Conditions()...)
	conditions = append(conditions, scripts...)
	conditions = append(conditions, dns...)
	conditions = append(conditions, addresses.Conditions()...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions, keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)})
	mutations := append(receipt.Mutations(), addresses.Mutations()...)
	mutations = append(mutations, epoch)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Blueprint Service omission settlement raced")
	}
	return true, nil
}
