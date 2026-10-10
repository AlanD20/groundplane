package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TrySettleBlueprintEntryOmission retires an Entry with no applied runtime
// representation. Returning false means the Agent must clean its managed files;
// publication of desired absence alone never proves that cleanup.
func (repository *EnvironmentBlueprintRepository) TrySettleBlueprintEntryOmission(
	ctx context.Context, parent TaskRecord, unit blueprintunits.Unit,
) (bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return false, err
	}
	if repository == nil || validateBlueprintParentClaimTask(parent) != nil ||
		parent.Status != taskjournal.TaskStatusRunning || !unit.Removal ||
		unit.Target.Kind != ids.KindEnvEntry {
		return false, errs.New(errs.KindValidationFailed, "Blueprint Entry omission identity is invalid")
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
		return false, errs.New(errs.KindStateConflict, "Blueprint Entry omission has been superseded")
	}
	// Per-Service acknowledgements can be newer than the Environment-wide
	// projection. Their presence precludes a metadata-only absence proof, even
	// when the older projection does not list this Entry.
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind == ids.KindService && applied.Record.SourceTaskID != "" &&
			applied.Record.State != blueprintunits.Absent {
			return false, nil
		}
	}
	environmentID, entryID := parent.Owner.EnvironmentID, unit.Target.ID
	keys := []string{
		taskjournal.TaskStorageKey(parent.ID), taskjournal.BlueprintParentClaimKey(parent.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID),
		entries.RecordKey(entryID), entries.EntryOwnerKey(environmentID, entryID),
		entries.BlueprintEntryEnvironmentPrefix + entryID,
		environmentprojection.EnvironmentComposeProjectionStorageKey(environmentID),
		deletions.TombstoneKey(string(deletions.DeletionTargetEntry), entryID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
		taskjournal.TaskMaterializationWriterKey(environmentID),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return false, errs.New(errs.KindInternal, "Blueprint Entry omission evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		if (index < 7 && value == nil) || (value != nil && value.Key != keys[index]) {
			return false, errs.New(errs.KindStateConflict, "Blueprint Entry omission authority is unavailable")
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	desiredInput, desiredFound, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, environmentID, parent.ID, snapshot.ReadRevision,
	)
	desired := desiredInput.Record
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if parentErr != nil || claimErr != nil || desiredErr != nil || !desiredFound ||
		desiredInput.Revision != read.Values[2].ModRevision || ownedErr != nil ||
		validateBlueprintParentClaimTask(storedParent) != nil || storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner || storedParent.PlanID != parent.PlanID ||
		storedParent.PlanHash != parent.PlanHash || storedParent.RenderGeneration != parent.RenderGeneration ||
		claimID != parent.ID || desired.EnvironmentID != environmentID || desired.RevisionID != parent.ID ||
		owned.EnvironmentID != environmentID || owned.RevisionID != parent.ID ||
		desired.RenderGeneration != uint64(parent.RenderGeneration) || owned.RenderGeneration != desired.RenderGeneration {
		return false, errs.New(errs.KindStateConflict, "Blueprint Entry omission revision changed")
	}
	for _, identity := range owned.Entries {
		if identity.ID == entryID {
			return false, errs.New(errs.KindStateConflict, "Blueprint Entry is still declared")
		}
	}
	record, err := entries.DecodeRecord(read.Values[4].Value)
	if err != nil || record.Entry.ID != entryID || record.EnvironmentID != environmentID ||
		string(read.Values[5].Value) != entryID || string(read.Values[6].Value) != environmentID {
		return false, errs.New(errs.KindStateConflict, "Blueprint Entry omission owner changed")
	}
	if _, declared := desired.Input.Entries[record.BlueprintKey]; declared {
		return false, errs.New(errs.KindStateConflict, "Blueprint Entry omission retains its authored name")
	}
	if read.Values[7] != nil {
		applied, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(read.Values[7].Value)
		if err != nil || applied.EnvironmentID != environmentID {
			return false, errs.New(errs.KindStateConflict, "Blueprint Entry applied runtime is invalid")
		}
		for _, entry := range applied.Entries {
			if entry.Entry.ID == entryID {
				return false, nil
			}
		}
	}
	for _, value := range read.Values[8:] {
		if value != nil {
			return false, errs.New(errs.KindResourceInUse, "Blueprint Entry cleanup overlaps an active removal or file writer")
		}
	}
	scriptConditions, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, entryID, snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	retirementConditions, mutations, err := entries.PrepareProjectedEntryRetirement(ctx, repository.store, record, snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	// The binding was already read with the other authority. Do not compare it
	// twice when joining the owning Entry module's retirement fragment.
	if len(retirementConditions) != 1 || retirementConditions[0] != conditions[6] {
		return false, errs.New(errs.KindStateConflict, "Blueprint Entry retirement lookup changed")
	}
	receipt, err := blueprintunits.PrepareControllerEntryRemoval(
		snapshot, unit, ids.New(ids.KindOperation), record.CurrentValueGenerationID,
	)
	if err != nil {
		return false, err
	}
	defer receipt.Clear()
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, snapshot.ReadRevision)
	if err != nil {
		return false, err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return false, err
	}
	defer clear(epochMutation.Value)
	conditions = append(conditions, receipt.Conditions()...)
	conditions = append(conditions, scriptConditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	conditions = append(conditions, keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)})
	mutations = append(mutations, receipt.Mutations()...)
	mutations = append(mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[4]},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[5]}, epochMutation,
	)
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return false, err
	}
	if !result.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Blueprint Entry omission raced")
	}
	return true, nil
}
