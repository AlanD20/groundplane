package etcd

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareBlueprintEntryOmission retires an Entry with no applied runtime
// representation. Returning false means the Agent must clean its managed files;
// publication of desired absence alone never proves that cleanup.
func (repository *EnvironmentBlueprintRepository) PrepareBlueprintEntryOmission(
	ctx context.Context, work workqueue.View, unit blueprintunits.Unit, at time.Time,
) (workqueue.Change, bool, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, false, err
	}
	parent := work.Task.Task
	if repository == nil || parent.Validate() != nil || parent.Action != workexecution.ActionApply ||
		work.ReadRevision <= 0 || work.Task.Revision <= 0 || work.Item.Revision <= 0 || !unit.Removal ||
		unit.Target.Kind != ids.KindEnvEntry {
		return workqueue.Change{}, false, errs.New(
			errs.KindValidationFailed,
			"Blueprint Entry omission identity is invalid",
		)
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, parent.Scope.EnvironmentID, work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return workqueue.Change{}, false, errs.New(
			errs.KindStateConflict,
			"Blueprint Entry omission has been superseded",
		)
	}
	// Per-Service acknowledgements can be newer than the Environment-wide
	// projection. Their presence precludes a metadata-only absence proof, even
	// when the older projection does not list this Entry.
	for _, applied := range snapshot.Applied {
		if applied.Record.Target.Kind == ids.KindService && applied.Record.Execution != nil &&
			applied.Record.State != blueprintunits.Absent {
			return workqueue.Change{}, false, nil
		}
	}
	environmentID, entryID := parent.Scope.EnvironmentID, unit.Target.ID
	keys := []string{
		workqueue.TaskKey(parent.ID), workqueue.ItemKey(work.Item.Item.ID),
		blueprints.EnvironmentBlueprintRootKey(environmentID, parent.ID),
		blueprints.EnvironmentBlueprintOwnedIdentitiesKey(environmentID, parent.ID),
		entries.RecordKey(entryID), entries.EntryOwnerKey(environmentID, entryID),
		entries.BlueprintEntryEnvironmentPrefix + entryID,
		environmentprojection.EnvironmentComposeProjectionStorageKey(environmentID),
		deletions.TombstoneKey(string(deletions.DeletionTargetEntry), entryID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(environmentID),
		workqueue.ClaimKey(workexecution.Claim{ScopeID: environmentID}),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return workqueue.Change{}, false, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, false, errs.New(errs.KindInternal, "Blueprint Entry omission evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	conditions := make([]keyvalue.Condition, 0, len(keys))
	for index, value := range read.Values {
		if (index < 7 && value == nil) || (value != nil && value.Key != keys[index]) {
			return workqueue.Change{}, false, errs.New(
				errs.KindStateConflict,
				"Blueprint Entry omission authority is unavailable",
			)
		}
		conditions = append(conditions, keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(value)})
	}
	storedParent, parentErr := workqueue.DecodeTask(read.Values[0].Value)
	storedWork, workErr := workqueue.DecodeItem(read.Values[1].Value)
	desiredInput, desiredFound, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, environmentID, parent.ID, snapshot.ReadRevision,
	)
	desired := desiredInput.Record
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if parentErr != nil || workErr != nil || desiredErr != nil || !desiredFound ||
		desiredInput.Revision != read.Values[2].ModRevision || ownedErr != nil ||
		read.Values[0].ModRevision != work.Task.Revision || read.Values[1].ModRevision != work.Item.Revision ||
		storedParent.ID != parent.ID || storedParent.Scope != parent.Scope || storedParent.Intent != parent.Intent ||
		storedWork.ID != work.Item.Item.ID || storedWork.TaskID != parent.ID ||
		desired.EnvironmentID != environmentID || desired.RevisionID != parent.ID ||
		owned.EnvironmentID != environmentID || owned.RevisionID != parent.ID ||
		owned.RenderGeneration != desired.RenderGeneration {
		return workqueue.Change{}, false, errs.New(errs.KindStateConflict, "Blueprint Entry omission revision changed")
	}
	encodedDesired, err := environmentprojection.EncodeEnvironmentDesiredInputStorage(desired)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	digest := sha256.Sum256(encodedDesired)
	clear(encodedDesired)
	if hex.EncodeToString(digest[:]) != parent.Intent.SHA256 {
		return workqueue.Change{}, false, errs.New(
			errs.KindStateConflict,
			"Blueprint Entry removal captured intent changed",
		)
	}
	for _, identity := range owned.Entries {
		if identity.ID == entryID {
			return workqueue.Change{}, false, errs.New(errs.KindStateConflict, "Blueprint Entry is still declared")
		}
	}
	record, err := entries.DecodeRecord(read.Values[4].Value)
	if err != nil || record.Entry.ID != entryID || record.EnvironmentID != environmentID ||
		string(read.Values[5].Value) != entryID || string(read.Values[6].Value) != environmentID {
		return workqueue.Change{}, false, errs.New(errs.KindStateConflict, "Blueprint Entry omission owner changed")
	}
	if _, declared := desired.Input.Entries[record.BlueprintKey]; declared {
		return workqueue.Change{}, false, errs.New(
			errs.KindStateConflict,
			"Blueprint Entry omission retains its authored name",
		)
	}
	if read.Values[7] != nil {
		applied, err := environmentprojection.DecodeEnvironmentComposeProjectionStorage(read.Values[7].Value)
		if err != nil || applied.EnvironmentID != environmentID {
			return workqueue.Change{}, false, errs.New(
				errs.KindStateConflict,
				"Blueprint Entry applied runtime is invalid",
			)
		}
		for _, entry := range applied.Entries {
			if entry.Entry.ID == entryID {
				return workqueue.Change{}, false, nil
			}
		}
	}
	for _, value := range read.Values[8:] {
		if value != nil {
			return workqueue.Change{}, false, errs.New(
				errs.KindResourceInUse,
				"Blueprint Entry cleanup overlaps an active removal or file writer",
			)
		}
	}
	scriptConditions, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, entryID, snapshot.ReadRevision)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	retirementConditions, mutations, err := entries.PrepareProjectedEntryRetirement(
		ctx,
		repository.store,
		record,
		snapshot.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	// The binding was already read with the other authority. Do not compare it
	// twice when joining the owning Entry module's retirement fragment.
	if len(retirementConditions) != 1 || retirementConditions[0] != conditions[6] {
		return workqueue.Change{}, false, errs.New(errs.KindStateConflict, "Blueprint Entry retirement lookup changed")
	}
	receipt, err := blueprintunits.PrepareEntryWorkSettlement(
		snapshot, work, unit, record.CurrentValueGenerationID, at,
	)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	defer keyvalue.ClearMutationValues(receipt.Mutations)
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, snapshot.ReadRevision)
	if err != nil {
		return workqueue.Change{}, false, err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, false, err
	}
	defer clear(epochMutation.Value)
	conditions = append(conditions, receipt.Conditions...)
	conditions = append(conditions, scriptConditions...)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations = append(mutations, receipt.Mutations...)
	mutations = append(mutations,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[4]},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[5]}, epochMutation,
	)
	// The queue owner joins dependency/claim fences before committing. Returned
	// bytes belong to that transaction, not to this preparation stack.
	prepared := workqueue.Change{Conditions: conditions, Mutations: make([]keyvalue.Mutation, len(mutations))}
	for index, mutation := range mutations {
		prepared.Mutations[index] = mutation
		prepared.Mutations[index].Value = bytes.Clone(mutation.Value)
	}
	return prepared, true, nil
}
