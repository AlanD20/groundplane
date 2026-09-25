package etcd

import (
	"bytes"
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintEntrySettlement carries the resolved value and metadata for exactly
// one ready Entry unit. The repository rederives metadata from immutable
// authored input before committing the value and its Controller receipt.
type BlueprintEntrySettlement struct {
	Parent      TaskRecord
	Unit        blueprintunits.Unit
	Entry       entries.Record
	Generation  entries.EntryValueGeneration
	OperationID string
}

func (repository *EnvironmentBlueprintRepository) SettleBlueprintEntry(
	ctx context.Context, input BlueprintEntrySettlement,
) error {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return err
	}
	parent, record := input.Parent, input.Entry
	if repository == nil || repository.HierarchyRepository == nil ||
		validateBlueprintParentClaimTask(parent) != nil || parent.Status != taskjournal.TaskStatusRunning ||
		parent.Params[blueprints.EnvironmentDesiredRevisionParam] != parent.ID ||
		parent.Owner.EnvironmentID != record.EnvironmentID ||
		input.Unit.Target != (blueprintunits.ResourceKey{Kind: ids.KindEnvEntry, ID: record.Entry.ID}) ||
		ids.Validate(ids.KindOperation, input.OperationID) != nil {
		return errs.New(errs.KindValidationFailed, "Blueprint Entry settlement identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return err
	}
	snapshot, err := ledger.Load(ctx, record.EnvironmentID)
	if err != nil {
		return err
	}
	if snapshot.HeadTaskID != parent.ID {
		return errs.New(errs.KindStateConflict, "Blueprint Entry parent is no longer current")
	}
	claim, err := blueprintunits.PrepareControllerEntrySettlement(
		snapshot, input.Unit, input.OperationID, record.CurrentValueGenerationID,
	)
	if err != nil {
		return err
	}
	defer claim.Clear()
	entryValue, err := entries.EncodeRecord(record)
	if err != nil {
		return err
	}
	defer clear(entryValue)
	generationKey := entryvalues.PlainKey(record.Entry.ID, record.CurrentValueGenerationID)
	if record.Entry.Secret {
		generationKey = entryvalues.SecretKey(record.Entry.ID, record.CurrentValueGenerationID)
	}

	parentKey := taskjournal.TaskStorageKey(parent.ID)
	claimKey := taskjournal.BlueprintParentClaimKey(parent.ID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(record.EnvironmentID, parent.ID)
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(record.EnvironmentID, parent.ID)
	entryKey := entries.RecordKey(record.Entry.ID)
	ownerKey := entries.EntryOwnerKey(record.EnvironmentID, record.Entry.ID)
	bindingKey := entries.BlueprintEntryEnvironmentPrefix + record.Entry.ID
	keys := []string{parentKey, claimKey, rootKey, identitiesKey, entryKey, ownerKey,
		generationKey, bindingKey,
		deletions.TombstoneKey(string(deletions.DeletionTargetEntry), record.Entry.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "Blueprint Entry settlement evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return errs.New(errs.KindInternal, "Blueprint Entry settlement evidence is corrupt")
		}
	}
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return errs.New(errs.KindStateConflict, "Blueprint Entry parent authority is unavailable")
	}
	storedParent, parentErr := DecodeTaskRecord(read.Values[0].Value)
	claimID, claimErr := idempotency.DecodeTaskReference(read.Values[1].Value)
	if parentErr != nil || claimErr != nil || claimID != parent.ID ||
		validateBlueprintParentClaimTask(storedParent) != nil ||
		storedParent.Status != taskjournal.TaskStatusRunning ||
		storedParent.ID != parent.ID || storedParent.Owner != parent.Owner ||
		storedParent.PlanID != parent.PlanID || storedParent.PlanHash != parent.PlanHash ||
		storedParent.RenderGeneration != parent.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint Entry parent claim changed")
	}
	desired, desiredErr := environmentprojection.DecodeEnvironmentDesiredInputStorage(read.Values[2].Value)
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if desiredErr != nil || ownedErr != nil || desired.EnvironmentID != record.EnvironmentID ||
		desired.RevisionID != parent.ID || owned.EnvironmentID != record.EnvironmentID ||
		owned.RevisionID != parent.ID || desired.RenderGeneration != owned.RenderGeneration ||
		desired.RenderGeneration > math.MaxInt32 ||
		int32(desired.RenderGeneration) != parent.RenderGeneration {
		return errs.New(errs.KindStateConflict, "Blueprint Entry authored input changed")
	}
	matched := false
	for _, identity := range owned.Entries {
		if identity.ID != record.Entry.ID {
			continue
		}
		spec, found := desired.Input.Entries[identity.Name]
		if !found || identity.ValueGenerationID != record.CurrentValueGenerationID ||
			identity.Name != record.BlueprintKey {
			return errs.New(errs.KindStateConflict, "Blueprint Entry identity changed")
		}
		projected, err := core.ProjectEntrySpec(identity.Name, spec, identity.ID)
		if err != nil {
			return errs.Wrap(errs.KindInternal, err)
		}
		if projected.Secret && projected.Source.Kind == core.SourceLiteral {
			projected.Source.Literal = ""
		}
		expected, err := entries.NewBlueprintRecord(record.EnvironmentID, identity.Name, projected, identity.ValueGenerationID)
		if err != nil || !entries.EqualRecord(expected, record) {
			return errs.New(errs.KindStateConflict, "Blueprint Entry metadata differs from authored input")
		}
		matched = true
		break
	}
	if !matched || read.Values[8] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint Entry value generation is not available")
	}
	entryRevision, ownerRevision := int64(0), int64(0)
	if read.Values[4] != nil {
		current, err := entries.DecodeRecord(read.Values[4].Value)
		if err != nil || current.EnvironmentID != record.EnvironmentID ||
			current.Entry.ID != record.Entry.ID ||
			read.Values[5] == nil || string(read.Values[5].Value) != record.Entry.ID {
			return errs.New(errs.KindStateConflict, "Blueprint Entry current owner changed")
		}
		adopted := current
		adopted.BlueprintKey = record.BlueprintKey
		if current.CurrentValueGenerationID == record.CurrentValueGenerationID &&
			!entries.EqualRecord(adopted, record) {
			return errs.New(errs.KindStateConflict, "Blueprint Entry reused generation changed its desired metadata")
		}
		entryRevision, ownerRevision = read.Values[4].ModRevision, read.Values[5].ModRevision
	} else if read.Values[5] != nil {
		return errs.New(errs.KindStateConflict, "Blueprint Entry owner index is occupied")
	}
	if read.Values[7] != nil && string(read.Values[7].Value) != record.EnvironmentID {
		return errs.New(errs.KindStateConflict, "Blueprint Entry lookup belongs to another Environment")
	}
	if read.Values[4] != nil {
		current, err := entries.DecodeRecord(read.Values[4].Value)
		if err != nil || (read.Values[6] == nil) ==
			(current.CurrentValueGenerationID == record.CurrentValueGenerationID) {
			return errs.New(errs.KindStateConflict, "Blueprint Entry value generation lineage changed")
		}
	}
	var generationValue []byte
	if read.Values[6] == nil {
		preparedKey, value, err := entries.PrepareEntryGeneration(record, input.Generation)
		if err != nil || preparedKey != generationKey {
			return errs.New(errs.KindStateConflict, "Blueprint Entry new value generation is invalid")
		}
		generationValue = value
	} else {
		if entryRevision == 0 {
			return errs.New(errs.KindStateConflict, "Blueprint Entry orphaned value generation exists")
		}
		if record.Entry.Secret {
			generation, err := entryvalues.DecodeSecret(read.Values[6].Value)
			if err != nil || generation.EnvironmentID != record.EnvironmentID ||
				generation.EntryID != record.Entry.ID || generation.GenerationID != record.CurrentValueGenerationID {
				clear(generation.Ciphertext)
				return errs.New(errs.KindStateConflict, "Blueprint Entry reused secret generation is invalid")
			}
			clear(generation.Ciphertext)
		} else {
			generation, err := entryvalues.DecodePlain(read.Values[6].Value)
			if err != nil || generation.EnvironmentID != record.EnvironmentID ||
				generation.EntryID != record.Entry.ID || generation.GenerationID != record.CurrentValueGenerationID ||
				(record.Entry.Source.Kind == core.SourceLiteral &&
					!bytes.Equal(generation.Content, []byte(record.Entry.Source.Literal))) {
				clear(generation.Content)
				return errs.New(errs.KindStateConflict, "Blueprint Entry reused plain generation is invalid")
			}
			clear(generation.Content)
		}
	}
	defer clear(generationValue)
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, record.EnvironmentID, snapshot.ReadRevision)
	if err != nil {
		return err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return err
	}
	defer clear(epochMutation.Value)
	conditions := append(claim.Conditions(),
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		keyvalue.Condition{Key: taskjournal.BlueprintParentAbortKey(parent.ID)},
		keyvalue.Condition{Key: rootKey, ModRevision: read.Values[2].ModRevision},
		keyvalue.Condition{Key: identitiesKey, ModRevision: read.Values[3].ModRevision},
		keyvalue.Condition{Key: entryKey, ModRevision: entryRevision},
		keyvalue.Condition{Key: ownerKey, ModRevision: ownerRevision},
		keyvalue.Condition{Key: generationKey, ModRevision: revisionOf(read.Values[6])},
		keyvalue.Condition{Key: bindingKey, ModRevision: revisionOf(read.Values[7])},
		keyvalue.Condition{Key: keys[8]},
	)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := append(claim.Mutations(),
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: entryKey, Value: entryValue},
		epochMutation,
	)
	if generationValue != nil {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: generationKey, Value: generationValue})
	}
	if entryRevision == 0 {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ownerKey, Value: []byte(record.Entry.ID)})
	}
	if read.Values[7] == nil {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: bindingKey, Value: []byte(record.EnvironmentID)})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Blueprint Entry settlement raced")
	}
	return nil
}

func revisionOf(value *keyvalue.KeyValue) int64 {
	if value == nil {
		return 0
	}
	return value.ModRevision
}
