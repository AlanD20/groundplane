package etcd

import (
	"bytes"
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entryvalues"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BlueprintEntrySettlement carries the resolved value and metadata for exactly
// one ready Entry unit. The repository rederives metadata from immutable
// authored input before committing the value and its Controller receipt.
type BlueprintEntrySettlement struct {
	Work       workqueue.View
	Unit       blueprintunits.Unit
	Entry      entries.Record
	Generation entries.EntryValueGeneration
	At         time.Time
}

func (repository *EnvironmentBlueprintRepository) PrepareBlueprintEntrySettlement(
	ctx context.Context, input BlueprintEntrySettlement,
) (workqueue.Change, error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return workqueue.Change{}, err
	}
	parent, record := input.Work.Task.Task, input.Entry
	if repository == nil || repository.HierarchyRepository == nil ||
		parent.Validate() != nil || parent.Action != workexecution.ActionApply ||
		input.Work.ReadRevision <= 0 || parent.Scope.EnvironmentID != record.EnvironmentID ||
		input.Unit.Target != (blueprintunits.ResourceKey{Kind: ids.KindEnvEntry, ID: record.Entry.ID}) {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Blueprint Entry settlement identity is invalid")
	}
	ledger, err := blueprintunits.NewRepository(repository.store)
	if err != nil {
		return workqueue.Change{}, err
	}
	snapshot, err := ledger.LoadAtRevision(ctx, record.EnvironmentID, input.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if snapshot.HeadTaskID != parent.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry parent is no longer current")
	}
	claim, err := blueprintunits.PrepareEntryWorkSettlement(
		snapshot, input.Work, input.Unit, record.CurrentValueGenerationID, input.At,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(claim.Mutations)
	entryValue, err := entries.EncodeRecord(record)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(entryValue)
	generationKey := entryvalues.PlainKey(record.Entry.ID, record.CurrentValueGenerationID)
	if record.Entry.Secret {
		generationKey = entryvalues.SecretKey(record.Entry.ID, record.CurrentValueGenerationID)
	}

	parentKey := workqueue.TaskKey(parent.ID)
	workKey := workqueue.ItemKey(input.Work.Item.Item.ID)
	rootKey := blueprints.EnvironmentBlueprintRootKey(record.EnvironmentID, parent.ID)
	identitiesKey := blueprints.EnvironmentBlueprintOwnedIdentitiesKey(record.EnvironmentID, parent.ID)
	entryKey := entries.RecordKey(record.Entry.ID)
	ownerKey := entries.EntryOwnerKey(record.EnvironmentID, record.Entry.ID)
	bindingKey := entries.BlueprintEntryEnvironmentPrefix + record.Entry.ID
	keys := []string{parentKey, workKey, rootKey, identitiesKey, entryKey, ownerKey,
		generationKey, bindingKey,
		deletions.TombstoneKey(string(deletions.DeletionTargetEntry), record.Entry.ID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: snapshot.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != snapshot.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Entry settlement evidence is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	for index, value := range read.Values {
		if value != nil && value.Key != keys[index] {
			return workqueue.Change{}, errs.New(errs.KindInternal, "Blueprint Entry settlement evidence is corrupt")
		}
	}
	if read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry parent authority is unavailable")
	}
	if read.Values[0].ModRevision != input.Work.Task.Revision || read.Values[1].ModRevision != input.Work.Item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry work changed")
	}
	desiredInput, desiredFound, desiredErr := blueprints.ReadDesiredInputRevision(
		ctx, repository.store, record.EnvironmentID, parent.ID, snapshot.ReadRevision,
	)
	desired := desiredInput.Record
	owned, ownedErr := environmentprojection.DecodeEnvironmentOwnedIdentities(read.Values[3].Value)
	if desiredErr != nil || !desiredFound || desiredInput.Revision != read.Values[2].ModRevision ||
		ownedErr != nil || desired.EnvironmentID != record.EnvironmentID ||
		desired.RevisionID != parent.ID || owned.EnvironmentID != record.EnvironmentID ||
		owned.RevisionID != parent.ID || desired.RenderGeneration != owned.RenderGeneration {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry authored input changed")
	}
	matched := false
	for _, identity := range owned.Entries {
		if identity.ID != record.Entry.ID {
			continue
		}
		spec, found := desired.Input.Entries[identity.Name]
		if !found || identity.ValueGenerationID != record.CurrentValueGenerationID ||
			identity.Name != record.BlueprintKey {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry identity changed")
		}
		projected, err := core.ProjectEntrySpec(identity.Name, spec, identity.ID)
		if err != nil {
			return workqueue.Change{}, errs.Wrap(errs.KindInternal, err)
		}
		if projected.Secret && projected.Source.Kind == core.SourceLiteral {
			projected.Source.Literal = ""
		}
		expected, err := entries.NewBlueprintRecord(
			record.EnvironmentID,
			identity.Name,
			projected,
			identity.ValueGenerationID,
		)
		if err != nil || !entries.EqualRecord(expected, record) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry metadata differs from authored input")
		}
		matched = true
		break
	}
	if !matched || read.Values[8] != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry value generation is not available")
	}
	entryRevision, ownerRevision := int64(0), int64(0)
	if read.Values[4] != nil {
		current, err := entries.DecodeRecord(read.Values[4].Value)
		if err != nil || current.EnvironmentID != record.EnvironmentID ||
			current.Entry.ID != record.Entry.ID ||
			read.Values[5] == nil || string(read.Values[5].Value) != record.Entry.ID {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry current owner changed")
		}
		adopted := current
		adopted.BlueprintKey = record.BlueprintKey
		if current.CurrentValueGenerationID == record.CurrentValueGenerationID &&
			!entries.EqualRecord(adopted, record) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry reused generation changed its desired metadata")
		}
		entryRevision, ownerRevision = read.Values[4].ModRevision, read.Values[5].ModRevision
	} else if read.Values[5] != nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry owner index is occupied")
	}
	if read.Values[7] != nil && string(read.Values[7].Value) != record.EnvironmentID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry lookup belongs to another Environment")
	}
	if read.Values[4] != nil {
		current, err := entries.DecodeRecord(read.Values[4].Value)
		if err != nil || (read.Values[6] == nil) ==
			(current.CurrentValueGenerationID == record.CurrentValueGenerationID) {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry value generation lineage changed")
		}
	}
	var generationValue []byte
	if read.Values[6] == nil {
		preparedKey, value, err := entries.PrepareEntryGeneration(record, input.Generation)
		if err != nil || preparedKey != generationKey {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry new value generation is invalid")
		}
		generationValue = value
	} else {
		if entryRevision == 0 {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry orphaned value generation exists")
		}
		if record.Entry.Secret {
			generation, err := entryvalues.DecodeSecret(read.Values[6].Value)
			if err != nil || generation.EnvironmentID != record.EnvironmentID ||
				generation.EntryID != record.Entry.ID || generation.GenerationID != record.CurrentValueGenerationID {
				clear(generation.Ciphertext)
				return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry reused secret generation is invalid")
			}
			clear(generation.Ciphertext)
		} else {
			generation, err := entryvalues.DecodePlain(read.Values[6].Value)
			if err != nil || generation.EnvironmentID != record.EnvironmentID ||
				generation.EntryID != record.Entry.ID || generation.GenerationID != record.CurrentValueGenerationID ||
				(record.Entry.Source.Kind == core.SourceLiteral &&
					!bytes.Equal(generation.Content, []byte(record.Entry.Source.Literal))) {
				clear(generation.Content)
				return workqueue.Change{}, errs.New(errs.KindStateConflict, "Blueprint Entry reused plain generation is invalid")
			}
			clear(generation.Content)
		}
	}
	defer clear(generationValue)
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, record.EnvironmentID, snapshot.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	epochMutation, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(epochMutation.Value)
	conditions := append(claim.Conditions,
		keyvalue.Condition{Key: parentKey, ModRevision: read.Values[0].ModRevision},
		keyvalue.Condition{Key: workKey, ModRevision: read.Values[1].ModRevision},
		keyvalue.Condition{Key: rootKey, ModRevision: read.Values[2].ModRevision},
		keyvalue.Condition{Key: identitiesKey, ModRevision: read.Values[3].ModRevision},
		keyvalue.Condition{Key: entryKey, ModRevision: entryRevision},
		keyvalue.Condition{Key: ownerKey, ModRevision: ownerRevision},
		keyvalue.Condition{Key: generationKey, ModRevision: revisionOf(read.Values[6])},
		keyvalue.Condition{Key: bindingKey, ModRevision: revisionOf(read.Values[7])},
		keyvalue.Condition{Key: keys[8]},
	)
	conditions = append(conditions, fence.TransactionConditions()...)
	mutations := append(claim.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: entryKey, Value: entryValue},
		epochMutation,
	)
	if generationValue != nil {
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: generationKey, Value: generationValue},
		)
	}
	if entryRevision == 0 {
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: ownerKey, Value: []byte(record.Entry.ID)},
		)
	}
	if read.Values[7] == nil {
		mutations = append(
			mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: bindingKey, Value: []byte(record.EnvironmentID)},
		)
	}
	// Prepared values outlive this call; all temporary secret buffers are cleared
	// by the existing defers. The transaction owner clears these owned copies.
	ownedMutations := make([]keyvalue.Mutation, len(mutations))
	for index, mutation := range mutations {
		ownedMutations[index] = mutation
		ownedMutations[index].Value = bytes.Clone(mutation.Value)
	}
	return workqueue.Change{Conditions: conditions, Mutations: ownedMutations}, nil
}

func revisionOf(value *keyvalue.KeyValue) int64 {
	if value == nil {
		return 0
	}
	return value.ModRevision
}
