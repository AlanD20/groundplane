package entrywork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// BlueprintRemoval retires only captured derived Entry state. Its Apply already
// owns the desired head; completing file cleanup must not promote another head.
type BlueprintRemoval struct {
	EntryID           string `json:"entry_id"`
	ValueGenerationID string `json:"value_generation_id"`
	RecordRevision    int64  `json:"record_revision"`
	OwnerRevision     int64  `json:"owner_revision"`
	BindingRevision   int64  `json:"binding_revision"`
}

func (removal BlueprintRemoval) matches(view workqueue.View) error {
	if view.Task.Task.Action != workexecution.ActionApply || view.Item.Item.TargetID != removal.EntryID ||
		ids.Validate(
			ids.KindEnvEntry,
			removal.EntryID,
		) != nil || ids.Validate(ids.KindConfig, removal.ValueGenerationID) != nil ||
		removal.RecordRevision <= 0 || removal.OwnerRevision <= 0 || removal.BindingRevision <= 0 ||
		removal.RecordRevision > view.ReadRevision || removal.OwnerRevision > view.ReadRevision || removal.BindingRevision > view.ReadRevision {
		return conflict("Entry file cleanup has no matching Blueprint omission authority")
	}
	return nil
}

// The caller joins the final host observation and resource receipt. Reads of
// new desired state cannot change which captured Entry versions are retired.
func (repository *InputRepository) prepareBlueprintRemoval(ctx context.Context, view workqueue.View,
	removal BlueprintRemoval,
) (workqueue.Change, error) {
	if err := removal.matches(view); err != nil {
		return workqueue.Change{}, err
	}
	environmentID := view.Task.Task.Scope.EnvironmentID
	keys := []string{entries.RecordKey(removal.EntryID), entries.EntryOwnerKey(environmentID, removal.EntryID),
		entries.BlueprintEntryEnvironmentPrefix + removal.EntryID}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	versions := []int64{removal.RecordRevision, removal.OwnerRevision, removal.BindingRevision}
	guards := make([]keyvalue.Condition, 0, len(keys))
	for index, key := range keys {
		if read.Values[index] == nil || read.Values[index].ModRevision != versions[index] {
			return workqueue.Change{}, conflict("Entry metadata changed after its cleanup input was captured")
		}
		guards = append(guards, keyvalue.Condition{Key: key, ModRevision: versions[index]})
	}
	record, err := entries.DecodeRecord(read.Values[0].Value)
	if err != nil || record.EnvironmentID != environmentID || record.Entry.ID != removal.EntryID ||
		record.CurrentValueGenerationID != removal.ValueGenerationID ||
		string(read.Values[1].Value) != removal.EntryID || string(read.Values[2].Value) != environmentID {
		return workqueue.Change{}, conflict("Entry cleanup metadata belongs to a different source")
	}
	scripts, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, removal.EntryID, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	retirement, mutations, err := entries.PrepareProjectedEntryRetirement(
		ctx,
		repository.store,
		record,
		view.ReadRevision,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadOrdinary(ctx, repository.store, environmentID, view.ReadRevision)
	if err != nil {
		keyvalue.ClearMutationValues(mutations)
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		keyvalue.ClearMutationValues(mutations)
		return workqueue.Change{}, err
	}
	guards = append(guards, scripts...)
	guards = append(guards, retirement...)
	guards = append(guards, fence.TransactionConditions()...)
	mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[0]},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[1]}, epoch)
	return workqueue.Change{Conditions: guards, Mutations: mutations}, nil
}
