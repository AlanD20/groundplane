package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *EnvironmentBlueprintRepository) prepareEntryMetadataRemoval(
	ctx context.Context, request EntryMetadataPublication,
) (workqueue.Change, error) {
	id := request.Task.TargetID
	conditions, err := entries.PrepareEntryScriptAbsence(ctx, repository.store, id, request.Source.Desired.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	keys := []string{entries.RecordKey(id), entries.EntryOwnerKey(request.Task.Scope.EnvironmentID, id),
		entries.BlueprintEntryEnvironmentPrefix + id}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: keys, Revision: request.Source.Desired.ReadRevision},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	if read == nil || read.ReadRevision != request.Source.Desired.ReadRevision || len(read.Values) != len(keys) {
		return workqueue.Change{}, errs.New(errs.KindInternal, "Entry removal metadata observation is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil {
		current, err := entries.DecodeRecord(read.Values[0].Value)
		if err != nil || current.EnvironmentID != request.Task.Scope.EnvironmentID || current.Entry.ID != id {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry removal metadata owner changed")
		}
	}
	if read.Values[1] != nil && string(read.Values[1].Value) != id ||
		read.Values[2] != nil && string(read.Values[2].Value) != request.Task.Scope.EnvironmentID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry removal lookup owner changed")
	}
	change := workqueue.Change{Conditions: conditions}
	for index, key := range keys {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
	}
	return change, nil
}
