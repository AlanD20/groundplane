package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) backingCheckRetentionHeld(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, error) {
	if !backingCheckTask(task) {
		return false, nil
	}
	keys := []string{backingchecks.Key(task.ID), backingchecks.OperationKey(task.OperationID)}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, errs.New(errs.KindInternal, "Backing check retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return read.Values[1] != nil, nil
	}
	record, err := backingchecks.Decode(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	defer record.Clear()
	if !backingCheckMatchesTask(record, task) {
		return false, errs.New(errs.KindStateConflict, "Backing check retention input differs from its Task")
	}
	return read.Values[1] != nil ||
		record.Phase != backingchecks.Finished && record.Phase != backingchecks.Transferred, nil
}

func (repository *TaskRepository) pruneBackingCheckInput(ctx context.Context, task TaskRecord, taskRevision int64,
	retention keyvalue.KeyValue, revision int64,
) (bool, error) {
	if !backingCheckTask(task) {
		return false, nil
	}
	input, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{backingchecks.Key(task.ID)}, Revision: revision},
	)
	if err != nil {
		return true, err
	}
	if input == nil || input.ReadRevision != revision || len(input.Values) != 1 {
		if input != nil {
			keyvalue.ClearValues(input.Values)
		}
		return true, errs.New(errs.KindInternal, "Backing check pruning input is incomplete")
	}
	defer keyvalue.ClearValues(input.Values)
	if input.Values[0] == nil {
		return false, nil
	}
	record, conditions, err := repository.loadBackingCheckAt(ctx, task, revision)
	if err != nil {
		return true, err
	}
	defer record.Clear()
	if record.Phase != backingchecks.Finished && record.Phase != backingchecks.Transferred {
		return true, nil
	}
	latestKey := backingchecks.LatestKey(record.Source.Parameters.AttachID, record.Source.GrantID)
	keys := []string{taskjournal.TaskActiveOperationKey(task.OperationID), taskjournal.TaskAssignmentIndexKey(task.ID),
		backingchecks.OperationKey(task.OperationID), latestKey}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return true, errs.New(errs.KindInternal, "Backing check pruning ownership is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil || read.Values[2] != nil {
		return true, nil
	}
	conditions = append(
		conditions,
		keyvalue.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		keyvalue.Condition{Key: retention.Key, ModRevision: retention.ModRevision},
	)
	// OperationKey is already captured by loadBackingCheckAt; avoid duplicate
	// compares while retaining exact Task, claim and latest-selection fences.
	for _, index := range []int{0, 1, 3} {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: keys[index], ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: backingchecks.Key(task.ID)}}
	if read.Values[3] != nil && string(read.Values[3].Value) == task.ID {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: latestKey})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return true, err
	}
	if !result.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Backing check pruning raced its Task or cleanup owner")
	}
	return true, nil
}
