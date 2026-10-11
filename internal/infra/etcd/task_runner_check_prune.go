package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) runnerCheckRetentionHeld(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (bool, error) {
	if !runnerCheckTask(task) {
		return false, nil
	}
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{
			Keys:     []string{runnerchecks.RecordKey(task.ID), runnerchecks.OperationKey(task.OperationID)},
			Revision: revision,
		},
	)
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return false, errs.New(errs.KindInternal, "Runner diagnostic retention read is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return read.Values[1] != nil, nil
	}
	record, err := runnerchecks.Decode(read.Values[0].Value)
	if err != nil {
		return false, err
	}
	if !runnerCheckMatchesTask(record, task) {
		return false, errs.New(errs.KindStateConflict, "Runner diagnostic retention differs from its Task")
	}
	return read.Values[1] != nil || record.Phase != runnerchecks.Finished, nil
}

func (repository *TaskRepository) pruneRunnerCheckInput(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retention keyvalue.KeyValue,
	revision int64,
) (bool, error) {
	if !runnerCheckTask(task) {
		return false, nil
	}
	input, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{runnerchecks.RecordKey(task.ID)}, Revision: revision},
	)
	if err != nil {
		return true, err
	}
	if input == nil || input.ReadRevision != revision || len(input.Values) != 1 {
		if input != nil {
			keyvalue.ClearValues(input.Values)
		}
		return true, errs.New(errs.KindInternal, "Runner diagnostic prune input is incomplete")
	}
	defer keyvalue.ClearValues(input.Values)
	if input.Values[0] == nil {
		return false, nil
	}
	record, conditions, err := repository.loadRunnerCheckAt(ctx, task, revision)
	if err != nil {
		return true, err
	}
	if record.Phase != runnerchecks.Finished {
		return true, nil
	}
	latestKey := runnerchecks.LatestKey(task.Target)
	keys := []string{
		taskjournal.TaskActiveOperationKey(task.OperationID),
		taskjournal.TaskAssignmentIndexKey(task.ID),
		latestKey,
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return true, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return true, errs.New(errs.KindInternal, "Runner diagnostic prune ownership is incomplete")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] != nil || read.Values[1] != nil {
		return true, nil
	}
	conditions = append(
		conditions,
		keyvalue.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskRevision},
		keyvalue.Condition{Key: retention.Key, ModRevision: retention.ModRevision},
	)
	for index, key := range keys {
		conditions = append(
			conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])},
		)
	}
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: runnerchecks.RecordKey(task.ID)}}
	if read.Values[2] != nil && string(read.Values[2].Value) == task.ID {
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: latestKey})
	}
	result, err := repository.store.Transact(ctx, conditions, mutations)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return true, err
	}
	if !result.Succeeded {
		return true, errs.New(errs.KindStateConflict, "Runner diagnostic pruning raced its Task or latest selection")
	}
	return true, nil
}
