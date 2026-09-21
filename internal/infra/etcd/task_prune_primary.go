package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) deleteTaskPrunePrimary(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
) (etcdstore.Versioned[taskjournal.PruneIntent], error) {
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(current.Record.TaskID)},
	})
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if taskResult == nil || taskResult.ReadRevision <= 0 || len(taskResult.Values) != 1 ||
		taskResult.Values[0] == nil ||
		taskResult.Values[0].ModRevision != current.Record.TaskRevision {
		if taskResult != nil {
			etcdstore.ClearValues(taskResult.Values)
		}
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	defer etcdstore.ClearValues(taskResult.Values)
	task, err := decodeTaskRecord(taskResult.Values[0].Value)
	if err != nil || task.ID != current.Record.TaskID || !taskjournal.IsTerminalTaskStatus(task.Status) {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	ownerKeys, err := taskJournalIndexKeys(task)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	ownerResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: ownerKeys, Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if ownerResult == nil || ownerResult.ReadRevision != taskResult.ReadRevision ||
		len(ownerResult.Values) != len(ownerKeys) {
		if ownerResult != nil {
			etcdstore.ClearValues(ownerResult.Values)
		}
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	defer etcdstore.ClearValues(ownerResult.Values)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(current.Record.TaskID), ModRevision: current.Record.TaskRevision},
		{Key: backupruntime.BackupCheckpointCursorTaskPrefix(current.Record.TaskID), Prefix: true},
		{Key: backupruntime.BackupCheckpointDedupTaskPrefix(current.Record.TaskID), Prefix: true},
	}
	mutations := []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: taskjournal.TaskStorageKey(current.Record.TaskID)}}
	for index, key := range ownerKeys {
		value := ownerResult.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 || string(value.Value) != task.ID {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
		}
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	next := current.Record
	next.TaskPrimaryDeleted = true
	return repository.advanceTaskPruneIntent(
		ctx,
		current,
		next,
		conditions,
		mutations,
	)
}
