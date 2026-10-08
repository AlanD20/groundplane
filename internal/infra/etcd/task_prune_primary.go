package etcd

import (
	"context"
	"encoding/hex"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) deleteTaskPrunePrimary(
	ctx context.Context,
	current etcdstore.Versioned[taskjournal.PruneIntent],
) (etcdstore.Versioned[taskjournal.PruneIntent], error) {
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			taskjournal.TaskStorageKey(current.Record.TaskID),
			backupruntime.BackupExecutionPlanKey(current.Record.TaskID),
			taskjournal.TaskTerminalReceiptKey(
				current.Record.TaskID,
			),
			taskjournal.TaskTerminalDeliveryKey(current.Record.TaskID),
		},
	})
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if taskResult == nil || taskResult.ReadRevision <= 0 || len(taskResult.Values) != 4 ||
		taskResult.Values[0] == nil ||
		taskResult.Values[0].ModRevision != current.Record.TaskRevision {
		if taskResult != nil {
			etcdstore.ClearValues(taskResult.Values)
		}
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	defer etcdstore.ClearValues(taskResult.Values)
	task, err := DecodeTaskRecord(taskResult.Values[0].Value)
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
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: taskjournal.TaskStorageKey(current.Record.TaskID)},
	}
	nativeConditions, nativeMutations, err := repository.prepareBackupNativeTaskPrune(
		ctx, task, taskResult.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	conditions = append(conditions, nativeConditions...)
	mutations = append(mutations, nativeMutations...)
	softwareConditions, softwareMutations, err := repository.prepareSoftwareProjectionPrune(
		ctx,
		task,
		taskResult.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	conditions = append(conditions, softwareConditions...)
	mutations = append(mutations, softwareMutations...)
	heldSoftware, childConditions, err := repository.softwareChildPruneAuthority(ctx, task, taskResult.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if heldSoftware {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	conditions = append(conditions, childConditions...)
	if held, err := terminalDeliveryPruneAuthority(task, current.Record.TaskRevision, taskResult.Values[2:]); held ||
		err != nil {
		if err != nil {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
		}
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	for _, value := range taskResult.Values[2:] {
		if value != nil {
			conditions = append(conditions, etcdstore.Condition{Key: value.Key, ModRevision: value.ModRevision})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: value.Key})
		}
	}
	procedure := taskResult.Values[1]
	if task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskBackupPrune ||
		task.Type == taskjournal.TaskRestore {
		if procedure == nil || procedure.Version != 1 || procedure.ModRevision <= 0 {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
		}
		sealed, err := backupruntime.DecodeBackupExecutionPlan(procedure.Value)
		if err != nil || sealed.PlanId != task.PlanID || hex.EncodeToString(sealed.PlanHash) != task.PlanHash ||
			sealed.TargetId != task.Target {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
		}
		stagingConditions, stagingMutations, err := repository.backupStagingPruneIndexes(
			ctx,
			task,
			sealed,
			taskResult.ReadRevision,
		)
		if err != nil {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
		}
		conditions = append(conditions, stagingConditions...)
		mutations = append(mutations, stagingMutations...)
		conditions = append(
			conditions,
			etcdstore.Condition{Key: backupruntime.BackupExecutionPlanKey(task.ID), ModRevision: procedure.ModRevision},
		)
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: backupruntime.BackupExecutionPlanKey(task.ID)},
		)
	} else if procedure != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
	}
	for index, key := range ownerKeys {
		value := ownerResult.Values[index]
		if value == nil || value.Key != key || value.ModRevision <= 0 || string(value.Value) != task.ID {
			return etcdstore.Versioned[taskjournal.PruneIntent]{}, taskjournal.CorruptPruneIntent()
		}
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: value.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: key})
	}
	held, orphanConditions, err := repository.taskOrphanCleanupPruneConditions(ctx, task, taskResult.ReadRevision)
	if err != nil {
		return etcdstore.Versioned[taskjournal.PruneIntent]{}, err
	}
	if held {
		return etcdstore.Versioned[taskjournal.PruneIntent]{},
			taskjournal.CorruptPruneIntent()
	}
	conditions = append(conditions, orphanConditions...)
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
