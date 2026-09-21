package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *TaskRepository) pruneTaskBackupCheckpointBatch(
	ctx context.Context,
	current etcdstore.Versioned[taskPruneIntent],
	cursors bool,
) (etcdstore.Versioned[taskPruneIntent], error) {
	prefix := backupruntime.BackupCheckpointDedupTaskPrefix(current.Record.TaskID)
	if cursors {
		prefix = backupruntime.BackupCheckpointCursorTaskPrefix(current.Record.TaskID)
	}
	page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: prefix,
		Limit:  int64(maximumTaskPruneBatchRecords + 1),
	})
	if err != nil {
		return etcdstore.Versioned[taskPruneIntent]{}, err
	}
	if page == nil || page.ReadRevision <= 0 {
		return etcdstore.Versioned[taskPruneIntent]{}, corruptTaskPruneIntent()
	}
	defer clearKeyValueSlice(page.Values)
	next := current.Record
	if len(page.Values) == 0 {
		if cursors {
			next.BackupCheckpointCursorsComplete = true
		} else {
			next.BackupCheckpointDeduplicationsComplete = true
		}
		return repository.advanceTaskPruneIntent(
			ctx,
			current,
			next,
			[]etcdstore.Condition{{Key: prefix, Prefix: true}},
			nil,
		)
	}
	count := len(page.Values)
	if count > maximumTaskPruneBatchRecords {
		count = maximumTaskPruneBatchRecords
	}
	conditions := make([]etcdstore.Condition, 0, count)
	mutations := make([]etcdstore.Mutation, 0, count)
	for index := 0; index < count; index++ {
		entry := page.Values[index]
		if err := validateTaskBackupCheckpointPruneEntry(
			current.Record.TaskID,
			entry,
			cursors,
		); err != nil {
			return etcdstore.Versioned[taskPruneIntent]{}, err
		}
		conditions = append(conditions, etcdstore.Condition{Key: entry.Key, ModRevision: entry.ModRevision})
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: entry.Key})
	}
	return repository.advanceTaskPruneIntent(ctx, current, next, conditions, mutations)
}

func validateTaskBackupCheckpointPruneEntry(
	taskID string,
	entry etcdstore.KeyValue,
	cursor bool,
) error {
	if entry.ModRevision <= 0 {
		return corruptTaskPruneIntent()
	}
	if cursor {
		record, err := backupruntime.DecodeBackupCheckpointCursorRecord(entry.Value)
		if err != nil || record.TaskID != taskID ||
			backupruntime.BackupCheckpointCursorKey(backupruntime.BackupCheckpointInput{
				TaskID: record.TaskID, AssignmentID: record.AssignmentID, StepID: record.StepID,
			}) != entry.Key {
			return corruptTaskPruneIntent()
		}
		return nil
	}
	record, err := backupruntime.DecodeBackupCheckpointDedupRecord(entry.Value)
	if err != nil || record.TaskID != taskID ||
		backupruntime.BackupCheckpointDedupKey(backupruntime.BackupCheckpointInput{
			TaskID: record.TaskID, AssignmentID: record.AssignmentID,
			StepID: record.StepID, Sequence: record.Sequence,
		}) != entry.Key {
		return corruptTaskPruneIntent()
	}
	return nil
}
