package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func environmentDeletionTaskPruneFence(
	task TaskRecord,
	values []*etcdstore.KeyValue,
) (bool, string, error) {
	if len(values) != 3 {
		return false, "", corruptTaskPruneIntent()
	}
	absent := 0
	for _, value := range values {
		if value == nil {
			absent++
		}
	}
	if absent == len(values) {
		return false, "", nil
	}
	if absent != 0 {
		return false, "", corruptTaskPruneIntent()
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != task.Target {
		return false, "", corruptTaskPruneIntent()
	}
	lock, err := decodeEnvironmentOperationLock(values[1], task.Target)
	if err != nil || lock.Kind != backupruntime.BackupOperationDeletion || lock.EnvironmentID != task.Target ||
		lock.OperationID != task.OperationID {
		return false, "", corruptTaskPruneIntent()
	}
	intent, err := decodeEnvironmentDeletionIntent(values[2].Value)
	if err != nil || intent.EnvironmentID != task.Target ||
		intent.OperationID != task.OperationID ||
		intent.TargetRevision != tombstone.TargetRevision ||
		!intent.CreatedAt.Equal(tombstone.CreatedAt) ||
		tombstone.TaskID != lock.TaskID ||
		tombstone.TaskID != intent.TaskID {
		return false, "", corruptTaskPruneIntent()
	}
	return tombstone.TaskID == task.ID, tombstone.TaskID, nil
}

func (repository *TaskRepository) validateEnvironmentDeletionIntentReplay(
	ctx context.Context,
	task TaskRecord,
	tombstone *deletionrecord.DeletionTombstoneRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentDeletionIntentKey(task.OperationID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if result == nil {
		return errs.New(
			errs.KindInternal,
			"environment deletion replay intent evidence is incomplete",
		)
	}
	if result.ReadRevision != revision || len(result.Values) != 1 {
		clearKeyValues(result.Values)
		return errs.New(
			errs.KindInternal,
			"environment deletion replay intent evidence is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if result.Values[0] != nil {
			return errs.New(
				errs.KindStateConflict,
				"completed environment deletion retained its intent",
			)
		}
		return nil
	}
	if tombstone == nil || result.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "environment deletion retry state is missing")
	}
	intent, err := decodeEnvironmentDeletionIntent(result.Values[0].Value)
	if err != nil {
		return err
	}
	if !environmentDeletionIntentMatches(intent, task, *tombstone) {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion retry intent ownership changed",
		)
	}
	return nil
}
