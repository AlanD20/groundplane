package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareEnvironmentDeletionIntentTerminal(
	ctx context.Context,
	task TaskRecord,
	tombstone deletionrecord.DeletionTombstoneRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	intent, err := loadEnvironmentDeletionIntent(ctx, repository.store, task, tombstone, revision)
	if err != nil {
		return nil, nil, err
	}
	conditions := []etcdstore.Condition{{
		Key: environmentDeletionIntentKey(task.OperationID), ModRevision: intent.Revision,
	}}
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return conditions, nil, nil
	}
	if intent.Record.CleanupPhase != EnvironmentDeletionCleanupComplete {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup enumeration is incomplete",
		)
	}
	if err := requireEnvironmentDeletionBackupStateEmpty(
		ctx, repository.store, task.Target, task.OperationID, revision,
	); err != nil {
		return nil, nil, err
	}
	return conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationDelete, Key: environmentDeletionIntentKey(task.OperationID),
	}}, nil
}
