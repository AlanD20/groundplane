package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// PrepareRestoreFailure records the failed resource transition, not proof of
// cleanup. Even before live mutation, a started executor can have staging files
// or a helper to retire. Keep all ownership and credentials for that cleanup.
func (repository *InputRepository) PrepareRestoreFailure(ctx context.Context,
	view workqueue.ResumeView, at time.Time,
) (workqueue.Change, error) {
	if view.Work.Item.Item.Operation != workexecution.Restore || !view.CurrentExecutor || !view.ActiveAttempt ||
		view.Attempt.Attempt.StartedAt == nil || view.Attempt.Attempt.FinishedAt != nil {
		return workqueue.Change{}, conflict("Restore failure no longer owns its started execution")
	}
	record, _, change, err := repository.LoadRestoreSource(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	var next backupruntime.BackupRestoreRecord
	switch record.Point.SourceKind {
	case backupruntime.BackupRuntimeSourceConfig:
		if record.MutationStarted {
			var publicationGuards []keyvalue.Condition
			record, publicationGuards, err = repository.preserveConfigHeadReceipt(ctx, record, view.Work.ReadRevision)
			if err != nil {
				return workqueue.Change{}, err
			}
			change.Conditions = append(change.Conditions, publicationGuards...)
			next, err = backupruntime.RequireConfigRestoreRecovery(record, at)
		} else {
			next, err = backupruntime.FailConfigRestoreBeforeMutation(record, at)
		}
	case backupruntime.BackupRuntimeSourceVolume:
		if record.MutationStarted {
			next, err = backupruntime.RequireVolumeRestoreRecovery(record, at)
		} else {
			next, err = backupruntime.FailVolumeRestoreBeforeMutation(record, at)
		}
	case backupruntime.BackupRuntimeSourceAttach:
		if record.MutationStarted {
			next, err = backupruntime.RequireDatabaseRestoreRecovery(record, at)
		} else {
			next, err = backupruntime.FailDatabaseRestoreBeforeMutation(record, at)
		}
	default:
		return workqueue.Change{}, conflict("Restore failure source is unsupported")
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	guards, err := repository.restoreTerminalGuards(ctx, record, view.Work.ReadRevision, false)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	fence, err := environmentfence.LoadOwned(ctx, repository.store, record.EnvironmentID, view.Work.ReadRevision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationRestore, OperationID: record.OperationID, TaskID: record.TaskID})
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	if record.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
		environment, found, err := backupruntime.DatabaseRestoreBackingEnvironmentID(record)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !found {
			return workqueue.Change{}, conflict("Restore backing ownership is missing")
		}
		backing, err := postgresbackingguard.PrepareOwnership(ctx, repository.store, []string{environment}, record.EnvironmentID,
			postgresbackingguard.Owner(backupruntime.BackupOperationRestore, record.OperationID, record.TaskID), view.Work.ReadRevision, false)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer backing.Clear()
		change.Conditions = append(change.Conditions, backing.Conditions...)
		for _, mutation := range backing.Mutations {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...)})
		}
	}
	encoded, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
		Key: backupruntime.BackupRestoreKey(record.TaskID), Value: encoded})
	return change, nil
}
