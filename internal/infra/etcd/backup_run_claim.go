package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Claim moves the Run and its Task together. Source delivery cannot observe a
// running Task paired with a queued Run, or infer execution from Task state.
func (repository *TaskRepository) prepareBackupRunClaim(ctx context.Context, task TaskRecord,
	claimedAt time.Time, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if task.Type != taskjournal.TaskBackup {
		return nil, nil, nil
	}
	key := backupruntime.BackupRunKey(task.ID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "queued Backup Run is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	run, err := backupruntime.DecodeBackupRunRecord(read.Values[0].Value)
	if err != nil || run.State != backupruntime.BackupRunQueued || ValidateBackupRunTaskBinding(task, run) != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "queued Backup Run does not match its Task")
	}
	next := backupruntime.CloneBackupRunPublicationRecord(run)
	next.State, next.UpdatedAt = backupruntime.BackupRunRunning, backupTerminalTimestamp(claimedAt, run.UpdatedAt)
	if err := backupruntime.ValidateBackupRunTransition(run, next, backupruntime.BackupRunTransitionOrdinary); err != nil {
		return nil, nil, err
	}
	fence, err := environmentfence.LoadBackupRunOwned(ctx, repository.store, run, revision)
	if err != nil {
		return nil, nil, err
	}
	backingEnvironmentIDs, err := backupruntime.DatabaseBackingEnvironmentIDs(run)
	if err != nil {
		return nil, nil, err
	}
	backingGuards, err := postgresbackingguard.PrepareOwnership(
		ctx,
		repository.store,
		backingEnvironmentIDs,
		run.EnvironmentID,
		postgresbackingguard.Owner(
			backupruntime.BackupOperationBackup,
			run.OperationID,
			run.TaskID,
		),
		revision,
		false,
	)
	if err != nil {
		return nil, nil, err
	}
	value, err := backupruntime.EncodeBackupRunRecord(next)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		clear(value)
		return nil, nil, err
	}
	conditions := append(
		[]etcdstore.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}},
		fence.TransactionConditions()...)
	conditions = append(conditions, backingGuards.Conditions...)
	return conditions, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}, epoch}, nil
}
