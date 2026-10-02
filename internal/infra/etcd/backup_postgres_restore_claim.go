package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresrestoreauthority"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) preparePostgresRestoreClaim(ctx context.Context, task TaskRecord,
	plan *agentpb.ExecutionPlan, assignment taskassignments.TaskAssignmentRecord, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if task.Type != taskjournal.TaskRestore || len(plan.GetSteps()) != 1 {
		return nil, nil, errs.New(errs.KindStateConflict, "PostgreSQL Restore claim plan is invalid")
	}
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(task.Owner.EnvironmentID, task.ID)
	if err != nil {
		return nil, nil, err
	}
	keys := []string{backupruntime.BackupRestoreKey(task.ID), membership}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil {
		return nil, nil, errs.New(errs.KindStateConflict, "queued PostgreSQL Restore authority is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	restore, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || restore.TaskID != task.ID || restore.OperationID != task.OperationID ||
		restore.EnvironmentID != task.Owner.EnvironmentID || restore.State != backupruntime.BackupRestoreQueued ||
		backupruntime.ValidatePostgresRestoreExecutionPlan(restore, plan) != nil ||
		read.Values[0].Key != keys[0] || read.Values[1].Key != keys[1] ||
		read.Values[1].Version != 1 || string(read.Values[1].Value) != task.ID ||
		read.Values[1].ModRevision != read.Values[0].ModRevision || !restore.CreatedAt.Equal(task.CreatedAt) {
		return nil, nil, errs.New(errs.KindStateConflict, "queued PostgreSQL Restore does not match its Task")
	}
	authority, err := postgresrestoreauthority.Read(ctx, repository.store, restore, revision)
	if err != nil {
		return nil, nil, err
	}
	fence, err := environmentfence.LoadOwned(ctx, repository.store, restore.EnvironmentID, revision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationRestore,
			OperationID: restore.OperationID, TaskID: restore.TaskID})
	if err != nil {
		return nil, nil, err
	}
	backingEnvironmentID, postgres, err := backupruntime.PostgresRestoreBackingEnvironmentID(restore)
	if err != nil || !postgres {
		if err != nil {
			return nil, nil, err
		}
		return nil, nil, errs.New(errs.KindInternal, "PostgreSQL Restore backing Environment is missing")
	}
	backingGuards, err := postgresbackingguard.PrepareOwnership(
		ctx,
		repository.store,
		[]string{backingEnvironmentID},
		restore.EnvironmentID,
		postgresbackingguard.Owner(
			backupruntime.BackupOperationRestore,
			restore.OperationID,
			restore.TaskID,
		),
		revision,
		false,
	)
	if err != nil {
		return nil, nil, err
	}
	next := backupruntime.CloneBackupRestoreRecord(restore)
	next.State, next.UpdatedAt = backupruntime.BackupRestoreDownloading,
		backupTerminalTimestamp(assignment.AssignedAt, restore.UpdatedAt)
	value, err := backupruntime.EncodeBackupRestoreRecord(next)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		clear(value)
		return nil, nil, err
	}
	conditions := []etcdstore.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision},
		{Key: keys[1], ModRevision: read.Values[1].ModRevision}}
	conditions = append(conditions, authority...)
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		clear(value)
		clear(epoch.Value)
		return nil, nil, err
	}
	conditions = append(conditions, backingGuards.Conditions...)
	return conditions, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: value}, epoch}, nil
}
