package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) prepareVolumeRestoreClaim(ctx context.Context, task TaskRecord,
	plan *agentpb.ExecutionPlan, assignment taskassignments.TaskAssignmentRecord, revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	if task.Type != taskjournal.TaskRestore || len(plan.GetSteps()) != 1 {
		return nil, nil, errs.New(errs.KindStateConflict, "Volume Restore claim plan is invalid")
	}
	membership, err := backupruntime.BackupRestoreEnvironmentIndexKey(task.Owner.EnvironmentID, task.ID)
	if err != nil {
		return nil, nil, err
	}
	keys := []string{backupruntime.BackupRestoreKey(task.ID), membership,
		blueprints.EnvironmentBlueprintHeadKey(task.Owner.EnvironmentID)}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, nil, errs.New(errs.KindStateConflict, "queued Volume Restore authority is unavailable")
	}
	defer etcdstore.ClearValues(read.Values)
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
			return nil, nil, errs.New(errs.KindStateConflict, "queued Volume Restore authority is incomplete")
		}
	}
	restore, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
	if err != nil || restore.TaskID != task.ID || restore.OperationID != task.OperationID ||
		restore.EnvironmentID != task.Owner.EnvironmentID || restore.State != backupruntime.BackupRestoreQueued ||
		backupruntime.ValidateVolumeRestoreExecutionPlan(restore, plan) != nil ||
		read.Values[1].Version != 1 || string(read.Values[1].Value) != task.ID ||
		read.Values[1].ModRevision != read.Values[0].ModRevision || !restore.CreatedAt.Equal(task.CreatedAt) ||
		restore.CurrentTarget.Volume == nil ||
		read.Values[2].ModRevision != restore.CurrentTarget.Volume.HeadRevision {
		return nil, nil, errs.New(errs.KindStateConflict, "queued Volume Restore does not match its Task")
	}
	headSHA := sha256.Sum256(read.Values[2].Value)
	if hex.EncodeToString(headSHA[:]) != restore.CurrentTarget.Volume.HeadSHA256 {
		return nil, nil, errs.New(errs.KindStateConflict, "Volume Restore predecessor changed before execution")
	}
	fence, err := environmentfence.LoadOwned(ctx, repository.store, restore.EnvironmentID, revision,
		environmentfence.Owner{Kind: backupruntime.BackupOperationRestore,
			OperationID: restore.OperationID, TaskID: restore.TaskID})
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
	conditions := make([]etcdstore.Condition, 0, len(keys))
	for index, key := range keys {
		conditions = append(conditions, etcdstore.Condition{Key: key, ModRevision: read.Values[index].ModRevision})
	}
	conditions, err = environmentfence.AppendConditions(conditions, fence)
	if err != nil {
		clear(value)
		clear(epoch.Value)
		return nil, nil, err
	}
	return conditions, []etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: keys[0], Value: value}, epoch}, nil
}
