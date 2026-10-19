package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupvolumecleanup"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreparePruneSettlement must be committed with PrepareCheckpoint. It removes
// resource ownership only after exact remote absence, never on Task completion
// alone. All reads and comparisons belong to the admitted attempt snapshot.
func (repository *InputRepository) PreparePruneSettlement(ctx context.Context, view workqueue.ResumeView,
	admitted CheckpointAdmission, at time.Time,
) (workqueue.Change, error) {
	if admitted.Replay != nil || !backupruntime.ValidBackupRuntimeInstant(at) ||
		admitted.Assignment == nil || admitted.Assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return workqueue.Change{}, conflict("Backup prune settlement requires a new absence checkpoint")
	}
	if _, err := executionplan.AdvanceBackupPrune(admitted.Assignment, admitted.Request, view.Attempt.Attempt.Executor); err != nil {
		return workqueue.Change{}, err
	}
	deleted := admitted.Request.GetPruneObjectDeleted()
	keys := []string{
		backupruntime.BackupRecoveryPointPruneDispatchKey(view.Work.Task.Task.ID),
		backupruntime.BackupRecoveryPointPruneKey(deleted.PointId),
	}
	read, err := repository.readSettlementKeys(ctx, keys, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil || read.Values[1] == nil {
		return workqueue.Change{}, conflict("Backup prune ownership is missing")
	}
	dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	current, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(read.Values[1].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	ordinal, found := backupruntime.BackupPruneDispatchPointOrdinal(dispatch, deleted.PointId)
	if !found || dispatch.TaskID != view.Work.Task.Task.ID || current.TaskID != dispatch.TaskID ||
		current.OperationID != dispatch.OperationID || current.State != backupruntime.BackupPruneAssigned ||
		current.Point.EnvironmentID != dispatch.EnvironmentID || dispatch.EnvironmentID != admitted.Assignment.Input.EnvironmentId ||
		!backupruntime.BackupPruneDeletedMatchesObject(deleted, ordinal, current.Point.ID, current.Point.Object) {
		return workqueue.Change{}, conflict("Backup prune report differs from retained Recovery Point ownership")
	}
	authorityKeys, err := backupruntime.BackupPruneAuthorityKeys(current.Point)
	if err != nil {
		return workqueue.Change{}, err
	}
	authorityKeys = append(authorityKeys, backupruntime.BackupRetentionKey(current.Point.SourceID, current.Point.ID),
		backupruntime.BackupRecoveryPointCaptureKey(current.Point.ID))
	anchor, err := repository.readSettlementKeys(ctx, authorityKeys, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(anchor.Values)
	if err := backupruntime.ValidatePendingBackupPruneAuthority(anchor.Values[:5], keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
		Record: current, Revision: read.Values[1].ModRevision, ReadRevision: view.Work.ReadRevision,
	}); err != nil {
		return workqueue.Change{}, err
	}
	if err := backupruntime.ValidateCompletedBackupRetentionSweep(anchor.Values[5], current.Point, anchor.Values[1].ModRevision); err != nil {
		return workqueue.Change{}, err
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		dispatch.EnvironmentID,
		view.Work.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationPrune,
			OperationID: dispatch.OperationID,
			TaskID:      dispatch.TaskID,
		},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	next := current
	next.State, next.UpdatedAt = backupruntime.BackupPruneVerifiedAbsent, at
	if !next.UpdatedAt.After(current.UpdatedAt) {
		next.UpdatedAt = current.UpdatedAt.Add(time.Nanosecond)
	}
	encoded, err := backupruntime.EncodeBackupRecoveryPointPruneRecord(next)
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: keys[0], ModRevision: read.Values[0].ModRevision}},
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: authorityKeys[0], Value: encoded}},
	}
	for index, key := range authorityKeys {
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(anchor.Values[index])},
		)
		if index > 0 {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		}
	}
	conditions, mutations, err := backupvolumecleanup.Prepare(
		ctx,
		repository.store,
		current.Point,
		view.Work.ReadRevision,
	)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	change.Mutations = append(change.Mutations, mutations...)
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, epoch)
	return change, nil
}

func (repository *InputRepository) readSettlementKeys(
	ctx context.Context,
	keys []string,
	revision int64,
) (*keyvalue.GetManyResult, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, conflict("Backup settlement snapshot is unavailable")
	}
	if revision <= 0 || read.ReadRevision != revision || len(read.Values) != len(keys) {
		keyvalue.ClearValues(read.Values)
		return nil, conflict("Backup settlement snapshot changed")
	}
	for index, value := range read.Values {
		if value != nil && (value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision) {
			keyvalue.ClearValues(read.Values)
			return nil, conflict("Backup settlement record is outside its snapshot")
		}
	}
	return read, nil
}
