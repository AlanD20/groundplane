package backupwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PruneSettlementFits reserves the final joined resource/execution decision,
// not just admission. Each object needs seven absence comparisons and two
// deletes. The fixed reserve covers input/progress, dispatch, ancestry/epoch,
// native attempt/item/slot/claims/host ownership and the four authentication
// guards. Selection may span multiple dispatches; no Point is dropped.
func PruneSettlementFits(input *agentpb.BackupWorkInput) bool {
	const fixedOperations, operationsPerObject = 32, 9
	return input != nil && input.Operation == agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE &&
		len(input.Steps) > 0 && fixedOperations+operationsPerObject*len(input.Steps) <= keyvalue.MaximumOperations
}

// PreparePruneCompletion releases resource authority only alongside native
// terminal settlement. Checkpoint receipts and captured input remain retained
// for acknowledgement replay; credential grants no longer serve a live source.
func (repository *InputRepository) PreparePruneCompletion(
	ctx context.Context,
	view workqueue.ResumeView,
) (workqueue.Change, error) {
	assignment, change, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Attempt.Attempt.StartedAt == nil ||
		assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return workqueue.Change{}, conflict("Backup prune completion requires a started prune attempt")
	}
	dispatchKey := backupruntime.BackupRecoveryPointPruneDispatchKey(view.Work.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{dispatchKey}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Backup prune dispatch is unavailable")
	}
	dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if dispatch.TaskID != view.Work.Task.Task.ID || dispatch.EnvironmentID != assignment.Input.EnvironmentId ||
		len(dispatch.RecoveryPointIDs) != len(assignment.Sources) {
		return workqueue.Change{}, conflict("Backup prune completion differs from its captured sources")
	}
	keys := make([]string, len(dispatch.RecoveryPointIDs))
	for index, pointID := range dispatch.RecoveryPointIDs {
		keys[index] = backupruntime.BackupRecoveryPointPruneKey(pointID)
	}
	points, err := repository.readSettlementKeys(ctx, keys, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(points.Values)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: dispatchKey, ModRevision: read.Values[0].ModRevision},
	)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: dispatchKey})
	for index, value := range points.Values {
		if value == nil {
			return workqueue.Change{}, conflict("Backup prune source has no absence receipt")
		}
		prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(value.Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		resume := assignment.Sources[index].GetPrune()
		if prune.State != backupruntime.BackupPruneVerifiedAbsent || prune.TaskID != dispatch.TaskID ||
			prune.OperationID != dispatch.OperationID || prune.Point.EnvironmentID != dispatch.EnvironmentID ||
			prune.Point.ID != dispatch.RecoveryPointIDs[index] || resume == nil || resume.CheckpointSequence != 1 ||
			resume.PrecedingCheckpoint == nil || resume.PrecedingCheckpoint.DedupeKeyModRevision != value.ModRevision ||
			!backupruntime.BackupPruneDeletedMatchesObject(
				resume.GetObjectDeleted(),
				uint32(index),
				prune.Point.ID,
				prune.Point.Object,
			) {
			return workqueue.Change{}, conflict("Backup prune has not durably proved every captured object absent")
		}
		authority, err := backupruntime.BackupPruneAuthorityKeys(prune.Point)
		if err != nil {
			return workqueue.Change{}, err
		}
		absentKeys := append(authority[1:], backupruntime.BackupRetentionKey(prune.Point.SourceID, prune.Point.ID),
			backupruntime.BackupRecoveryPointCaptureKey(prune.Point.ID))
		absent, err := repository.readSettlementKeys(ctx, absentKeys, view.Work.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		for _, retained := range absent.Values {
			if retained != nil {
				keyvalue.ClearValues(absent.Values)
				return workqueue.Change{}, conflict("Backup prune still retains Recovery Point ownership")
			}
		}
		keyvalue.ClearValues(absent.Values)
		for _, key := range absentKeys {
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: key})
		}
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision},
		)
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[index]},
			keyvalue.Mutation{
				Type: keyvalue.MutationDelete,
				Key:  credentialGrantKey(view.Work.Item.Item.ID, uint32(index)),
			},
		)
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
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
		Key: hierarchy.EnvironmentOperationLockKey(dispatch.EnvironmentID)})
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, epoch)
	return change, nil
}
