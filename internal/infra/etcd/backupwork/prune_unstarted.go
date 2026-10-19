package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PrepareUnstartedStop releases never-executed resource reservations. It cannot
// release ownership for assigned or retried work.
func (repository *InputRepository) PrepareUnstartedStop(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if (item.Operation != workexecution.BackupPrune && item.Operation != workexecution.Backup && item.Operation != workexecution.Restore) ||
		item.Input == nil ||
		item.AttemptNumber != 0 ||
		item.ActiveAttemptID != "" ||
		item.Cleanup != workexecution.CleanupNone ||
		!task.PublicationComplete ||
		task.FinishedAt != nil ||
		task.CollectionStartedAt != nil ||
		!backupruntime.ValidBackupRuntimeInstant(at) ||
		at.Before(item.UpdatedAt) {
		return workqueue.Change{}, conflict("Backup work cannot settle as never assigned")
	}
	outcome := workexecution.Failed
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if at.Before(task.Deadline) && at.Before(item.Deadline) {
		return workqueue.Change{}, conflict("Backup work has no stop or expired deadline")
	}
	resources, err := repository.prepareUnexecutedResources(ctx, view, outcome, executionplan.BackupDeadlineExpired, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedSettlement(view.Task, view.Item,
		workexecution.Completion{Outcome: outcome, Evidence: *item.Input, EffectsSettled: true}, at)
	if err != nil {
		keyvalue.ClearMutationValues(resources.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, resources.Conditions...)
	change.Mutations = append(change.Mutations, resources.Mutations...)
	return change, nil
}

// Callers must establish either no assignment or no start grant, and compare
// that execution state in the transaction that releases these selections.
func (repository *InputRepository) prepareUnexecutedPruneResources(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
) (workqueue.Change, error) {
	task := view.Task.Task
	input, guards, err := repository.PrepareAdmission(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE {
		return workqueue.Change{}, conflict("Backup operation is not prune")
	}
	dispatchKey := backupruntime.BackupRecoveryPointPruneDispatchKey(task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{dispatchKey}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{}, conflict("Backup prune dispatch is missing")
	}
	dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if dispatch.TaskID != task.ID || dispatch.EnvironmentID != input.EnvironmentId ||
		len(dispatch.RecoveryPointIDs) != len(input.Steps) {
		return workqueue.Change{}, conflict("Backup prune dispatch differs from captured input")
	}
	change := guards
	for ordinal, pointID := range dispatch.RecoveryPointIDs {
		key := backupruntime.BackupRecoveryPointPruneKey(pointID)
		read, err := repository.readSettlementKeys(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		if read.Values[0] == nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, conflict("Backup prune selection is missing")
		}
		prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(read.Values[0].Value)
		revision := read.Values[0].ModRevision
		keyvalue.ClearValues(read.Values)
		if err != nil || prune.State != backupruntime.BackupPruneAssigned || prune.TaskID != task.ID ||
			prune.OperationID != dispatch.OperationID || prune.Point.ID != pointID || prune.Point.EnvironmentID != input.EnvironmentId ||
			!at.After(prune.UpdatedAt) {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, conflict("Backup prune selection no longer belongs to unstarted work")
		}
		objects := input.Steps[ordinal].GetPrune().GetObjects()
		if len(objects) != 1 || objects[0].PointId != pointID {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, conflict("Backup prune selection differs from captured source order")
		}
		authorityKeys, err := backupruntime.BackupPruneAuthorityKeys(prune.Point)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		authority, err := repository.readSettlementKeys(ctx, authorityKeys, view.ReadRevision)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		err = backupruntime.ValidatePendingBackupPruneAuthority(authority.Values,
			keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record: prune, Revision: revision, ReadRevision: view.ReadRevision,
			})
		if err != nil {
			keyvalue.ClearValues(authority.Values)
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		for index, authorityKey := range authorityKeys {
			change.Conditions = append(change.Conditions, keyvalue.Condition{
				Key: authorityKey, ModRevision: keyvalue.RevisionOf(authority.Values[index]),
			})
		}
		keyvalue.ClearValues(authority.Values)
		prune.State, prune.TaskID = backupruntime.BackupPrunePending, ""
		prune.UpdatedAt = at
		encoded, err := backupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
		if err != nil {
			keyvalue.ClearMutationValues(change.Mutations)
			return workqueue.Change{}, err
		}
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded},
		)
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		input.EnvironmentId,
		view.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationPrune,
			OperationID: dispatch.OperationID,
			TaskID:      task.ID,
		},
	)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Mutations = append(
		change.Mutations,
		epoch,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: dispatchKey},
		keyvalue.Mutation{
			Type: keyvalue.MutationDelete,
			Key:  hierarchy.EnvironmentOperationLockKey(input.EnvironmentId),
		},
	)
	return change, nil
}
