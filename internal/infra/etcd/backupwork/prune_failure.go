package backupwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreparePruneFailure returns incomplete deletions to resource ownership. The
// caller must join this change with authenticated executor-stop settlement; a
// deadline or lost connection alone cannot authorize its commit. A deleted
// remote object is never described as restored, even when its report was lost.
func (repository *InputRepository) PreparePruneFailure(ctx context.Context, view workqueue.ResumeView,
	at time.Time,
) (workqueue.Change, error) {
	assignment, change, err := repository.LoadAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if assignment.Input.Operation != agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE ||
		view.Attempt.Attempt.StartedAt == nil || view.Attempt.Attempt.FinishedAt != nil ||
		!backupruntime.ValidBackupRuntimeInstant(at) || at.Before(*view.Attempt.Attempt.StartedAt) {
		return workqueue.Change{}, conflict("Backup prune failure requires a started unsettled attempt")
	}
	resources, err := repository.preparePruneReleasedResources(ctx, view.Work, assignment.Input, assignment.Sources, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	resources.Conditions = append(resources.Conditions, change.Conditions...)
	return resources, nil
}

// The caller supplies authenticated stop proof and joins execution settlement.
// This shared resource operation makes no assumptions about attempt direction.
func (repository *InputRepository) preparePruneReleasedResources(ctx context.Context, view workqueue.View,
	input *agentpb.BackupWorkInput, sources []*agentpb.BackupStepResume, at time.Time,
) (workqueue.Change, error) {
	change := workqueue.Change{}
	dispatchKey := backupruntime.BackupRecoveryPointPruneDispatchKey(view.Task.Task.ID)
	read, err := repository.readSettlementKeys(ctx, []string{dispatchKey}, view.ReadRevision)
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
	if dispatch.TaskID != view.Task.Task.ID || dispatch.EnvironmentID != input.EnvironmentId ||
		len(dispatch.RecoveryPointIDs) != len(sources) || !at.After(dispatch.CreatedAt) {
		return workqueue.Change{}, conflict("Backup prune failure differs from captured ownership")
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: dispatchKey, ModRevision: read.Values[0].ModRevision},
	)
	prepared := false
	defer func() {
		if !prepared {
			keyvalue.ClearMutationValues(change.Mutations)
		}
	}()
	for ordinal, pointID := range dispatch.RecoveryPointIDs {
		key := backupruntime.BackupRecoveryPointPruneKey(pointID)
		selected, err := repository.readSettlementKeys(ctx, []string{key}, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		if selected.Values[0] == nil {
			return workqueue.Change{}, conflict("Backup prune selection is missing")
		}
		prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(selected.Values[0].Value)
		revision := selected.Values[0].ModRevision
		keyvalue.ClearValues(selected.Values)
		if err != nil {
			return workqueue.Change{}, err
		}
		resume := sources[ordinal].GetPrune()
		objects := input.Steps[ordinal].GetPrune().GetObjects()
		if prune.TaskID != dispatch.TaskID || prune.OperationID != dispatch.OperationID ||
			prune.Point.EnvironmentID != dispatch.EnvironmentID || prune.Point.ID != pointID ||
			!at.After(prune.UpdatedAt) || resume == nil || len(objects) != 1 || objects[0].PointId != pointID {
			return workqueue.Change{}, conflict("Backup prune failure source differs from captured ownership")
		}
		authorityKeys, err := backupruntime.BackupPruneAuthorityKeys(prune.Point)
		if err != nil {
			return workqueue.Change{}, err
		}
		authority, err := repository.readSettlementKeys(ctx, authorityKeys, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		err = validateStoppedPruneSource(authority.Values, prune, revision, view.ReadRevision, resume, uint32(ordinal))
		if err != nil {
			keyvalue.ClearValues(authority.Values)
			return workqueue.Change{}, err
		}
		for index, authorityKey := range authorityKeys {
			change.Conditions = append(change.Conditions, keyvalue.Condition{Key: authorityKey,
				ModRevision: keyvalue.RevisionOf(authority.Values[index])})
		}
		keyvalue.ClearValues(authority.Values)
		if prune.State == backupruntime.BackupPruneVerifiedAbsent {
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: key})
		} else {
			prune.State, prune.TaskID, prune.UpdatedAt = backupruntime.BackupPrunePending, "", at
			encoded, err := backupruntime.EncodeBackupRecoveryPointPruneRecord(prune)
			if err != nil {
				return workqueue.Change{}, err
			}
			change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: key, Value: encoded})
		}
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
			Key: credentialGrantKey(view.Item.Item.ID, uint32(ordinal))})
	}
	fence, err := environmentfence.LoadOwned(
		ctx,
		repository.store,
		dispatch.EnvironmentID,
		view.ReadRevision,
		environmentfence.Owner{
			Kind:        backupruntime.BackupOperationPrune,
			OperationID: dispatch.OperationID,
			TaskID:      dispatch.TaskID,
		},
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Mutations = append(
		change.Mutations,
		epoch,
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: dispatchKey},
		keyvalue.Mutation{
			Type: keyvalue.MutationDelete,
			Key:  hierarchy.EnvironmentOperationLockKey(dispatch.EnvironmentID),
		},
	)
	prepared = true
	return change, nil
}

func validateStoppedPruneSource(values []*keyvalue.KeyValue, prune backupruntime.BackupRecoveryPointPruneRecord,
	revision, readRevision int64, resume *agentpb.BackupPruneResume, ordinal uint32,
) error {
	if prune.State == backupruntime.BackupPruneAssigned {
		if resume.CheckpointSequence != 0 || resume.GetObjectDeleted() != nil {
			return conflict("Backup prune progress claims absence without resource settlement")
		}
		return backupruntime.ValidatePendingBackupPruneAuthority(
			values,
			keyvalue.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record:       prune,
				Revision:     revision,
				ReadRevision: readRevision,
			},
		)
	}
	if prune.State != backupruntime.BackupPruneVerifiedAbsent || resume.CheckpointSequence != 1 ||
		resume.PrecedingCheckpoint == nil || resume.PrecedingCheckpoint.DedupeKeyModRevision != revision ||
		!backupruntime.BackupPruneDeletedMatchesObject(
			resume.GetObjectDeleted(),
			ordinal,
			prune.Point.ID,
			prune.Point.Object,
		) {
		return conflict("Backup prune absence has no matching durable checkpoint")
	}
	if len(values) != 5 || values[0] == nil || values[0].ModRevision != revision {
		return conflict("Backup prune absence ownership changed")
	}
	for _, value := range values[1:] {
		if value != nil {
			return conflict("Backup prune absence still retains Recovery Point ownership")
		}
	}
	return nil
}
