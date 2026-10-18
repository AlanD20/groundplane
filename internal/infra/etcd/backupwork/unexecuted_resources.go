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
	"github.com/AlanD20/groundplane/internal/infra/etcd/postgresbackingguard"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// The caller must atomically compare either a never-assigned item or an
// assignment without a start grant. Queued resource state alone is not enough.
func (repository *InputRepository) prepareUnexecutedResources(ctx context.Context, view workqueue.View,
	outcome workexecution.Outcome, reason executionplan.BackupFailureReason, at time.Time,
) (workqueue.Change, error) {
	if view.Item.Item.Operation == workexecution.BackupPrune {
		return repository.prepareUnexecutedPruneResources(ctx, view, at)
	}
	input, change, err := repository.PrepareAdmission(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	var kind backupruntime.BackupOperationKind
	var operationID, recordKey string
	var encoded []byte
	var environments []string
	var exclusions []keyvalue.Condition
	switch input.Operation {
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE:
		run, guard, err := repository.LoadStagingCapture(ctx, StagingSource{View: view, Input: input})
		if err != nil {
			return workqueue.Change{}, err
		}
		if run.State != backupruntime.BackupRunQueued {
			return workqueue.Change{}, conflict("Unstarted Backup already has execution progress")
		}
		next, err := failedCaptureRun(run, outcome, reason, at)
		if err != nil {
			return workqueue.Change{}, err
		}
		exclusions, err = repository.captureExclusionGuards(ctx, run, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		environments, err = backupruntime.DatabaseBackingEnvironmentIDs(run)
		if err != nil {
			return workqueue.Change{}, err
		}
		encoded, err = backupruntime.EncodeBackupRunRecord(next)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, guard)
		kind, operationID, recordKey = backupruntime.BackupOperationBackup, run.OperationID, guard.Key
	case agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE:
		recordKey = backupruntime.BackupRestoreKey(view.Task.Task.ID)
		read, err := repository.readSettlementKeys(ctx, []string{recordKey}, view.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		defer keyvalue.ClearValues(read.Values)
		if read.Values[0] == nil {
			return workqueue.Change{}, conflict("Unstarted Restore record is unavailable")
		}
		record, err := backupruntime.DecodeBackupRestoreRecord(read.Values[0].Value)
		if err != nil {
			return workqueue.Change{}, err
		}
		if record.State != backupruntime.BackupRestoreQueued || record.MutationStarted || record.TaskID != view.Task.Task.ID {
			return workqueue.Change{}, conflict("Unstarted Restore already has execution progress")
		}
		if err := backupruntime.ValidateRestoreWorkInput(record, input); err != nil {
			return workqueue.Change{}, err
		}
		var next backupruntime.BackupRestoreRecord
		switch record.Point.SourceKind {
		case backupruntime.BackupRuntimeSourceConfig:
			next, err = backupruntime.FailConfigRestoreBeforeMutation(record, at)
		case backupruntime.BackupRuntimeSourceVolume:
			next, err = backupruntime.FailVolumeRestoreBeforeMutation(record, at)
		case backupruntime.BackupRuntimeSourceAttach:
			next, err = backupruntime.FailDatabaseRestoreBeforeMutation(record, at)
			if err == nil {
				var environment string
				var found bool
				environment, found, err = backupruntime.DatabaseRestoreBackingEnvironmentID(record)
				if err == nil && !found {
					err = conflict("Unstarted Restore backing ownership is missing")
				}
				environments = []string{environment}
			}
		default:
			return workqueue.Change{}, conflict("Unstarted Restore source is unsupported")
		}
		if err != nil {
			return workqueue.Change{}, err
		}
		encoded, err = backupruntime.EncodeBackupRestoreRecord(next)
		if err != nil {
			return workqueue.Change{}, err
		}
		kind, operationID = backupruntime.BackupOperationRestore, record.OperationID
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: recordKey, ModRevision: read.Values[0].ModRevision})
	default:
		return workqueue.Change{}, conflict("Unstarted Backup operation is unsupported")
	}
	defer clear(encoded)
	fence, err := environmentfence.LoadOwned(ctx, repository.store, input.EnvironmentId, view.ReadRevision,
		environmentfence.Owner{Kind: kind, OperationID: operationID, TaskID: view.Task.Task.ID})
	if err != nil {
		return workqueue.Change{}, err
	}
	backings, err := postgresbackingguard.PrepareOwnership(ctx, repository.store, environments, input.EnvironmentId,
		postgresbackingguard.Owner(kind, operationID, view.Task.Task.ID), view.ReadRevision, true)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer backings.Clear()
	epoch, err := fence.EpochRewriteMutation()
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, exclusions...)
	change.Conditions = append(change.Conditions, fence.TransactionConditions()...)
	change.Conditions = append(change.Conditions, backings.Conditions...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: recordKey, Value: append([]byte(nil), encoded...)},
		keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: hierarchy.EnvironmentOperationLockKey(input.EnvironmentId)}, epoch)
	for _, exclusion := range exclusions {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: exclusion.Key})
	}
	for ordinal := range input.Steps {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: credentialGrantKey(view.Item.Item.ID, uint32(ordinal))})
	}
	for _, mutation := range backings.Mutations {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: mutation.Type, Key: mutation.Key, Value: append([]byte(nil), mutation.Value...)})
	}
	return change, nil
}
