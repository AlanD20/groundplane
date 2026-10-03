package hierarchydeletionfinalization

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Preparer) prepareHierarchyDeletionBackupExactFinalizer(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, action.TargetID, action)
	if err != nil {
		return Effects{}, err
	}
	return Effects{fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(primary.Value),
		conditions: []keyvalue.Condition{{Key: primary.Key, ModRevision: primary.ModRevision}},
		mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: primary.Key}},
		values:     [][]byte{primary.Value}}, nil
}

func (repository *Preparer) prepareHierarchyDeletionBackupDueFinalizer(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, action.TargetID, action)
	if err != nil {
		return Effects{}, err
	}
	record, err := backupruntime.DecodeBackupDueOutcomeRecord(primary.Value)
	if err != nil {
		clear(primary.Value)
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	retentionKey, err := backupruntime.BackupDueRetentionIndexKey(
		record.RetainUntil, record.EnvironmentID, record.PolicyRevision, record.ScheduledAt,
	)
	if err != nil {
		clear(primary.Value)
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	expectedKey, err := backupruntime.BackupDueOutcomeKey(
		record.EnvironmentID, record.PolicyRevision, record.ScheduledAt,
	)
	if err != nil || expectedKey != action.TargetID {
		clear(primary.Value)
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	index, err := repository.store.Get(ctx, retentionKey)
	if err != nil {
		clear(primary.Value)
		return Effects{}, err
	}
	if index == nil || index.Entry == nil || string(index.Entry.Value) != action.TargetID {
		clear(primary.Value)
		if index != nil && index.Entry != nil {
			clear(index.Entry.Value)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	return Effects{fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(primary.Value),
		conditions: []keyvalue.Condition{{Key: primary.Key, ModRevision: primary.ModRevision},
			{Key: retentionKey, ModRevision: index.Entry.ModRevision}},
		mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: primary.Key},
			{Type: keyvalue.MutationDelete, Key: retentionKey}},
		values: [][]byte{primary.Value, index.Entry.Value}}, nil
}

func (repository *Preparer) prepareHierarchyDeletionBackupSourceFinalizer(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, backuppolicy.BackupSourceKey(action.TargetID), action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	record, err := backuppolicy.DecodeBackupSourceRecord(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, []string{
		backupruntime.BackupRecoveryPointSourcePrefix + action.TargetID + "/",
	}); err != nil {
		return Effects{}, err
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		backuppolicy.BackupSourceEnvironmentKey(record.EnvironmentID, record.ID),
		backuppolicy.BackupSourceIdentityKey(record.EnvironmentID, record.Kind, record.TargetID),
	})
	if err != nil {
		return Effects{}, err
	}
	effects.conditions = append(effects.conditions, keyvalue.Condition{
		Key: backupruntime.BackupRecoveryPointSourcePrefix + action.TargetID + "/", Prefix: true,
	})
	return effects, nil
}

func (repository *Preparer) prepareHierarchyDeletionBackupHistoryDetach(ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (Effects, error) {
	var primaryKey, indexKey string
	var valid bool
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionBackupRunDetach:
		primaryKey = backupruntime.BackupRunKey(action.TargetID)
	case hierarchydeletion.HierarchyDeletionBackupRestoreDetach:
		primaryKey = backupruntime.BackupRestoreKey(action.TargetID)
	case hierarchydeletion.HierarchyDeletionBackupKeyRotationDetach:
		primaryKey = backupruntime.BackupKeyRotationKey(action.TargetID)
	default:
		return Effects{}, errs.New(errs.KindValidationFailed, "hierarchy deletion backup history action is unsupported")
	}
	primary, err := repository.readHierarchyDeletionPrimary(ctx, primaryKey, action)
	if err != nil {
		return Effects{}, err
	}
	defer clear(primary.Value)
	environmentID, operationID := "", ""
	switch action.ActionKind {
	case hierarchydeletion.HierarchyDeletionBackupRunDetach:
		record, decodeErr := backupruntime.DecodeBackupRunRecord(primary.Value)
		valid = decodeErr == nil && record.TaskID == action.TargetID &&
			backupruntime.TerminalBackupRunState(record.State)
		environmentID = record.EnvironmentID
		operationID = record.OperationID
		indexKey, err = backupruntime.BackupRunEnvironmentIndexKey(environmentID, action.TargetID)
	case hierarchydeletion.HierarchyDeletionBackupRestoreDetach:
		record, decodeErr := backupruntime.DecodeBackupRestoreRecord(primary.Value)
		valid = decodeErr == nil && record.TaskID == action.TargetID &&
			(record.State == backupruntime.BackupRestoreCompleted || record.State == backupruntime.BackupRestoreFailedSafe)
		environmentID = record.EnvironmentID
		operationID = record.OperationID
		indexKey, err = backupruntime.BackupRestoreEnvironmentIndexKey(environmentID, action.TargetID)
	case hierarchydeletion.HierarchyDeletionBackupKeyRotationDetach:
		record, decodeErr := backupruntime.DecodeBackupKeyRotationRecord(primary.Value)
		valid = decodeErr == nil && record.TaskID == action.TargetID &&
			record.State == backupruntime.BackupKeyRotationApplied
		environmentID = record.EnvironmentID
		operationID = record.OperationID
		indexKey, err = backupruntime.BackupKeyRotationEnvironmentIndexKey(environmentID, action.TargetID)
	}
	if err != nil || !valid {
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys := []string{indexKey}
	if action.ActionKind == hierarchydeletion.HierarchyDeletionBackupKeyRotationDetach {
		keys = append(keys, taskjournal.TaskStorageKey(action.TargetID))
	} else {
		keys = append(keys, backupruntime.BackupTerminalReceiptKey(action.TargetID))
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys})
	if err != nil {
		return Effects{}, err
	}
	if read == nil || len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil ||
		string(read.Values[0].Value) != action.TargetID {
		if read != nil {
			keyvalue.ClearValues(read.Values)
		}
		return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer keyvalue.ClearValues(read.Values)
	if action.ActionKind != hierarchydeletion.HierarchyDeletionBackupKeyRotationDetach {
		receipt, decodeErr := backupruntime.DecodeBackupTerminalReceiptRecord(read.Values[1].Value)
		expectedType := taskjournal.TaskBackup
		if action.ActionKind == hierarchydeletion.HierarchyDeletionBackupRestoreDetach {
			expectedType = taskjournal.TaskRestore
		}
		if decodeErr != nil || receipt.Task.TaskID != action.TargetID || receipt.Task.TaskType != expectedType ||
			receipt.Task.Owner.EnvironmentID != environmentID || receipt.Task.OperationID != operationID {
			return Effects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
	}
	return Effects{fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(primary.Value),
		conditions: []keyvalue.Condition{{Key: primary.Key, ModRevision: primary.ModRevision},
			{Key: indexKey, ModRevision: read.Values[0].ModRevision},
			{Key: keys[1], ModRevision: read.Values[1].ModRevision}},
		mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: indexKey}}}, nil
}
