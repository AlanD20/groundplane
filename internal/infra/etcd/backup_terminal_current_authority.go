package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
)

func (repository *TaskRepository) validateCurrentBackupTerminalAuthority(
	ctx context.Context,
	task TaskRecord,
	terminalRevision int64,
	receipt backupruntime.BackupTerminalReceiptRecord,
) error {
	environmentID := receipt.Task.Owner.EnvironmentID
	keys := []string{
		hierarchyrecord.EnvironmentKey(environmentID),
		hierarchyrecord.EnvironmentMutationEpochKey(environmentID),
		hierarchyrecord.EnvironmentOperationLockKey(environmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environmentID),
		backupruntime.BackupRecoveryPointPruneDispatchKey(receipt.Task.TaskID),
	}
	runIndex, membershipIndex, exclusionsStart := -1, -1, -1
	var exclusionKeys []string
	if receipt.Task.TaskType == taskjournal.TaskBackup {
		membershipKey, err := backupruntime.BackupRunEnvironmentIndexKey(environmentID, receipt.Task.TaskID)
		if err != nil {
			return err
		}
		exclusionKeys, err = backupTerminalExclusionKeys(receipt.Sources)
		if err != nil {
			return err
		}
		runIndex = len(keys)
		keys = append(keys, backupruntime.BackupRunKey(receipt.Task.TaskID))
		membershipIndex = len(keys)
		keys = append(keys, membershipKey)
		exclusionsStart = len(keys)
		keys = append(keys, exclusionKeys...)
	}
	authorityEnd := len(keys)
	pointsStart := len(keys)
	for _, outcome := range receipt.Points {
		pointKeys, err := backupPruneAuthorityKeys(outcome.Point)
		if err != nil {
			return err
		}
		keys = append(keys, pointKeys...)
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys})
	if err != nil {
		return err
	}
	if read == nil || read.ReadRevision <= 0 || len(read.Values) != len(keys) {
		return errs.New(errs.KindInternal, "backup terminal authority read is incomplete")
	}
	defer etcdstore.ClearValues(read.Values)
	ownerPresent, successorLock, deletionOwner, err := repository.validateBackupTerminalOwnerSnapshot(
		ctx,
		read.ReadRevision,
		terminalRevision,
		receipt,
		read.Values[:authorityEnd],
	)
	if err != nil {
		return err
	}
	if receipt.Task.TaskType == taskjournal.TaskBackup {
		runValue, membershipValue := read.Values[runIndex], read.Values[membershipIndex]
		exclusionValues := read.Values[exclusionsStart:authorityEnd]
		if !ownerPresent {
			return nil
		}
		if runValue == nil || membershipValue == nil {
			if runValue != nil || membershipValue != nil || !deletionOwner ||
				!allBackupRuntimeValuesAbsent(exclusionValues) {
				return errs.New(errs.KindStateConflict, "terminal backup owner authority is torn")
			}
			return nil
		}
		if membershipValue.Key != keys[membershipIndex] || membershipValue.Version != 1 ||
			membershipValue.ModRevision > terminalRevision ||
			string(membershipValue.Value) != receipt.Task.TaskID {
			return errs.New(errs.KindStateConflict, "terminal backup run membership changed")
		}
		run, err := backupruntime.DecodeBackupRunRecord(runValue.Value)
		if err != nil {
			return err
		}
		if runValue.ModRevision != terminalRevision {
			return errs.New(errs.KindStateConflict, "terminal backup run binding changed")
		}
		if ValidateBackupRunTaskBinding(task, run) != nil {
			return errs.New(errs.KindInternal, "same-revision terminal backup run binding is invalid")
		}
		value, err := backupruntime.EncodeBackupRunRecord(run)
		if err != nil {
			return err
		}
		defer clear(value)
		digest := sha256.Sum256(append([]byte("groundplane.backup.terminal.run.v1\x00"), value...))
		if hex.EncodeToString(digest[:]) != receipt.DomainDigest {
			return errs.New(errs.KindInternal, "same-revision terminal backup run digest is invalid")
		}
		outcomes := backupTerminalRunOutcomes(run)
		if !slices.Equal(outcomes, receipt.Sources) {
			return errs.New(errs.KindInternal, "same-revision terminal backup source outcomes are invalid")
		}
		for index, exclusionValue := range exclusionValues {
			if exclusionValue == nil {
				continue
			}
			exclusion, err := backupruntime.DecodeBackupSourceTargetExclusionRecord(exclusionValue.Value)
			if err != nil {
				return err
			}
			exclusionKey, keyErr := backupruntime.BackupSourceTargetExclusionKey(
				exclusion.TargetKind,
				exclusion.TargetID,
			)
			if keyErr != nil || exclusion.EnvironmentID != environmentID ||
				exclusion.TaskID == receipt.Task.TaskID || successorLock == nil ||
				exclusion.OperationID != successorLock.OperationID ||
				exclusion.TaskID != successorLock.TaskID ||
				exclusion.OperationKind != successorLock.Kind ||
				exclusionValue.ModRevision != read.Values[2].ModRevision ||
				exclusionValue.Key != exclusionKeys[index] || exclusionKey != exclusionValue.Key {
				return errs.New(errs.KindStateConflict, "terminal backup exclusion authority changed")
			}
		}
		return nil
	}
	for index, outcome := range receipt.Points {
		values := read.Values[pointsStart+index*5 : pointsStart+index*5+5]
		if outcome.Outcome == backupruntime.BackupPruneTerminalRemoved {
			if !allBackupRuntimeValuesAbsent(values) {
				return errs.New(
					errs.KindStateConflict,
					"removed terminal backup prune authority was reconstructed",
				)
			}
			continue
		}
		if values[0] == nil {
			if !allBackupRuntimeValuesAbsent(values) {
				return errs.New(
					errs.KindStateConflict,
					"terminal backup prune point authority is torn",
				)
			}
			continue
		}
		if !ownerPresent {
			return errs.New(errs.KindStateConflict, "deleted backup owner retained point authority")
		}
		prune, err := backupruntime.DecodeBackupRecoveryPointPruneRecord(values[0].Value)
		if err != nil {
			return err
		}
		if values[0].ModRevision == terminalRevision {
			if outcome.Outcome != backupruntime.BackupPruneTerminalRetained || prune.Point != outcome.Point ||
				prune.OperationID != receipt.Task.OperationID || prune.State != backupruntime.BackupPrunePending ||
				prune.TaskID != "" || prune.CreatedAt != outcome.CreatedAt ||
				!prune.UpdatedAt.Equal(receipt.Task.FinishedAt) {
				return errs.New(errs.KindInternal, "same-revision backup prune outcome is invalid")
			}
			if err := validatePendingBackupPruneAuthority(
				values,
				etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
					Record:   prune,
					Revision: values[0].ModRevision,
				},
			); err != nil {
				return errs.New(errs.KindInternal, "same-revision backup prune authority is invalid")
			}
			continue
		}
		if values[0].ModRevision < terminalRevision || prune.TaskID == receipt.Task.TaskID {
			return errs.New(errs.KindStateConflict, "stale terminal backup prune owner remains")
		}
		if prune.Point != outcome.Point || prune.CreatedAt != outcome.CreatedAt {
			return errs.New(errs.KindStateConflict, "terminal backup prune successor changed")
		}
		if err := validatePendingBackupPruneAuthority(
			values,
			etcdstore.Versioned[backupruntime.BackupRecoveryPointPruneRecord]{
				Record: prune, Revision: values[0].ModRevision,
			},
		); err != nil {
			return err
		}
		if prune.TaskID == "" {
			if prune.State != backupruntime.BackupPrunePending {
				return errs.New(errs.KindStateConflict, "terminal backup prune successor changed")
			}
			continue
		}
		if successorLock == nil || successorLock.Kind != backupruntime.BackupOperationPrune ||
			successorLock.OperationID != prune.OperationID || successorLock.TaskID != prune.TaskID {
			return errs.New(errs.KindStateConflict, "terminal backup prune successor ownership changed")
		}
		dispatchRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys:     []string{backupruntime.BackupRecoveryPointPruneDispatchKey(prune.TaskID)},
			Revision: read.ReadRevision,
		})
		if err != nil {
			return err
		}
		if dispatchRead == nil || dispatchRead.ReadRevision != read.ReadRevision ||
			len(dispatchRead.Values) != 1 || dispatchRead.Values[0] == nil {
			if dispatchRead != nil {
				etcdstore.ClearValues(dispatchRead.Values)
			}
			return errs.New(errs.KindStateConflict, "terminal backup prune successor dispatch is missing")
		}
		dispatchRevision := dispatchRead.Values[0].ModRevision
		dispatch, err := backupruntime.DecodeBackupRecoveryPointPruneDispatchRecord(dispatchRead.Values[0].Value)
		etcdstore.ClearValues(dispatchRead.Values)
		if err != nil {
			return err
		}
		if dispatch.EnvironmentID != environmentID || dispatch.OperationID != prune.OperationID ||
			dispatch.TaskID != prune.TaskID || !dispatch.CreatedAt.Equal(successorLock.CreatedAt) ||
			dispatchRevision != values[0].ModRevision ||
			values[0].ModRevision != read.Values[2].ModRevision ||
			!slices.Contains(dispatch.RecoveryPointIDs, prune.Point.ID) {
			return errs.New(errs.KindStateConflict, "terminal backup prune successor dispatch changed")
		}
	}
	return nil
}

func backupTerminalExclusionKeys(
	sources []backupruntime.BackupTerminalSourceOutcome,
) ([]string, error) {
	byKey := make(map[string]struct{})
	for _, source := range sources {
		var kind backupruntime.BackupSourceTargetKind
		switch source.Kind {
		case backupruntime.BackupRuntimeSourceAttach:
			kind = backupruntime.BackupSourceTargetAttach
		case backupruntime.BackupRuntimeSourceVolume:
			kind = backupruntime.BackupSourceTargetVolume
		case backupruntime.BackupRuntimeSourceConfig:
			continue
		default:
			return nil, errs.New(errs.KindInternal, "backup terminal source kind is invalid")
		}
		key, err := backupruntime.BackupSourceTargetExclusionKey(kind, source.TargetID)
		if err != nil {
			return nil, err
		}
		byKey[key] = struct{}{}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys, nil
}
