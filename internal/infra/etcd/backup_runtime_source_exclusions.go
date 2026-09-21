package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
	"time"
)

func backupRunExclusionRecords(
	run backupruntime.BackupRunRecord,
	at time.Time,
) ([]backupruntime.BackupSourceTargetExclusionRecord, error) {
	if backupruntime.ValidateBackupRunRecord(run) != nil || !backupruntime.ValidBackupRuntimeInstant(at) {
		return nil, errs.New(errs.KindValidationFailed, "backup run exclusion input is invalid")
	}
	byKey := make(map[string]backupruntime.BackupSourceTargetExclusionRecord)
	add := func(kind backupruntime.BackupSourceTargetKind, targetID string) error {
		key, err := backupruntime.BackupSourceTargetExclusionKey(kind, targetID)
		if err != nil {
			return err
		}
		byKey[key] = backupruntime.BackupSourceTargetExclusionRecord{
			EnvironmentID: run.EnvironmentID,
			OperationID:   run.OperationID,
			TaskID:        run.TaskID,
			OperationKind: backupruntime.BackupOperationBackup,
			TargetKind:    kind,
			TargetID:      targetID,
			CreatedAt:     at,
			UpdatedAt:     at,
		}
		return nil
	}
	for _, source := range run.Sources {
		switch source.Kind {
		case backupruntime.BackupRuntimeSourceAttach:
			if err := add(backupruntime.BackupSourceTargetAttach, source.TargetID); err != nil {
				return nil, err
			}
		case backupruntime.BackupRuntimeSourceVolume:
			if err := add(backupruntime.BackupSourceTargetVolume, source.TargetID); err != nil {
				return nil, err
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]backupruntime.BackupSourceTargetExclusionRecord, len(keys))
	for index, key := range keys {
		result[index] = byKey[key]
	}
	return result, nil
}

func exactBackupExclusions(
	values []*etcdstore.KeyValue,
	records []backupruntime.BackupSourceTargetExclusionRecord,
	resultRevision int64,
) bool {
	if len(values) != len(records) {
		return false
	}
	for index, value := range values {
		if value == nil || value.ModRevision != resultRevision {
			return false
		}
		stored, err := backupruntime.DecodeBackupSourceTargetExclusionRecord(value.Value)
		if err != nil || !sameBackupExclusionOwner(stored, records[index]) {
			return false
		}
	}
	return true
}

func sameBackupExclusionOwner(
	left backupruntime.BackupSourceTargetExclusionRecord,
	right backupruntime.BackupSourceTargetExclusionRecord,
) bool {
	return left.EnvironmentID == right.EnvironmentID && left.OperationID == right.OperationID &&
		left.TaskID == right.TaskID && left.OperationKind == right.OperationKind &&
		left.TargetKind == right.TargetKind && left.TargetID == right.TargetID
}
