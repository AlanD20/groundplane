package etcd

import (
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"slices"
	"time"
)

func backupRunExclusionRecords(
	run BackupRunRecord,
	at time.Time,
) ([]BackupSourceTargetExclusionRecord, error) {
	if validateBackupRunRecord(run) != nil || !validBackupRuntimeInstant(at) {
		return nil, errs.New(errs.KindValidationFailed, "backup run exclusion input is invalid")
	}
	byKey := make(map[string]BackupSourceTargetExclusionRecord)
	add := func(kind BackupSourceTargetKind, targetID string) error {
		key, err := backupSourceTargetExclusionKey(kind, targetID)
		if err != nil {
			return err
		}
		byKey[key] = BackupSourceTargetExclusionRecord{
			EnvironmentID: run.EnvironmentID,
			OperationID:   run.OperationID,
			TaskID:        run.TaskID,
			OperationKind: BackupOperationBackup,
			TargetKind:    kind,
			TargetID:      targetID,
			CreatedAt:     at,
			UpdatedAt:     at,
		}
		return nil
	}
	for _, source := range run.Sources {
		switch source.Kind {
		case BackupRuntimeSourceAttach:
			if err := add(BackupSourceTargetAttach, source.TargetID); err != nil {
				return nil, err
			}
		case BackupRuntimeSourceVolume:
			if err := add(BackupSourceTargetVolume, source.TargetID); err != nil {
				return nil, err
			}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	result := make([]BackupSourceTargetExclusionRecord, len(keys))
	for index, key := range keys {
		result[index] = byKey[key]
	}
	return result, nil
}

func exactBackupExclusions(
	values []*etcdstore.KeyValue,
	records []BackupSourceTargetExclusionRecord,
	resultRevision int64,
) bool {
	if len(values) != len(records) {
		return false
	}
	for index, value := range values {
		if value == nil || value.ModRevision != resultRevision {
			return false
		}
		stored, err := decodeBackupSourceTargetExclusionRecord(value.Value)
		if err != nil || !sameBackupExclusionOwner(stored, records[index]) {
			return false
		}
	}
	return true
}

func sameBackupExclusionOwner(
	left BackupSourceTargetExclusionRecord,
	right BackupSourceTargetExclusionRecord,
) bool {
	return left.EnvironmentID == right.EnvironmentID && left.OperationID == right.OperationID &&
		left.TaskID == right.TaskID && left.OperationKind == right.OperationKind &&
		left.TargetKind == right.TargetKind && left.TargetID == right.TargetID
}
