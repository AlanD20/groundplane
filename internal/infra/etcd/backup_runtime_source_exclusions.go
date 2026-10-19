package etcd

import (
	"slices"
	"time"

	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/pkg/errs"
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
