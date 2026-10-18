package backupwork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

func (repository *InputRepository) captureExclusionGuards(ctx context.Context, run backupruntime.BackupRunRecord,
	revision int64,
) ([]keyvalue.Condition, error) {
	targets := make(map[string]backupruntime.BackupRunSourceAttemptRecord)
	for _, source := range run.Sources {
		var kind backupruntime.BackupSourceTargetKind
		switch source.Kind {
		case backupruntime.BackupRuntimeSourceAttach:
			kind = backupruntime.BackupSourceTargetAttach
		case backupruntime.BackupRuntimeSourceVolume:
			kind = backupruntime.BackupSourceTargetVolume
		default:
			continue
		}
		key, err := backupruntime.BackupSourceTargetExclusionKey(kind, source.TargetID)
		if err != nil {
			return nil, err
		}
		targets[key] = source
	}
	if len(targets) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(targets))
	for key := range targets {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil {
		return nil, conflict("Backup source exclusions are unavailable")
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != len(keys) {
		return nil, conflict("Backup source exclusion snapshot is incomplete")
	}
	guards := make([]keyvalue.Condition, len(keys))
	for index, value := range read.Values {
		if value == nil || value.Key != keys[index] || value.ModRevision <= 0 || value.ModRevision > revision {
			return nil, conflict("Backup source exclusion is missing")
		}
		record, err := backupruntime.DecodeBackupSourceTargetExclusionRecord(value.Value)
		if err != nil {
			return nil, err
		}
		source := targets[value.Key]
		if record.TaskID != run.TaskID || record.OperationID != run.OperationID || record.EnvironmentID != run.EnvironmentID ||
			record.OperationKind != backupruntime.BackupOperationBackup || record.TargetID != source.TargetID ||
			string(record.TargetKind) != string(source.Kind) {
			return nil, conflict("Backup source exclusion belongs to another operation")
		}
		guards[index] = keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision}
	}
	return guards, nil
}
