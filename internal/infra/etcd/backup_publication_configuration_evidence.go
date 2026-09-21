package etcd

import (
	"context"
	backupconfigrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
)

// Rationale: config snapshot progress is assignment-fenced and may advance
// after publication; its immutable bidirectional Task references retain the
// publication revision and bind the current validated snapshot identity.
func (repository *BackupRuntimeRepository) currentBackupRunConfigCompanions(
	ctx context.Context,
	run backupruntime.BackupRunRecord,
	readRevision int64,
	publicationRevision int64,
) bool {
	for _, source := range run.Sources {
		if source.Kind != backupruntime.BackupRuntimeSourceConfig {
			continue
		}
		snapshot := source.Snapshot.Config
		keys := []string{
			backupconfigrecord.BackupConfigSnapshotKey(snapshot.ConfigSnapshotID),
			backupconfigrecord.BackupConfigSnapshotTaskReferenceKey(run.TaskID, snapshot.ConfigSnapshotID),
			backupconfigrecord.BackupConfigSnapshotReferenceTaskKey(snapshot.ConfigSnapshotID, run.TaskID),
		}
		read, err := repository.ReadFixedKeys(ctx, keys, readRevision)
		if err != nil {
			return false
		}
		primaryRevisionValid := read.Values[0] != nil &&
			((run.RetryOfTaskID == "" && read.Values[0].ModRevision >= publicationRevision) ||
				run.RetryOfTaskID != "")
		if len(read.Values) != len(keys) || !primaryRevisionValid ||
			read.Values[1] == nil || read.Values[2] == nil ||
			read.Values[1].ModRevision != publicationRevision ||
			read.Values[2].ModRevision != publicationRevision ||
			string(read.Values[1].Value) != snapshot.ConfigSnapshotID ||
			string(read.Values[2].Value) != run.TaskID {
			etcdstore.ClearValues(read.Values)
			return false
		}
		stored, decodeErr := backupconfigrecord.DecodeBackupConfigSnapshotRecord(read.Values[0].Value)
		etcdstore.ClearValues(read.Values)
		createdAtValid := (run.RetryOfTaskID == "" && stored.CreatedAt.Equal(run.CreatedAt)) ||
			(run.RetryOfTaskID != "" && stored.CreatedAt.Before(run.CreatedAt))
		if decodeErr != nil || stored.SnapshotID != snapshot.ConfigSnapshotID ||
			stored.EnvironmentID != run.EnvironmentID || stored.SourceID != source.SourceID ||
			stored.State == backupconfigrecord.BackupConfigSnapshotUninitialized ||
			stored.ReadRevision != snapshot.ReadRevision || !createdAtValid {
			return false
		}
	}
	return true
}
