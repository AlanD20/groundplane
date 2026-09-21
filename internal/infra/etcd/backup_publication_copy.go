package etcd

import (
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
)

func cloneBackupRunPublicationRecord(record backupruntime.BackupRunRecord) backupruntime.BackupRunRecord {
	clone := record
	if record.ScheduledAt != nil {
		scheduledAt := *record.ScheduledAt
		clone.ScheduledAt = &scheduledAt
	}
	clone.Sources = make([]backupruntime.BackupRunSourceAttemptRecord, len(record.Sources))
	for index, source := range record.Sources {
		clone.Sources[index] = source
		clone.Sources[index].Snapshot = cloneBackupRunSourceSnapshot(source.Snapshot)
	}
	return clone
}

func cloneBackupRunSourceSnapshot(snapshot backupruntime.BackupRunSourceSnapshot) backupruntime.BackupRunSourceSnapshot {
	clone := snapshot
	if snapshot.Postgres != nil {
		postgres := *snapshot.Postgres
		clone.Postgres = &postgres
	}
	if snapshot.Volume != nil {
		volume := *snapshot.Volume
		volume.Services = append([]backupruntime.BackupVolumeServiceSnapshot(nil), snapshot.Volume.Services...)
		for index := range volume.Services {
			volume.Services[index].MountPaths = append(
				[]string(nil),
				volume.Services[index].MountPaths...)
		}
		clone.Volume = &volume
	}
	if snapshot.Config != nil {
		config := *snapshot.Config
		clone.Config = &config
	}
	return clone
}
