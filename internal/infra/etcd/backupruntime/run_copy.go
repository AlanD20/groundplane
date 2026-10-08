package backupruntime

import "slices"

func CloneBackupRunPublicationRecord(record BackupRunRecord) BackupRunRecord {
	clone := record
	if record.ScheduledAt != nil {
		scheduledAt := *record.ScheduledAt
		clone.ScheduledAt = &scheduledAt
	}
	clone.Sources = make([]BackupRunSourceAttemptRecord, len(record.Sources))
	for index, source := range record.Sources {
		clone.Sources[index] = source
		clone.Sources[index].Snapshot = CloneBackupRunSourceSnapshot(source.Snapshot)
	}
	return clone
}

func CloneBackupRunSourceSnapshot(snapshot BackupRunSourceSnapshot) BackupRunSourceSnapshot {
	clone := snapshot
	if snapshot.Postgres != nil {
		postgres := *snapshot.Postgres
		clone.Postgres = &postgres
	}
	if snapshot.MySQL != nil {
		mysql := *snapshot.MySQL
		clone.MySQL = &mysql
	}
	if snapshot.Volume != nil {
		volume := *snapshot.Volume
		volume.Services = slices.Clone(snapshot.Volume.Services)
		for index := range volume.Services {
			volume.Services[index].MountPaths = slices.Clone(volume.Services[index].MountPaths)
		}
		clone.Volume = &volume
	}
	if snapshot.Config != nil {
		config := *snapshot.Config
		clone.Config = &config
	}
	return clone
}
