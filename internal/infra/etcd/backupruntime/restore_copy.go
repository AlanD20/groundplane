package backupruntime

import "bytes"

func CloneBackupRestoreRecord(record BackupRestoreRecord) BackupRestoreRecord {
	clone := record
	target := CloneBackupRunSourceSnapshot(BackupRunSourceSnapshot{
		Volume: record.CurrentTarget.Volume})
	clone.CurrentTarget.Volume = target.Volume
	if record.CurrentTarget.Postgres != nil {
		postgres := *record.CurrentTarget.Postgres
		postgres.Consumers = append([]BackupRestorePostgresServiceSnapshot(nil), postgres.Consumers...)
		postgres.DependentIndexes = append([]BackupRestorePostgresDependentIndex(nil), postgres.DependentIndexes...)
		clone.CurrentTarget.Postgres = &postgres
	}
	if record.CurrentTarget.Config != nil {
		config := *record.CurrentTarget.Config
		clone.CurrentTarget.Config = &config
	}
	if record.Artifact != nil {
		artifact := *record.Artifact
		clone.Artifact = &artifact
	}
	if record.ConfigProgress != nil {
		progress := *record.ConfigProgress
		clone.ConfigProgress = &progress
	}
	if record.VolumeProgress != nil {
		progress := *record.VolumeProgress
		clone.VolumeProgress = &progress
	}
	if record.PostgresProgress != nil {
		progress := *record.PostgresProgress
		clone.PostgresProgress = &progress
	}
	return clone
}

func BackupRestoreRecordsEqual(left, right BackupRestoreRecord) bool {
	l, leftErr := EncodeBackupRestoreRecord(left)
	r, rightErr := EncodeBackupRestoreRecord(right)
	defer clear(l)
	defer clear(r)
	return leftErr == nil && rightErr == nil && bytes.Equal(l, r)
}
