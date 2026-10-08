package backupruntime

import "bytes"

func CloneBackupRestoreRecord(record BackupRestoreRecord) BackupRestoreRecord {
	clone := record
	if record.TargetVersions != nil {
		versions := *record.TargetVersions
		clone.TargetVersions = &versions
	}
	target := CloneBackupRunSourceSnapshot(BackupRunSourceSnapshot{
		Volume: record.CurrentTarget.Volume})
	clone.CurrentTarget.Volume = target.Volume
	if record.CurrentTarget.Postgres != nil {
		postgres := *record.CurrentTarget.Postgres
		postgres.Consumers = append([]BackupRestoreDatabaseServiceSnapshot(nil), postgres.Consumers...)
		postgres.DependentIndexes = append([]BackupRestoreDatabaseDependentIndex(nil), postgres.DependentIndexes...)
		clone.CurrentTarget.Postgres = &postgres
	}
	if record.CurrentTarget.MySQL != nil {
		mysql := *record.CurrentTarget.MySQL
		mysql.Consumers = append([]BackupRestoreDatabaseServiceSnapshot(nil), mysql.Consumers...)
		mysql.DependentIndexes = append([]BackupRestoreDatabaseDependentIndex(nil), mysql.DependentIndexes...)
		clone.CurrentTarget.MySQL = &mysql
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
	if record.DatabaseProgress != nil {
		progress := *record.DatabaseProgress
		clone.DatabaseProgress = &progress
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
