package backupruntime

import "time"

// PrepareBackupPointPublication derives publication records from the verified
// run source. The transactional owner must also require its cleanup checkpoint.
func PrepareBackupPointPublication(
	run BackupRunRecord,
	ordinal uint32,
	at time.Time,
) (BackupRecoveryPointRecord, BackupRetentionSweepRecord, error) {
	if ValidateBackupRunRecord(run) != nil || int(ordinal) >= len(run.Sources) || !ValidBackupRuntimeInstant(at) {
		return BackupRecoveryPointRecord{}, BackupRetentionSweepRecord{}, invalidBackupRuntimeRecord(
			"backup point publication authority is invalid",
		)
	}
	source := run.Sources[ordinal]
	if source.State != BackupSourceAttemptPointCommitted || source.Phase != BackupSourcePhaseRetention {
		return BackupRecoveryPointRecord{}, BackupRetentionSweepRecord{}, invalidBackupRuntimeRecord(
			"backup point publication requires completed source cleanup",
		)
	}
	point := BackupRecoveryPointRecord{
		BackupRecoveryPointSnapshot: BackupRecoveryPointSnapshot{
			BackupRecoveryPointTargetSnapshot: backupSourceTarget(
				run,
				source,
			), Evidence: source.Evidence, Object: source.Object, ConfigArchive: source.ConfigArchive,
			VolumeArchive: source.VolumeArchive, PostgresArchive: source.PostgresArchive,
			MySQLArchive: source.MySQLArchive,
		}, VerifiedAt: at,
	}
	if source.Kind == BackupRuntimeSourceAttach {
		if source.Snapshot.Postgres == nil && source.Snapshot.MySQL == nil {
			return BackupRecoveryPointRecord{}, BackupRetentionSweepRecord{}, invalidBackupRuntimeRecord(
				"database point publication lacks captured target identity")
		}
		if postgres := source.Snapshot.Postgres; postgres != nil {
			point.Postgres = BackupPostgresPointIdentity{Database: postgres.Database, Role: postgres.Role,
				BackingEnvironmentID: postgres.BackingEnvironmentID,
				BackingServiceID:     postgres.BackingServiceID, ConsumerServiceID: postgres.ConsumerServiceID}
		}
		if mysql := source.Snapshot.MySQL; mysql != nil {
			point.MySQL = BackupMySQLPointIdentity{Database: mysql.Database, Role: mysql.Role,
				BackingEnvironmentID: mysql.BackingEnvironmentID,
				BackingServiceID:     mysql.BackingServiceID, ConsumerServiceID: mysql.ConsumerServiceID}
		}
	}
	sweep := BackupRetentionSweepRecord{SourceID: source.SourceID, TriggerRecoveryPointID: source.RecoveryPointID,
		Keep: run.RetentionKeep, Revision: run.PolicyRevision, PolicySHA256: run.PolicySHA256,
		State: BackupRetentionPending, CreatedAt: at, UpdatedAt: at}
	if err := validateBackupRecoveryPointRecord(point); err != nil {
		return BackupRecoveryPointRecord{}, BackupRetentionSweepRecord{}, err
	}
	if err := validateBackupRetentionSweepRecord(sweep); err != nil {
		return BackupRecoveryPointRecord{}, BackupRetentionSweepRecord{}, err
	}
	return point, sweep, nil
}
