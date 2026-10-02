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
			VolumeArchive: source.VolumeArchive,
		}, VerifiedAt: at,
	}
	if source.Kind == BackupRuntimeSourceAttach {
		if source.Snapshot.Postgres == nil {
			return BackupRecoveryPointRecord{}, BackupRetentionSweepRecord{}, invalidBackupRuntimeRecord(
				"postgres point publication lacks captured target identity")
		}
		postgres := source.Snapshot.Postgres
		point.Postgres = BackupPostgresPointIdentity{Database: postgres.Database, Role: postgres.Role,
			BackingEnvironmentID: postgres.BackingEnvironmentID,
			BackingServiceID:     postgres.BackingServiceID, ConsumerServiceID: postgres.ConsumerServiceID}
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
