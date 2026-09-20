package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func invalidBackupRuntimeRecord(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}

func corruptBackupRuntimeRecord() error {
	return errs.New(errs.KindInternal, "backup runtime durable record is corrupt")
}

func encodeBackupRuntimeRecord[T any](
	kind string,
	record T,
	validate func(T) error,
) ([]byte, error) {
	if err := validate(record); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode(kind, record)
	if err != nil {
		return nil, err
	}
	if len(value) > maximumBackupRuntimeRecordBytes {
		return nil, invalidBackupRuntimeRecord("backup runtime durable record exceeds 256 KiB")
	}
	return value, nil
}

func decodeBackupRuntimeRecord[T any](
	value []byte,
	kind string,
	validate func(T) error,
) (T, error) {
	var zero T
	if len(value) == 0 || len(value) > maximumBackupRuntimeRecordBytes {
		return zero, corruptBackupRuntimeRecord()
	}
	record, err := recordcodec.Decode[T](value, kind)
	if err != nil {
		return zero, err
	}
	if validate(record) != nil {
		return zero, corruptBackupRuntimeRecord()
	}
	return record, nil
}

func encodeBackupScheduleCursorRecord(record BackupScheduleCursorRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-schedule-cursor",
		record,
		validateBackupScheduleCursorRecord,
	)
}

func encodeEnvironmentMutationEpochRecord(record EnvironmentMutationEpochRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"environment-mutation-epoch",
		record,
		validateEnvironmentMutationEpochRecord,
	)
}

func decodeEnvironmentMutationEpochRecord(value []byte) (EnvironmentMutationEpochRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"environment-mutation-epoch",
		validateEnvironmentMutationEpochRecord,
	)
}

func decodeBackupScheduleCursorRecord(value []byte) (BackupScheduleCursorRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-schedule-cursor",
		validateBackupScheduleCursorRecord,
	)
}

func encodeBackupDueOutcomeRecord(record BackupDueOutcomeRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-due-outcome", record, validateBackupDueOutcomeRecord)
}

func decodeBackupDueOutcomeRecord(value []byte) (BackupDueOutcomeRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-due-outcome", validateBackupDueOutcomeRecord)
}

func encodeBackupOperationLockRecord(record BackupOperationLockRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-operation-lock",
		record,
		validateBackupOperationLockRecord,
	)
}

func decodeBackupOperationLockRecord(value []byte) (BackupOperationLockRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-operation-lock",
		validateBackupOperationLockRecord,
	)
}

func encodeBackupSourceTargetExclusionRecord(
	record BackupSourceTargetExclusionRecord,
) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-source-target-exclusion",
		record,
		validateBackupSourceTargetExclusionRecord,
	)
}

func decodeBackupSourceTargetExclusionRecord(
	value []byte,
) (BackupSourceTargetExclusionRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-source-target-exclusion",
		validateBackupSourceTargetExclusionRecord,
	)
}

func encodeBackupRunRecord(record BackupRunRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-run", record, validateBackupRunRecord)
}

func decodeBackupRunRecord(value []byte) (BackupRunRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-run", validateBackupRunRecord)
}

func encodeBackupRecoveryPointRecord(record BackupRecoveryPointRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("recovery-point", record, validateBackupRecoveryPointRecord)
}

func decodeBackupRecoveryPointRecord(value []byte) (BackupRecoveryPointRecord, error) {
	return decodeBackupRuntimeRecord(value, "recovery-point", validateBackupRecoveryPointRecord)
}

func encodeBackupOrphanRecord(record BackupOrphanRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-orphan", record, validateBackupOrphanRecord)
}

func decodeBackupOrphanRecord(value []byte) (BackupOrphanRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-orphan", validateBackupOrphanRecord)
}

func encodeBackupRetentionSweepRecord(record BackupRetentionSweepRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-retention-sweep",
		record,
		validateBackupRetentionSweepRecord,
	)
}

func decodeBackupRetentionSweepRecord(value []byte) (BackupRetentionSweepRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-retention-sweep",
		validateBackupRetentionSweepRecord,
	)
}

func encodeBackupRecoveryPointPruneRecord(record BackupRecoveryPointPruneRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"recovery-point-prune",
		record,
		validateBackupRecoveryPointPruneRecord,
	)
}

func decodeBackupRecoveryPointPruneRecord(value []byte) (BackupRecoveryPointPruneRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"recovery-point-prune",
		validateBackupRecoveryPointPruneRecord,
	)
}

func encodeBackupRecoveryPointPruneDispatchRecord(
	record BackupRecoveryPointPruneDispatchRecord,
) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"recovery-point-prune-dispatch",
		record,
		validateBackupRecoveryPointPruneDispatchRecord,
	)
}

func decodeBackupRecoveryPointPruneDispatchRecord(
	value []byte,
) (BackupRecoveryPointPruneDispatchRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"recovery-point-prune-dispatch",
		validateBackupRecoveryPointPruneDispatchRecord,
	)
}

func encodeBackupRestoreRecord(record BackupRestoreRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-restore", record, validateBackupRestoreRecord)
}

func decodeBackupRestoreRecord(value []byte) (BackupRestoreRecord, error) {
	return decodeBackupRuntimeRecord(value, "backup-restore", validateBackupRestoreRecord)
}

func encodeBackupRestoreServiceRecord(record BackupRestoreServiceRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord(
		"backup-restore-service",
		record,
		validateBackupRestoreServiceRecord,
	)
}

func decodeBackupRestoreServiceRecord(value []byte) (BackupRestoreServiceRecord, error) {
	return decodeBackupRuntimeRecord(
		value,
		"backup-restore-service",
		validateBackupRestoreServiceRecord,
	)
}

func encodeBackupKeyRotationRecord(record BackupKeyRotationRecord) ([]byte, error) {
	return encodeBackupRuntimeRecord("backup-key-rotation", record, validateBackupKeyRotationRecord)
}

func decodeBackupKeyRotationRecord(value []byte) (BackupKeyRotationRecord, error) {
	if len(value) == 0 || len(value) > maximumBackupRuntimeRecordBytes {
		return BackupKeyRotationRecord{}, corruptBackupRuntimeRecord()
	}
	record, err := recordcodec.Decode[BackupKeyRotationRecord](value, "backup-key-rotation")
	if err != nil || validateBackupKeyRotationRecord(record) != nil {
		clear(record.NextEncryptedIdentity)
		return BackupKeyRotationRecord{}, corruptBackupRuntimeRecord()
	}
	return record, nil
}
