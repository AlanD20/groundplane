package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func invalidBackupRuntimeRecord(message string) error {
	return errs.New(errs.KindValidationFailed, message)
}

func CorruptBackupRuntimeRecord() error {
	return errs.New(errs.KindInternal, "backup runtime durable record is corrupt")
}

func EncodeBackupRuntimeRecord[T any](
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

func DecodeBackupRuntimeRecord[T any](
	value []byte,
	kind string,
	validate func(T) error,
) (T, error) {
	var zero T
	if len(value) == 0 || len(value) > maximumBackupRuntimeRecordBytes {
		return zero, CorruptBackupRuntimeRecord()
	}
	record, err := recordcodec.Decode[T](value, kind)
	if err != nil {
		return zero, err
	}
	if validate(record) != nil {
		return zero, CorruptBackupRuntimeRecord()
	}
	return record, nil
}

func encodeBackupScheduleCursorRecord(record BackupScheduleCursorRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-schedule-cursor",
		record,
		validateBackupScheduleCursorRecord,
	)
}

func EncodeEnvironmentMutationEpochRecord(record EnvironmentMutationEpochRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"environment-mutation-epoch",
		record,
		ValidateEnvironmentMutationEpochRecord,
	)
}

func DecodeEnvironmentMutationEpochRecord(value []byte) (EnvironmentMutationEpochRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"environment-mutation-epoch",
		ValidateEnvironmentMutationEpochRecord,
	)
}

func decodeBackupScheduleCursorRecord(value []byte) (BackupScheduleCursorRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-schedule-cursor",
		validateBackupScheduleCursorRecord,
	)
}

func EncodeBackupDueOutcomeRecord(record BackupDueOutcomeRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord("backup-due-outcome", record, validateBackupDueOutcomeRecord)
}

func DecodeBackupDueOutcomeRecord(value []byte) (BackupDueOutcomeRecord, error) {
	return DecodeBackupRuntimeRecord(value, "backup-due-outcome", validateBackupDueOutcomeRecord)
}

func EncodeBackupOperationLockRecord(record BackupOperationLockRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-operation-lock",
		record,
		validateBackupOperationLockRecord,
	)
}

func DecodeBackupOperationLockRecord(value []byte) (BackupOperationLockRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-operation-lock",
		validateBackupOperationLockRecord,
	)
}

func EncodeBackupSourceTargetExclusionRecord(
	record BackupSourceTargetExclusionRecord,
) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-source-target-exclusion",
		record,
		validateBackupSourceTargetExclusionRecord,
	)
}

func DecodeBackupSourceTargetExclusionRecord(
	value []byte,
) (BackupSourceTargetExclusionRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-source-target-exclusion",
		validateBackupSourceTargetExclusionRecord,
	)
}

func EncodeBackupRunRecord(record BackupRunRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord("backup-run", record, ValidateBackupRunRecord)
}

func DecodeBackupRunRecord(value []byte) (BackupRunRecord, error) {
	return DecodeBackupRuntimeRecord(value, "backup-run", ValidateBackupRunRecord)
}

func EncodeBackupRecoveryPointRecord(record BackupRecoveryPointRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord("recovery-point", record, validateBackupRecoveryPointRecord)
}

func DecodeBackupRecoveryPointRecord(value []byte) (BackupRecoveryPointRecord, error) {
	return DecodeBackupRuntimeRecord(value, "recovery-point", validateBackupRecoveryPointRecord)
}

func EncodeBackupOrphanRecord(record BackupOrphanRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord("backup-orphan", record, validateBackupOrphanRecord)
}

func DecodeBackupOrphanRecord(value []byte) (BackupOrphanRecord, error) {
	return DecodeBackupRuntimeRecord(value, "backup-orphan", validateBackupOrphanRecord)
}

func EncodeBackupRetentionSweepRecord(record BackupRetentionSweepRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-retention-sweep",
		record,
		validateBackupRetentionSweepRecord,
	)
}

func DecodeBackupRetentionSweepRecord(value []byte) (BackupRetentionSweepRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-retention-sweep",
		validateBackupRetentionSweepRecord,
	)
}

func EncodeBackupRecoveryPointPruneRecord(record BackupRecoveryPointPruneRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"recovery-point-prune",
		record,
		validateBackupRecoveryPointPruneRecord,
	)
}

func DecodeBackupRecoveryPointPruneRecord(value []byte) (BackupRecoveryPointPruneRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"recovery-point-prune",
		validateBackupRecoveryPointPruneRecord,
	)
}

func EncodeBackupRecoveryPointPruneDispatchRecord(
	record BackupRecoveryPointPruneDispatchRecord,
) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"recovery-point-prune-dispatch",
		record,
		ValidateBackupRecoveryPointPruneDispatchRecord,
	)
}

func DecodeBackupRecoveryPointPruneDispatchRecord(
	value []byte,
) (BackupRecoveryPointPruneDispatchRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"recovery-point-prune-dispatch",
		ValidateBackupRecoveryPointPruneDispatchRecord,
	)
}

func encodeBackupRestoreRecord(record BackupRestoreRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord("backup-restore", record, validateBackupRestoreRecord)
}

func decodeBackupRestoreRecord(value []byte) (BackupRestoreRecord, error) {
	return DecodeBackupRuntimeRecord(value, "backup-restore", validateBackupRestoreRecord)
}

func encodeBackupRestoreServiceRecord(record BackupRestoreServiceRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-restore-service",
		record,
		validateBackupRestoreServiceRecord,
	)
}

func decodeBackupRestoreServiceRecord(value []byte) (BackupRestoreServiceRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-restore-service",
		validateBackupRestoreServiceRecord,
	)
}

func EncodeBackupKeyRotationRecord(record BackupKeyRotationRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord("backup-key-rotation", record, validateBackupKeyRotationRecord)
}

func DecodeBackupKeyRotationRecord(value []byte) (BackupKeyRotationRecord, error) {
	if len(value) == 0 || len(value) > maximumBackupRuntimeRecordBytes {
		return BackupKeyRotationRecord{}, CorruptBackupRuntimeRecord()
	}
	record, err := recordcodec.Decode[BackupKeyRotationRecord](value, "backup-key-rotation")
	if err != nil || validateBackupKeyRotationRecord(record) != nil {
		clear(record.NextEncryptedIdentity)
		return BackupKeyRotationRecord{}, CorruptBackupRuntimeRecord()
	}
	return record, nil
}
