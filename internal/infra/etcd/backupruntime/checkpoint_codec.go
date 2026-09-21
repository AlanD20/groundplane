package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func EncodeBackupCheckpointCursorRecord(record BackupCheckpointCursorRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-checkpoint-cursor",
		record,
		validateBackupCheckpointCursorRecord,
	)
}

func DecodeBackupCheckpointCursorRecord(value []byte) (BackupCheckpointCursorRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-cursor",
		validateBackupCheckpointCursorRecord,
	)
}

func validateBackupCheckpointCursorRecord(record BackupCheckpointCursorRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, record.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, record.StepID) != nil || record.NextSequence < 2 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint cursor is invalid")
	}
	return nil
}

func EncodeBackupCheckpointDedupRecord(record BackupCheckpointDedupRecord) ([]byte, error) {
	return EncodeBackupRuntimeRecord(
		"backup-checkpoint-dedup",
		record,
		validateBackupCheckpointDedupRecord,
	)
}

func DecodeBackupCheckpointDedupRecord(value []byte) (BackupCheckpointDedupRecord, error) {
	return DecodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-dedup",
		validateBackupCheckpointDedupRecord,
	)
}

func validateBackupCheckpointDedupRecord(record BackupCheckpointDedupRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, record.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, record.StepID) != nil || record.Sequence == 0 ||
		record.Kind < BackupCheckpointArtifactPrepared ||
		record.Kind > BackupCheckpointUploadCompleted || !recordcodec.ValidSHA256(record.PayloadSHA256) {
		return errs.New(
			errs.KindValidationFailed,
			"backup checkpoint deduplication record is invalid",
		)
	}
	return nil
}
