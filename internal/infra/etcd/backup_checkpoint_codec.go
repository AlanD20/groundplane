package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func encodeBackupCheckpointCursorRecord(record backupCheckpointCursorRecord) ([]byte, error) {
	return backupruntime.EncodeBackupRuntimeRecord(
		"backup-checkpoint-cursor",
		record,
		validateBackupCheckpointCursorRecord,
	)
}

func decodeBackupCheckpointCursorRecord(value []byte) (backupCheckpointCursorRecord, error) {
	return backupruntime.DecodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-cursor",
		validateBackupCheckpointCursorRecord,
	)
}

func validateBackupCheckpointCursorRecord(record backupCheckpointCursorRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindAssignment, record.AssignmentID) != nil ||
		recordcodec.ValidateID(ids.KindStep, record.StepID) != nil || record.NextSequence < 2 {
		return errs.New(errs.KindValidationFailed, "backup checkpoint cursor is invalid")
	}
	return nil
}

func encodeBackupCheckpointDedupRecord(record backupCheckpointDedupRecord) ([]byte, error) {
	return backupruntime.EncodeBackupRuntimeRecord(
		"backup-checkpoint-dedup",
		record,
		validateBackupCheckpointDedupRecord,
	)
}

func decodeBackupCheckpointDedupRecord(value []byte) (backupCheckpointDedupRecord, error) {
	return backupruntime.DecodeBackupRuntimeRecord(
		value,
		"backup-checkpoint-dedup",
		validateBackupCheckpointDedupRecord,
	)
}

func validateBackupCheckpointDedupRecord(record backupCheckpointDedupRecord) error {
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
