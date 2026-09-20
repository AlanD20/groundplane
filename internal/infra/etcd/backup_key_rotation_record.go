package etcd

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type BackupKeyRotationRecord struct {
	TaskID                        string                 `json:"task_id"`
	OperationID                   string                 `json:"operation_id"`
	EnvironmentID                 string                 `json:"environment_id"`
	ExpectedCurrentRecordRevision int64                  `json:"expected_current_record_revision"`
	ExpectedCurrentValueRevision  int64                  `json:"expected_current_value_revision"`
	CurrentKeyEra                 int                    `json:"current_key_era"`
	NextKeyEra                    int                    `json:"next_key_era"`
	NextRecipient                 string                 `json:"next_recipient"`
	NextEncryptedIdentity         []byte                 `json:"next_encrypted_identity"`
	State                         BackupKeyRotationState `json:"state"`
	CreatedAt                     time.Time              `json:"created_at"`
	UpdatedAt                     time.Time              `json:"updated_at"`
}

func validateBackupKeyRotationRecord(record BackupKeyRotationRecord) error {
	if recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		record.ExpectedCurrentRecordRevision <= 0 || record.ExpectedCurrentValueRevision <= 0 ||
		record.CurrentKeyEra <= 0 || record.NextKeyEra != record.CurrentKeyEra+1 ||
		!validBackupRecipient(record.NextRecipient) ||
		(record.State != BackupKeyRotationPrepared && record.State != BackupKeyRotationApplied) ||
		!validBackupRuntimeLifecycle(record.CreatedAt, record.UpdatedAt) {
		return invalidBackupRuntimeRecord("backup key rotation is invalid")
	}
	if record.State == BackupKeyRotationPrepared && (len(record.NextEncryptedIdentity) == 0 ||
		len(record.NextEncryptedIdentity) > backuppolicy.MaximumKeyCiphertextLen) {
		return invalidBackupRuntimeRecord(
			"prepared backup key rotation requires a wrapped identity",
		)
	}
	if record.State == BackupKeyRotationApplied && len(record.NextEncryptedIdentity) != 0 {
		return invalidBackupRuntimeRecord("applied backup key rotation retains a wrapped identity")
	}
	return nil
}
