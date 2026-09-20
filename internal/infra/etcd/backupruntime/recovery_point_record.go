package backupruntime

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"time"
)

type BackupArtifactEvidence struct {
	SizeBytes int64  `json:"size_bytes"`
	SHA256    string `json:"sha256"`
}

type BackupRecoveryPointSnapshot struct {
	ID              string                  `json:"id"`
	EnvironmentID   string                  `json:"environment_id"`
	SourceID        string                  `json:"source_id"`
	SourceKind      BackupRuntimeSourceKind `json:"source_kind"`
	TargetID        string                  `json:"target_id"`
	ConnectorID     string                  `json:"connector_id"`
	ConnectorPrefix string                  `json:"connector_prefix,omitempty"`
	ObjectKey       string                  `json:"object_key"`
	SourceFormat    BackupRuntimeFormat     `json:"source_format"`
	Encryption      BackupRuntimeEncryption `json:"encryption"`
	KeyEra          int                     `json:"key_era,omitempty"`
	Recipient       string                  `json:"recipient,omitempty"`
	SizeBytes       int64                   `json:"size_bytes"`
	SHA256          string                  `json:"sha256"`
	CreatedAt       time.Time               `json:"created_at"`
}

type BackupRecoveryPointRecord struct {
	BackupRecoveryPointSnapshot
	VerifiedAt time.Time `json:"verified_at"`
}

func ValidateBackupRecoveryPointSnapshot(record BackupRecoveryPointSnapshot) error {
	if recordcodec.ValidateID(ids.KindRecoveryPoint, record.ID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindBackupSource, record.SourceID) != nil ||
		recordcodec.ValidateID(ids.KindConnector, record.ConnectorID) != nil || record.SizeBytes <= 0 ||
		!recordcodec.ValidSHA256(record.SHA256) || !ValidBackupRuntimeInstant(record.CreatedAt) ||
		!recoveryPointIDMatchesInstant(record.ID, record.CreatedAt) ||
		!validBackupObjectKey(
			record.ObjectKey,
			record.EnvironmentID,
			record.SourceID,
			record.ConnectorPrefix,
			record.ID,
		) {
		return invalidBackupRuntimeRecord("recovery point snapshot is invalid")
	}
	if err := validateBackupSourceIdentity(
		record.SourceKind,
		record.TargetID,
		record.SourceFormat,
	); err != nil {
		return err
	}
	if record.SourceKind == BackupRuntimeSourceConfig && record.TargetID != record.EnvironmentID {
		return invalidBackupRuntimeRecord(
			"config recovery point target does not match its Environment",
		)
	}
	if record.SourceKind == BackupRuntimeSourceConfig &&
		record.Encryption != BackupRuntimeEncryptionAge {
		return invalidBackupRuntimeRecord("config recovery point requires age encryption")
	}
	switch record.Encryption {
	case BackupRuntimeEncryptionAge:
		return validateBackupRuntimeEncryption(
			record.Encryption,
			1,
			1,
			record.KeyEra,
			record.Recipient,
		)
	case BackupRuntimeEncryptionNone:
		return validateBackupRuntimeEncryption(
			record.Encryption,
			0,
			0,
			record.KeyEra,
			record.Recipient,
		)
	default:
		return invalidBackupRuntimeRecord("recovery point encryption is invalid")
	}
}

func validateBackupRecoveryPointRecord(record BackupRecoveryPointRecord) error {
	if err := ValidateBackupRecoveryPointSnapshot(record.BackupRecoveryPointSnapshot); err != nil {
		return err
	}
	if !ValidBackupRuntimeInstant(record.VerifiedAt) || record.VerifiedAt.Before(record.CreatedAt) {
		return invalidBackupRuntimeRecord("recovery point verification time is invalid")
	}
	return nil
}
