package backupruntime

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/mysql84protocol"
	"github.com/AlanD20/groundplane/internal/common/postgresidentity"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

// BackupRecoveryPointTargetSnapshot is an upload target, not verified remote
// ownership. Prepared and unknown uploads retain this snapshot without
// manufacturing a selected immutable object.
type BackupRecoveryPointTargetSnapshot struct {
	ID                 string                  `json:"id"`
	EnvironmentID      string                  `json:"environment_id"`
	SourceID           string                  `json:"source_id"`
	SourceKind         BackupRuntimeSourceKind `json:"source_kind"`
	TargetID           string                  `json:"target_id"`
	ConnectorID        string                  `json:"connector_id"`
	ConnectorPrefix    string                  `json:"connector_prefix,omitempty"`
	ConnectorEndpoint  string                  `json:"connector_endpoint"`
	ConnectorBucket    string                  `json:"connector_bucket"`
	ConnectorRegion    string                  `json:"connector_region"`
	ConnectorPathStyle bool                    `json:"connector_path_style"`
	ObjectKey          string                  `json:"object_key"`
	SourceFormat       BackupRuntimeFormat     `json:"source_format"`
	Encryption         BackupRuntimeEncryption `json:"encryption"`
	KeyEra             int                     `json:"key_era,omitempty"`
	Recipient          string                  `json:"recipient,omitempty"`
	CreatedAt          time.Time               `json:"created_at"`
}

type BackupRecoveryPointSnapshot struct {
	BackupRecoveryPointTargetSnapshot
	Evidence        BackupArtifactEvidence        `json:"evidence"`
	Object          BackupObjectIdentity          `json:"object"`
	Postgres        BackupPostgresPointIdentity   `json:"postgres"`
	PostgresArchive BackupPostgresArchiveEvidence `json:"postgres_archive"`
	MySQL           BackupMySQLPointIdentity      `json:"mysql"`
	MySQLArchive    BackupMySQLArchiveEvidence    `json:"mysql_archive"`
	ConfigArchive   BackupConfigArchiveEvidence   `json:"config_archive"`
	VolumeArchive   BackupVolumeArchiveEvidence   `json:"volume_archive"`
}

// The Point keeps the captured database identity. A later Attach revision may
// authorize execution only when it still names this same database and role.
type BackupPostgresPointIdentity struct {
	Database             string `json:"database"`
	Role                 string `json:"role"`
	BackingEnvironmentID string `json:"backing_environment_id"`
	BackingServiceID     string `json:"backing_service_id"`
	ConsumerServiceID    string `json:"consumer_service_id"`
}

type BackupMySQLPointIdentity struct {
	Database             string `json:"database"`
	Role                 string `json:"role"`
	BackingEnvironmentID string `json:"backing_environment_id"`
	BackingServiceID     string `json:"backing_service_id"`
	ConsumerServiceID    string `json:"consumer_service_id"`
}

type BackupRecoveryPointRecord struct {
	BackupRecoveryPointSnapshot
	VerifiedAt time.Time                  `json:"verified_at"`
	Capture    BackupRecoveryPointCapture `json:"-"`
}

// Capture identifies the producing attempt independently of Task retention.
// Zero means provenance was not recorded; it must never be inferred from time.
type BackupRecoveryPointCapture struct {
	TaskID      string    `json:"task_id"`
	CreatedAt   time.Time `json:"created_at"`
	SourceCount int       `json:"source_count"`
}

func (record BackupRecoveryPointTargetSnapshot) ObjectTarget() BackupObjectTarget {
	return BackupObjectTarget{
		ConnectorID: record.ConnectorID, ConnectorPrefix: record.ConnectorPrefix,
		ConnectorEndpoint: record.ConnectorEndpoint, ConnectorBucket: record.ConnectorBucket,
		ConnectorRegion: record.ConnectorRegion, ConnectorPathStyle: record.ConnectorPathStyle,
		ObjectKey: record.ObjectKey,
	}
}

func ValidateBackupRecoveryPointTargetSnapshot(record BackupRecoveryPointTargetSnapshot) error {
	if recordcodec.ValidateID(ids.KindRecoveryPoint, record.ID) != nil ||
		recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindBackupSource, record.SourceID) != nil ||
		!ValidBackupRuntimeInstant(record.CreatedAt) ||
		!recoveryPointIDMatchesInstant(record.ID, record.CreatedAt) ||
		!validBackupObjectTarget(record.ObjectTarget()) ||
		!validBackupObjectKey(
			record.ObjectKey,
			record.EnvironmentID,
			record.SourceID,
			record.ConnectorPrefix,
			record.ID,
		) {
		return invalidBackupRuntimeRecord("recovery point target snapshot is invalid")
	}
	if err := validateBackupSourceIdentity(record.SourceKind, record.TargetID, record.SourceFormat); err != nil {
		return err
	}
	if record.SourceKind == BackupRuntimeSourceConfig &&
		(record.TargetID != record.EnvironmentID || record.Encryption != BackupRuntimeEncryptionAge) {
		return invalidBackupRuntimeRecord("config recovery point target or encryption is invalid")
	}
	switch record.Encryption {
	case BackupRuntimeEncryptionAge:
		return validateBackupRuntimeEncryption(record.Encryption, 1, 1, record.KeyEra, record.Recipient)
	case BackupRuntimeEncryptionNone:
		return validateBackupRuntimeEncryption(record.Encryption, 0, 0, record.KeyEra, record.Recipient)
	default:
		return invalidBackupRuntimeRecord("recovery point encryption is invalid")
	}
}

func ValidateBackupRecoveryPointSnapshot(record BackupRecoveryPointSnapshot) error {
	if err := ValidateBackupRecoveryPointTargetSnapshot(record.BackupRecoveryPointTargetSnapshot); err != nil {
		return err
	}
	if !validBackupArtifactForTarget(record.Evidence, record.BackupRecoveryPointTargetSnapshot,
		record.PostgresArchive, record.MySQLArchive) ||
		!validBackupObjectIdentity(record.Object) || record.Object.Target != record.ObjectTarget() {
		return invalidBackupRuntimeRecord("recovery point requires complete evidence and an immutable selected object")
	}
	if err := validateSelectedConfigArchive(record.SourceKind, record.ConfigArchive, record.Evidence); err != nil {
		return err
	}
	if err := validateSelectedVolumeArchive(record.SourceKind, record.VolumeArchive, record.Evidence); err != nil {
		return err
	}
	if err := validateSelectedMySQLArchive(record.SourceKind, record.SourceFormat, record.MySQLArchive); err != nil {
		return err
	}
	if err := validateSelectedPostgresArchive(record.SourceKind, record.SourceFormat, record.PostgresArchive); err != nil {
		return err
	}
	if record.SourceKind == BackupRuntimeSourceAttach {
		switch record.SourceFormat {
		case BackupRuntimeFormatPostgres:
			if !postgresidentity.ValidGenerated(record.Postgres.Database) ||
				!postgresidentity.ValidGenerated(record.Postgres.Role) ||
				recordcodec.ValidateID(ids.KindEnvironment, record.Postgres.BackingEnvironmentID) != nil ||
				recordcodec.ValidateID(ids.KindService, record.Postgres.BackingServiceID) != nil ||
				recordcodec.ValidateID(ids.KindService, record.Postgres.ConsumerServiceID) != nil {
				return invalidBackupRuntimeRecord("postgres recovery point target identity is incomplete")
			}
			if record.MySQL != (BackupMySQLPointIdentity{}) {
				return invalidBackupRuntimeRecord("postgres recovery point carries MySQL identity")
			}
		case BackupRuntimeFormatMySQL:
			if !mysql84protocol.ValidGeneratedIdentity(record.MySQL.Database) ||
				!mysql84protocol.ValidGeneratedIdentity(record.MySQL.Role) ||
				recordcodec.ValidateID(ids.KindEnvironment, record.MySQL.BackingEnvironmentID) != nil ||
				recordcodec.ValidateID(ids.KindService, record.MySQL.BackingServiceID) != nil ||
				recordcodec.ValidateID(ids.KindService, record.MySQL.ConsumerServiceID) != nil ||
				record.MySQLArchive.Validate() != nil {
				return invalidBackupRuntimeRecord("MySQL recovery point target identity is incomplete")
			}
			if record.Postgres != (BackupPostgresPointIdentity{}) {
				return invalidBackupRuntimeRecord("MySQL recovery point carries PostgreSQL identity")
			}
		default:
			return invalidBackupRuntimeRecord("database recovery point format is invalid")
		}
	} else if record.Postgres != (BackupPostgresPointIdentity{}) || record.MySQL != (BackupMySQLPointIdentity{}) {
		return invalidBackupRuntimeRecord("non-database recovery point carries database identity")
	}
	return nil
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

func validateRecoveryPointCapture(capture BackupRecoveryPointCapture) error {
	if recordcodec.ValidateID(ids.KindTask, capture.TaskID) != nil ||
		!ValidBackupRuntimeInstant(capture.CreatedAt) || capture.SourceCount < 1 ||
		capture.SourceCount > backuppolicy.MaximumBackupPolicySources {
		return invalidBackupRuntimeRecord("recovery point capture identity is invalid")
	}
	return nil
}
