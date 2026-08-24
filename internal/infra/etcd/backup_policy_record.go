package etcd

import (
	"strings"
	"time"
	"unicode/utf8"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	backupPolicyPrefix            = "/v1/records/backup-policies/"
	backupSourcePrefix            = "/v1/records/backup-sources/"
	backupKeyPrefix               = "/v1/records/backup-keys/"
	backupKeyValuePrefix          = "/v1/secret-values/backup-keys/"
	maximumBackupKeyCiphertextLen = 64 * 1024
)

// BackupPolicyRecord is the Environment singleton. Source metadata remains in
// separate stable records so policy replacement cannot orphan Recovery Points.
type BackupPolicyRecord struct {
	EnvironmentID string    `json:"environment_id"`
	Enabled       bool      `json:"enabled"`
	Frequency     string    `json:"frequency,omitempty"`
	Keep          int       `json:"keep,omitempty"`
	Encryption    string    `json:"encryption,omitempty"`
	ConnectorID   string    `json:"connector_id,omitempty"`
	SourceIDs     []string  `json:"source_ids,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// BackupSourceRecord retains one resolved source identity even while it is not
// selected by the current policy.
type BackupSourceRecord struct {
	ID            string                `json:"id"`
	EnvironmentID string                `json:"environment_id"`
	Kind          core.BackupSourceKind `json:"kind"`
	TargetID      string                `json:"target_id"`
	CreatedAt     time.Time             `json:"created_at"`
}

// BackupKeyRecord is safe public metadata for the current Environment key era.
type BackupKeyRecord struct {
	EnvironmentID string    `json:"environment_id"`
	Recipient     string    `json:"recipient"`
	KeyEra        int       `json:"key_era"`
	CreatedAt     time.Time `json:"created_at"`
	RotatedAt     time.Time `json:"rotated_at"`
}

// BackupKeyEncryptedValue is the Controller-key-wrapped private identity. Its
// plaintext is never part of BackupKeyRecord or BackupPolicyRecord.
type BackupKeyEncryptedValue struct {
	EnvironmentID string `json:"environment_id"`
	KeyEra        int    `json:"key_era"`
	Ciphertext    []byte `json:"ciphertext"`
}

func backupPolicyKey(environmentID string) string {
	return backupPolicyPrefix + environmentID
}

func backupSourceKey(sourceID string) string {
	return backupSourcePrefix + sourceID
}

func backupSourceEnvironmentPrefix(environmentID string) string {
	return "/v1/indexes/backup-sources/by-environment/" + environmentID + "/"
}

func backupSourceEnvironmentKey(environmentID string, sourceID string) string {
	return backupSourceEnvironmentPrefix(environmentID) + sourceID
}

func backupSourceIdentityKey(
	environmentID string,
	kind core.BackupSourceKind,
	targetID string,
) string {
	return "/v1/indexes/backup-sources/by-identity/" + environmentID + "/" +
		string(kind) + "/" + targetID
}

func backupPolicyConnectorReferencePrefix(connectorID string) string {
	return "/v1/indexes/backup-policies/by-connector/" + connectorID + "/"
}

func backupPolicyConnectorReferenceKey(connectorID string, environmentID string) string {
	return backupPolicyConnectorReferencePrefix(connectorID) + environmentID
}

func backupKeyKey(environmentID string) string {
	return backupKeyPrefix + environmentID
}

func backupKeyValueKey(environmentID string) string {
	return backupKeyValuePrefix + environmentID
}

func validateBackupPolicyRecord(record BackupPolicyRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if !validUTCInstant(record.UpdatedAt) {
		return errs.New(errs.KindValidationFailed, "backup policy update time is invalid")
	}
	if !utf8.ValidString(record.Frequency) || strings.TrimSpace(record.Frequency) != record.Frequency {
		return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
	}
	if record.Keep < 0 {
		return errs.New(errs.KindValidationFailed, "backup policy retention cannot be negative")
	}
	if record.Encryption != "" && record.Encryption != "age" && record.Encryption != "none" {
		return errs.New(errs.KindValidationFailed, "backup policy encryption is invalid")
	}
	if record.ConnectorID != "" {
		if err := validateID(ids.KindConnector, record.ConnectorID); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(record.SourceIDs))
	for _, sourceID := range record.SourceIDs {
		if err := validateID(ids.KindBackupSource, sourceID); err != nil {
			return err
		}
		if _, duplicate := seen[sourceID]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source ids must be unique")
		}
		seen[sourceID] = struct{}{}
	}
	if record.Enabled && (record.Frequency == "" || record.Keep <= 0 || record.Encryption == "" ||
		record.ConnectorID == "" || len(record.SourceIDs) == 0) {
		return errs.New(errs.KindValidationFailed, "enabled Backup Policy is incomplete")
	}
	return nil
}

func validateBackupSourceRecord(record BackupSourceRecord) error {
	if err := validateID(ids.KindBackupSource, record.ID); err != nil {
		return err
	}
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	switch record.Kind {
	case core.BackupSourceAttach:
		if err := validateID(ids.KindAttach, record.TargetID); err != nil {
			return err
		}
	case core.BackupSourceVolume:
		if err := validateID(ids.KindVolume, record.TargetID); err != nil {
			return err
		}
	case core.BackupSourceConfig:
		if record.TargetID != record.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "config Backup source must target its Environment")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup source kind is invalid")
	}
	if !validUTCInstant(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "backup source creation time is invalid")
	}
	return nil
}

func validateBackupKeyRecord(record BackupKeyRecord) error {
	if err := validateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	recipient, err := age.ParseX25519Recipient(record.Recipient)
	if err != nil || recipient.String() != record.Recipient {
		return errs.New(errs.KindValidationFailed, "backup key recipient is invalid")
	}
	if record.KeyEra <= 0 || !validUTCInstant(record.CreatedAt) || !validUTCInstant(record.RotatedAt) ||
		record.RotatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "backup key lifecycle is invalid")
	}
	return nil
}

func validateBackupKeyEncryptedValue(value BackupKeyEncryptedValue) error {
	if err := validateID(ids.KindEnvironment, value.EnvironmentID); err != nil {
		return err
	}
	if value.KeyEra <= 0 || len(value.Ciphertext) == 0 || len(value.Ciphertext) > maximumBackupKeyCiphertextLen {
		return errs.New(errs.KindValidationFailed, "encrypted Backup key value is invalid")
	}
	return nil
}

func validUTCInstant(value time.Time) bool {
	return !value.IsZero() && value.Equal(value.UTC())
}

func encodeBackupPolicyRecord(record BackupPolicyRecord) ([]byte, error) {
	if err := validateBackupPolicyRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("backup-policy", record)
}

func decodeBackupPolicyRecord(value []byte) (BackupPolicyRecord, error) {
	record, err := decodeEnvelope[BackupPolicyRecord](value, "backup-policy")
	if err != nil {
		return BackupPolicyRecord{}, err
	}
	if err := validateBackupPolicyRecord(record); err != nil {
		return BackupPolicyRecord{}, corruptRecord()
	}
	return record, nil
}

func encodeBackupSourceRecord(record BackupSourceRecord) ([]byte, error) {
	if err := validateBackupSourceRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("backup-source", record)
}

func decodeBackupSourceRecord(value []byte) (BackupSourceRecord, error) {
	record, err := decodeEnvelope[BackupSourceRecord](value, "backup-source")
	if err != nil {
		return BackupSourceRecord{}, err
	}
	if err := validateBackupSourceRecord(record); err != nil {
		return BackupSourceRecord{}, corruptRecord()
	}
	return record, nil
}

func encodeBackupKeyRecord(record BackupKeyRecord) ([]byte, error) {
	if err := validateBackupKeyRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("backup-key", record)
}

func decodeBackupKeyRecord(value []byte) (BackupKeyRecord, error) {
	record, err := decodeEnvelope[BackupKeyRecord](value, "backup-key")
	if err != nil {
		return BackupKeyRecord{}, err
	}
	if err := validateBackupKeyRecord(record); err != nil {
		return BackupKeyRecord{}, corruptRecord()
	}
	return record, nil
}

func encodeBackupKeyEncryptedValue(value BackupKeyEncryptedValue) ([]byte, error) {
	if err := validateBackupKeyEncryptedValue(value); err != nil {
		return nil, err
	}
	return encodeEnvelope("backup-key-value", value)
}

func decodeBackupKeyEncryptedValue(value []byte) (BackupKeyEncryptedValue, error) {
	record, err := decodeEnvelope[BackupKeyEncryptedValue](value, "backup-key-value")
	if err != nil {
		return BackupKeyEncryptedValue{}, err
	}
	if err := validateBackupKeyEncryptedValue(record); err != nil {
		clear(record.Ciphertext)
		return BackupKeyEncryptedValue{}, corruptRecord()
	}
	return record, nil
}
