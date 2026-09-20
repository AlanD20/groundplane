package backuppolicy

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"strings"
	"time"
	"unicode/utf8"

	"filippo.io/age"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumBackupPolicyKeep int64 = 9_007_199_254_740_991
	PolicyPrefix                  = "/v1/records/backup-policies/"
	backupSourcePrefix            = "/v1/records/backup-sources/"
	backupKeyPrefix               = "/v1/records/backup-keys/"
	backupKeyValuePrefix          = "/v1/secret-values/backup-keys/"
	MaximumKeyCiphertextLen       = 64 * 1024
)

// BackupPolicyRecord is the Environment singleton. Source metadata remains in
// separate stable records so policy replacement cannot orphan Recovery Points.
type BackupPolicyRecord struct {
	EnvironmentID string    `json:"environment_id"`
	Enabled       bool      `json:"enabled"`
	Frequency     string    `json:"frequency,omitempty"`
	Keep          int64     `json:"keep,omitempty"`
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

func BackupPolicyKey(environmentID string) string {
	return PolicyPrefix + environmentID
}

func BackupSourceKey(sourceID string) string {
	return backupSourcePrefix + sourceID
}

func BackupSourceEnvironmentPrefix(environmentID string) string {
	return "/v1/indexes/backup-sources/by-environment/" + environmentID + "/"
}

func BackupSourceEnvironmentKey(environmentID string, sourceID string) string {
	return BackupSourceEnvironmentPrefix(environmentID) + sourceID
}

func BackupSourceIdentityKey(
	environmentID string,
	kind core.BackupSourceKind,
	targetID string,
) string {
	return "/v1/indexes/backup-sources/by-identity/" + environmentID + "/" +
		string(kind) + "/" + targetID
}

func BackupPolicyConnectorReferencePrefix(connectorID string) string {
	return "/v1/indexes/backup-policies/by-connector/" + connectorID + "/"
}

func BackupPolicyConnectorReferenceKey(connectorID string, environmentID string) string {
	return BackupPolicyConnectorReferencePrefix(connectorID) + environmentID
}

func BackupKeyKey(environmentID string) string {
	return backupKeyPrefix + environmentID
}

func BackupKeyValueKey(environmentID string) string {
	return backupKeyValuePrefix + environmentID
}

func ValidateBackupPolicyRecord(record BackupPolicyRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	if !ValidUTCInstant(record.UpdatedAt) {
		return errs.New(errs.KindValidationFailed, "backup policy update time is invalid")
	}
	if !utf8.ValidString(record.Frequency) || strings.TrimSpace(record.Frequency) != record.Frequency {
		return errs.New(errs.KindValidationFailed, "backup policy frequency is invalid")
	}
	if record.Keep < 0 || record.Keep > MaximumBackupPolicyKeep {
		return errs.New(errs.KindValidationFailed, "backup policy retention is out of range")
	}
	if record.Encryption != "" && record.Encryption != "age" && record.Encryption != "none" {
		return errs.New(errs.KindValidationFailed, "backup policy encryption is invalid")
	}
	if record.ConnectorID != "" {
		if err := recordcodec.ValidateID(ids.KindConnector, record.ConnectorID); err != nil {
			return err
		}
	}
	seen := make(map[string]struct{}, len(record.SourceIDs))
	for _, sourceID := range record.SourceIDs {
		if err := recordcodec.ValidateID(ids.KindBackupSource, sourceID); err != nil {
			return err
		}
		if _, duplicate := seen[sourceID]; duplicate {
			return errs.New(errs.KindValidationFailed, "backup policy source ids must be unique")
		}
		seen[sourceID] = struct{}{}
	}
	configured := record.Frequency != "" || record.Keep != 0 || record.Encryption != "" ||
		record.ConnectorID != "" || len(record.SourceIDs) != 0
	if record.Enabled || configured {
		if record.Frequency == "" || record.Keep <= 0 || record.Encryption == "" ||
			record.ConnectorID == "" || (record.Enabled && len(record.SourceIDs) == 0) {
			return errs.New(errs.KindValidationFailed, "configured Backup Policy is incomplete")
		}
	}
	return nil
}

func ValidateBackupSourceRecord(record BackupSourceRecord) error {
	if err := recordcodec.ValidateID(ids.KindBackupSource, record.ID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	switch record.Kind {
	case core.BackupSourceAttach:
		if err := recordcodec.ValidateID(ids.KindAttach, record.TargetID); err != nil {
			return err
		}
	case core.BackupSourceVolume:
		if err := recordcodec.ValidateID(ids.KindVolume, record.TargetID); err != nil {
			return err
		}
	case core.BackupSourceConfig:
		if record.TargetID != record.EnvironmentID {
			return errs.New(errs.KindValidationFailed, "config Backup source must target its Environment")
		}
	default:
		return errs.New(errs.KindValidationFailed, "backup source kind is invalid")
	}
	if !ValidUTCInstant(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "backup source creation time is invalid")
	}
	return nil
}

func ValidateBackupKeyRecord(record BackupKeyRecord) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID); err != nil {
		return err
	}
	recipient, err := age.ParseX25519Recipient(record.Recipient)
	if err != nil || recipient.String() != record.Recipient {
		return errs.New(errs.KindValidationFailed, "backup key recipient is invalid")
	}
	if record.KeyEra <= 0 || !ValidUTCInstant(record.CreatedAt) || !ValidUTCInstant(record.RotatedAt) ||
		record.RotatedAt.Before(record.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "backup key lifecycle is invalid")
	}
	return nil
}

func ValidateBackupKeyEncryptedValue(value BackupKeyEncryptedValue) error {
	if err := recordcodec.ValidateID(ids.KindEnvironment, value.EnvironmentID); err != nil {
		return err
	}
	if value.KeyEra <= 0 || len(value.Ciphertext) == 0 || len(value.Ciphertext) > MaximumKeyCiphertextLen {
		return errs.New(errs.KindValidationFailed, "encrypted Backup key value is invalid")
	}
	return nil
}

func ValidUTCInstant(value time.Time) bool {
	return !value.IsZero() && value.Equal(value.UTC())
}

func EncodeBackupPolicyRecord(record BackupPolicyRecord) ([]byte, error) {
	if err := ValidateBackupPolicyRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backup-policy", record)
}

func DecodeBackupPolicyRecord(value []byte) (BackupPolicyRecord, error) {
	record, err := recordcodec.Decode[BackupPolicyRecord](value, "backup-policy")
	if err != nil {
		return BackupPolicyRecord{}, err
	}
	if err := ValidateBackupPolicyRecord(record); err != nil {
		return BackupPolicyRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func EncodeBackupSourceRecord(record BackupSourceRecord) ([]byte, error) {
	if err := ValidateBackupSourceRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backup-source", record)
}

func DecodeBackupSourceRecord(value []byte) (BackupSourceRecord, error) {
	record, err := recordcodec.Decode[BackupSourceRecord](value, "backup-source")
	if err != nil {
		return BackupSourceRecord{}, err
	}
	if err := ValidateBackupSourceRecord(record); err != nil {
		return BackupSourceRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func EncodeBackupKeyRecord(record BackupKeyRecord) ([]byte, error) {
	if err := ValidateBackupKeyRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backup-key", record)
}

func DecodeBackupKeyRecord(value []byte) (BackupKeyRecord, error) {
	record, err := recordcodec.Decode[BackupKeyRecord](value, "backup-key")
	if err != nil {
		return BackupKeyRecord{}, err
	}
	if err := ValidateBackupKeyRecord(record); err != nil {
		return BackupKeyRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func EncodeBackupKeyEncryptedValue(value BackupKeyEncryptedValue) ([]byte, error) {
	if err := ValidateBackupKeyEncryptedValue(value); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backup-key-value", value)
}

func DecodeBackupKeyEncryptedValue(value []byte) (BackupKeyEncryptedValue, error) {
	record, err := recordcodec.Decode[BackupKeyEncryptedValue](value, "backup-key-value")
	if err != nil {
		return BackupKeyEncryptedValue{}, err
	}
	if err := ValidateBackupKeyEncryptedValue(record); err != nil {
		clear(record.Ciphertext)
		return BackupKeyEncryptedValue{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
