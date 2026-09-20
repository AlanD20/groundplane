package etcd

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"path"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/entrymaterialization"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	secretRecordPrefix = "/v1/records/secrets/"
	secretValuePrefix  = "/v1/secret-values/secrets/"
)

// SecretRecord contains only listable desired metadata. The encrypted value
// is stored under secretValuePrefix and committed with this record.
type SecretRecord struct {
	Secret core.Secret `json:"secret"`
}

// SecretEncryptedValue is the Controller-key envelope stored for one Secret.
// No plaintext length or plaintext digest is persisted.
type SecretEncryptedValue struct {
	SecretID         string `json:"secret_id"`
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	Ciphertext       []byte `json:"ciphertext"`
}

func NewProjectSecretRecord(
	id string,
	projectID string,
	key string,
	kind core.SecretKind,
	filePath string,
	updatedAt time.Time,
) (SecretRecord, error) {
	return newSecretRecord(id, core.SecretScopeProject, projectID, key, kind, filePath, updatedAt)
}

func NewPlatformSecretRecord(
	id string,
	key string,
	kind core.SecretKind,
	filePath string,
	updatedAt time.Time,
) (SecretRecord, error) {
	return newSecretRecord(id, core.SecretScopePlatform, "", key, kind, filePath, updatedAt)
}

func newSecretRecord(
	id string,
	scope core.SecretScope,
	projectID string,
	key string,
	kind core.SecretKind,
	filePath string,
	updatedAt time.Time,
) (SecretRecord, error) {
	reference := filePath
	if kind == core.SecretKindEnvVar {
		if scope == core.SecretScopeProject {
			reference = "secrets/.env." + projectID
		} else {
			reference = "secrets/.env.edge"
		}
	}
	record := SecretRecord{Secret: core.Secret{
		ID: id, Scope: scope, ProjectID: projectID, Key: key, Kind: kind,
		Ref: reference, UpdatedAt: updatedAt,
	}}
	if err := validateSecretRecord(record); err != nil {
		return SecretRecord{}, err
	}
	return record, nil
}

func secretRecordKey(secretID string) string { return secretRecordPrefix + secretID }
func secretValueKey(secretID string) string  { return secretValuePrefix + secretID }

func encodeSecretRecord(record SecretRecord) ([]byte, error) {
	if err := validateSecretRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("secret", record)
}

func decodeSecretRecord(value []byte) (SecretRecord, error) {
	record, err := recordcodec.Decode[SecretRecord](value, "secret")
	if err != nil || validateSecretRecord(record) != nil {
		return SecretRecord{}, corruptSecretRecord()
	}
	return record, nil
}

func encodeSecretEncryptedValue(value SecretEncryptedValue) ([]byte, error) {
	if err := validateSecretEncryptedValue(value); err != nil {
		return nil, err
	}
	copyOfValue := value
	copyOfValue.Ciphertext = append([]byte(nil), value.Ciphertext...)
	return recordcodec.Encode("secret_value", copyOfValue)
}

func decodeSecretEncryptedValue(value []byte) (SecretEncryptedValue, error) {
	record, err := recordcodec.Decode[SecretEncryptedValue](value, "secret_value")
	if err != nil || validateSecretEncryptedValue(record) != nil {
		clear(record.Ciphertext)
		return SecretEncryptedValue{}, corruptSecretRecord()
	}
	return record, nil
}

func validateSecretRecord(record SecretRecord) error {
	secret := record.Secret
	if validateStableID(ids.KindSecret, secret.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Secret stable identity is invalid")
	}
	if err := validateLabel("secret key", secret.Key); err != nil {
		return err
	}
	switch secret.Scope {
	case core.SecretScopeProject:
		if validateStableID(ids.KindProject, secret.ProjectID) != nil {
			return errs.New(errs.KindValidationFailed, "Secret project owner is invalid")
		}
	case core.SecretScopePlatform:
		if secret.ProjectID != "" {
			return errs.New(errs.KindValidationFailed, "Platform Secret must not have a project owner")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Secret scope is invalid")
	}
	switch secret.Kind {
	case core.SecretKindEnvVar:
		if !validSecretEnvironmentKey(secret.Key) {
			return errs.New(errs.KindValidationFailed, "Secret environment key is invalid")
		}
		want := "secrets/.env.edge"
		if secret.Scope == core.SecretScopeProject {
			want = "secrets/.env." + secret.ProjectID
		}
		if secret.Ref != want {
			return errs.New(errs.KindValidationFailed, "Secret environment reference is not canonical")
		}
	case core.SecretKindFile:
		if err := validateSecretFilePath(secret.Ref); err != nil {
			return err
		}
	default:
		return errs.New(errs.KindValidationFailed, "Secret kind is invalid")
	}
	return validateTimestamp("Secret updated_at", secret.UpdatedAt)
}

func validateSecretEncryptedValue(value SecretEncryptedValue) error {
	if validateStableID(ids.KindSecret, value.SecretID) != nil || value.EnvelopeVersion != 1 ||
		value.Cipher != "age-x25519" || value.DigestAlgorithm != "sha256" ||
		len(value.Ciphertext) == 0 || len(value.Ciphertext) > MaximumEntryValueBytes ||
		!validSHA256(value.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "Secret encrypted value envelope is invalid")
	}
	digest := sha256.Sum256(value.Ciphertext)
	want, _ := hex.DecodeString(value.CiphertextSHA256)
	if subtle.ConstantTimeCompare(digest[:], want) != 1 {
		return errs.New(errs.KindValidationFailed, "Secret encrypted value digest does not match")
	}
	return nil
}

func validateSecretFilePath(value string) error {
	if value == "" || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') ||
		strings.Contains(value, `\`) || strings.HasPrefix(value, "/") {
		return errs.New(errs.KindValidationFailed, "Secret file path is invalid")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned != value || strings.HasPrefix(cleaned, "../") {
		return errs.New(errs.KindValidationFailed, "Secret file path is invalid")
	}
	for _, component := range strings.Split(value, "/") {
		if strings.HasPrefix(component, entrymaterialization.TemporaryPrefix) {
			return errs.New(errs.KindValidationFailed, "Secret file path uses a reserved name")
		}
	}
	return nil
}

func validSecretEnvironmentKey(value string) bool {
	if value == "" {
		return false
	}
	for index := range len(value) {
		character := value[index]
		if index == 0 {
			if character != '_' && (character < 'A' || character > 'Z') &&
				(character < 'a' || character > 'z') {
				return false
			}
			continue
		}
		if character != '_' && (character < 'A' || character > 'Z') &&
			(character < 'a' || character > 'z') && (character < '0' || character > '9') {
			return false
		}
	}
	return true
}

func corruptSecretRecord() error {
	return errs.New(errs.KindInternal, "Secret durable record is corrupt")
}
