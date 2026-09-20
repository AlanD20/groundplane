package secrets

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

// Record contains only listable desired metadata. The encrypted value
// is stored under secretValuePrefix and committed with this record.
type Record struct {
	Secret core.Secret `json:"secret"`
}

// EncryptedValue is the Controller-key envelope stored for one Secret.
// No plaintext length or plaintext digest is persisted.
type EncryptedValue struct {
	SecretID         string `json:"secret_id"`
	EnvelopeVersion  uint8  `json:"envelope_version"`
	Cipher           string `json:"cipher"`
	DigestAlgorithm  string `json:"digest_algorithm"`
	CiphertextSHA256 string `json:"ciphertext_sha256"`
	Ciphertext       []byte `json:"ciphertext"`
}

func NewProjectRecord(
	id string,
	projectID string,
	key string,
	kind core.SecretKind,
	filePath string,
	updatedAt time.Time,
) (Record, error) {
	return newSecretRecord(id, core.SecretScopeProject, projectID, key, kind, filePath, updatedAt)
}

func NewPlatformRecord(
	id string,
	key string,
	kind core.SecretKind,
	filePath string,
	updatedAt time.Time,
) (Record, error) {
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
) (Record, error) {
	reference := filePath
	if kind == core.SecretKindEnvVar {
		if scope == core.SecretScopeProject {
			reference = "secrets/.env." + projectID
		} else {
			reference = "secrets/.env.edge"
		}
	}
	record := Record{Secret: core.Secret{
		ID: id, Scope: scope, ProjectID: projectID, Key: key, Kind: kind,
		Ref: reference, UpdatedAt: updatedAt,
	}}
	if err := ValidateRecord(record); err != nil {
		return Record{}, err
	}
	return record, nil
}

func RecordKey(secretID string) string { return secretRecordPrefix + secretID }
func ValueKey(secretID string) string  { return secretValuePrefix + secretID }

func EncodeRecord(record Record) ([]byte, error) {
	if err := ValidateRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("secret", record)
}

func DecodeRecord(value []byte) (Record, error) {
	record, err := recordcodec.Decode[Record](value, "secret")
	if err != nil || ValidateRecord(record) != nil {
		return Record{}, CorruptRecord()
	}
	return record, nil
}

func EncodeEncryptedValue(value EncryptedValue) ([]byte, error) {
	if err := ValidateEncryptedValue(value); err != nil {
		return nil, err
	}
	copyOfValue := value
	copyOfValue.Ciphertext = append([]byte(nil), value.Ciphertext...)
	return recordcodec.Encode("secret_value", copyOfValue)
}

func DecodeEncryptedValue(value []byte) (EncryptedValue, error) {
	record, err := recordcodec.Decode[EncryptedValue](value, "secret_value")
	if err != nil || ValidateEncryptedValue(record) != nil {
		clear(record.Ciphertext)
		return EncryptedValue{}, CorruptRecord()
	}
	return record, nil
}

func ValidateRecord(record Record) error {
	secret := record.Secret
	if recordcodec.ValidateID(ids.KindSecret, secret.ID) != nil {
		return errs.New(errs.KindValidationFailed, "Secret stable identity is invalid")
	}
	if err := recordcodec.ValidateLabel("secret key", secret.Key); err != nil {
		return err
	}
	switch secret.Scope {
	case core.SecretScopeProject:
		if recordcodec.ValidateID(ids.KindProject, secret.ProjectID) != nil {
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
		if !ValidEnvironmentKey(secret.Key) {
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
	return recordcodec.ValidateTimestamp("Secret updated_at", secret.UpdatedAt)
}

func ValidateEncryptedValue(value EncryptedValue) error {
	if recordcodec.ValidateID(ids.KindSecret, value.SecretID) != nil || value.EnvelopeVersion != 1 ||
		value.Cipher != "age-x25519" || value.DigestAlgorithm != "sha256" ||
		len(value.Ciphertext) == 0 || len(value.Ciphertext) > recordcodec.MaximumValueBytes ||
		!recordcodec.ValidSHA256(value.CiphertextSHA256) {
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

func ValidEnvironmentKey(value string) bool {
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

func CorruptRecord() error {
	return errs.New(errs.KindInternal, "Secret durable record is corrupt")
}
