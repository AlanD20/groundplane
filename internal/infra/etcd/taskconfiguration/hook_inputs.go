package taskconfiguration

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpinrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const backingHookTaskInputPrefix = "/v1/private/backing-hook-task-inputs/"

const TaskBackingServiceAfterStartParam = "backing_service_after_start_service_id"

// TaskBackingHookInputSet is durable, non-secret authority for one operation's
// private hook input envelope and exact Secret sources.
type TaskBackingHookInputSet struct {
	ProjectID        string                       `json:"project_id"`
	CiphertextSHA256 string                       `json:"ciphertext_sha256"`
	SecretSources    []tasksecretpinrecord.Record `json:"secret_sources,omitempty"`
}

// BackingHookEncryptedInputs is the Controller-key envelope stored outside the
// public Task record. It is owned by OperationID so an explicit Retry consumes
// the exact same captured values.
type BackingHookEncryptedInputs struct {
	OperationID      string                       `json:"operation_id"`
	EnvelopeVersion  uint8                        `json:"envelope_version"`
	Cipher           string                       `json:"cipher"`
	DigestAlgorithm  string                       `json:"digest_algorithm"`
	CiphertextSHA256 string                       `json:"ciphertext_sha256"`
	Ciphertext       []byte                       `json:"ciphertext"`
	SecretSources    []tasksecretpinrecord.Record `json:"-"`
}

func NewBackingHookEncryptedInputs(
	operationID string,
	envelopeVersion uint8,
	cipher string,
	digestAlgorithm string,
	ciphertext []byte,
	secretSources []tasksecretpinrecord.Record,
) (BackingHookEncryptedInputs, error) {
	digest := sha256.Sum256(ciphertext)
	record := BackingHookEncryptedInputs{
		OperationID: operationID, EnvelopeVersion: envelopeVersion, Cipher: cipher,
		DigestAlgorithm: digestAlgorithm, CiphertextSHA256: hex.EncodeToString(digest[:]),
		Ciphertext:    append([]byte(nil), ciphertext...),
		SecretSources: append([]tasksecretpinrecord.Record(nil), secretSources...),
	}
	if err := ValidateBackingHookEncryptedInputs(record); err != nil {
		clear(record.Ciphertext)
		return BackingHookEncryptedInputs{}, err
	}
	return record, nil
}

func BackingHookTaskInputKey(operationID string) string {
	return backingHookTaskInputPrefix + operationID
}

func SameTaskBackingHookInputSet(left, right *TaskBackingHookInputSet) bool {
	return left != nil && right != nil && left.ProjectID == right.ProjectID &&
		left.CiphertextSHA256 == right.CiphertextSHA256 && slices.Equal(left.SecretSources, right.SecretSources)
}

func ValidateBackingHookEncryptedInputs(record BackingHookEncryptedInputs) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil || record.EnvelopeVersion != 1 ||
		record.Cipher != "age-x25519" || record.DigestAlgorithm != "sha256" ||
		!recordcodec.ValidSHA256(record.CiphertextSHA256) || len(record.Ciphertext) == 0 || len(record.Ciphertext) > 256<<10 {
		return errs.New(errs.KindValidationFailed, "Backing hook encrypted inputs are invalid")
	}
	want, _ := hex.DecodeString(record.CiphertextSHA256)
	actual := sha256.Sum256(record.Ciphertext)
	if subtle.ConstantTimeCompare(want, actual[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "Backing hook encrypted input digest does not match")
	}
	return nil
}

func EncodeBackingHookEncryptedInputs(record BackingHookEncryptedInputs) ([]byte, error) {
	if err := ValidateBackingHookEncryptedInputs(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("backing-hook-task-inputs", record)
}

func DecodeBackingHookEncryptedInputs(value []byte) (BackingHookEncryptedInputs, error) {
	record, err := recordcodec.Decode[BackingHookEncryptedInputs](value, "backing-hook-task-inputs")
	if err != nil || ValidateBackingHookEncryptedInputs(record) != nil {
		clear(record.Ciphertext)
		return BackingHookEncryptedInputs{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
