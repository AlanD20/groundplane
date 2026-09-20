package etcd

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
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
	if err := validateBackingHookEncryptedInputs(record); err != nil {
		clear(record.Ciphertext)
		return BackingHookEncryptedInputs{}, err
	}
	return record, nil
}

func BindBackingHookTaskInputs(
	task TaskRecord,
	projectID string,
	input BackingHookEncryptedInputs,
) (TaskRecord, error) {
	if ids.Validate(ids.KindProject, projectID) != nil || input.OperationID != task.OperationID ||
		validateBackingHookEncryptedInputs(input) != nil {
		return TaskRecord{}, errs.New(errs.KindValidationFailed, "Backing hook Task input binding is invalid")
	}
	bound := cloneTaskRecord(task)
	if bound.Configuration == nil {
		bound.Configuration = &TaskConfiguration{}
	}
	if bound.Configuration.BackingHookInputs != nil || bound.Configuration.SecretPins != nil {
		return TaskRecord{}, errs.New(errs.KindStateConflict, "Backing hook Task inputs are already bound")
	}
	bound.Configuration.BackingHookInputs = &TaskBackingHookInputSet{
		ProjectID: projectID, CiphertextSHA256: input.CiphertextSHA256,
		SecretSources: append([]tasksecretpinrecord.Record(nil), input.SecretSources...),
	}
	if err := validateTaskBackingHookInputSet(bound); err != nil {
		return TaskRecord{}, err
	}
	return bound, nil
}

func backingHookTaskInputKey(operationID string) string {
	return backingHookTaskInputPrefix + operationID
}

func validateTaskBackingHookInputSet(task TaskRecord) error {
	if task.Configuration == nil || task.Configuration.BackingHookInputs == nil {
		return nil
	}
	binding := task.Configuration.BackingHookInputs
	if ids.Validate(ids.KindProject, binding.ProjectID) != nil || !validSHA256(binding.CiphertextSHA256) {
		return errs.New(errs.KindValidationFailed, "Task backing hook input authority is invalid")
	}
	previous := ""
	for _, source := range binding.SecretSources {
		if tasksecretpinrecord.Validate(source) != nil || source.OperationID != task.OperationID ||
			source.SecretID <= previous {
			return errs.New(errs.KindValidationFailed, "Task backing hook Secret sources are invalid")
		}
		previous = source.SecretID
	}
	return nil
}

func sameTaskBackingHookInputSet(left, right *TaskBackingHookInputSet) bool {
	return left != nil && right != nil && left.ProjectID == right.ProjectID &&
		left.CiphertextSHA256 == right.CiphertextSHA256 && slices.Equal(left.SecretSources, right.SecretSources)
}

func validateBackingHookEncryptedInputs(record BackingHookEncryptedInputs) error {
	if ids.Validate(ids.KindOperation, record.OperationID) != nil || record.EnvelopeVersion != 1 ||
		record.Cipher != "age-x25519" || record.DigestAlgorithm != "sha256" ||
		!validSHA256(record.CiphertextSHA256) || len(record.Ciphertext) == 0 || len(record.Ciphertext) > 256<<10 {
		return errs.New(errs.KindValidationFailed, "Backing hook encrypted inputs are invalid")
	}
	want, _ := hex.DecodeString(record.CiphertextSHA256)
	actual := sha256.Sum256(record.Ciphertext)
	if subtle.ConstantTimeCompare(want, actual[:]) != 1 {
		return errs.New(errs.KindValidationFailed, "Backing hook encrypted input digest does not match")
	}
	return nil
}

func encodeBackingHookEncryptedInputs(record BackingHookEncryptedInputs) ([]byte, error) {
	if err := validateBackingHookEncryptedInputs(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("backing-hook-task-inputs", record)
}

func decodeBackingHookEncryptedInputs(value []byte) (BackingHookEncryptedInputs, error) {
	record, err := decodeEnvelope[BackingHookEncryptedInputs](value, "backing-hook-task-inputs")
	if err != nil || validateBackingHookEncryptedInputs(record) != nil {
		clear(record.Ciphertext)
		return BackingHookEncryptedInputs{}, corruptRecord()
	}
	return record, nil
}

func (repository *AttachRepository) GetBackingHookTaskInputs(
	ctx context.Context,
	task TaskRecord,
) (BackingHookEncryptedInputs, error) {
	if err := validateTaskRecord(task); err != nil || task.Configuration == nil ||
		task.Configuration.BackingHookInputs == nil {
		return BackingHookEncryptedInputs{}, errs.New(
			errs.KindValidationFailed,
			"Task backing hook input authority is missing",
		)
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{backingHookTaskInputKey(task.OperationID)},
	})
	if err != nil {
		return BackingHookEncryptedInputs{}, err
	}
	if result == nil || len(result.Values) != 1 || result.Values[0] == nil {
		return BackingHookEncryptedInputs{}, errs.New(errs.KindStateConflict, "Backing hook Task inputs are unavailable")
	}
	defer clearKeyValues(result.Values)
	record, err := decodeBackingHookEncryptedInputs(result.Values[0].Value)
	if err != nil {
		return BackingHookEncryptedInputs{}, err
	}
	if record.OperationID != task.OperationID ||
		record.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
		clear(record.Ciphertext)
		return BackingHookEncryptedInputs{}, errs.New(errs.KindStateConflict, "Backing hook Task input authority changed")
	}
	return record, nil
}

func (repository *TaskRepository) backingHookInputClaimConditions(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) ([]Condition, error) {
	if task.Configuration == nil || task.Configuration.BackingHookInputs == nil {
		return nil, nil
	}
	key := backingHookTaskInputKey(task.OperationID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if read != nil {
			clearKeyValues(read.Values)
		}
		return nil, errs.New(errs.KindStateConflict, "Backing hook Task inputs are unavailable at claim")
	}
	defer clearKeyValues(read.Values)
	record, err := decodeBackingHookEncryptedInputs(read.Values[0].Value)
	if err != nil {
		return nil, err
	}
	defer clear(record.Ciphertext)
	if record.OperationID != task.OperationID ||
		record.CiphertextSHA256 != task.Configuration.BackingHookInputs.CiphertextSHA256 {
		return nil, errs.New(errs.KindStateConflict, "Backing hook Task input authority changed before claim")
	}
	conditions := []Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}
	pins, err := repository.recoverySecretPinClaimConditions(ctx, task)
	if err != nil {
		return nil, err
	}
	return append(conditions, pins...), nil
}
