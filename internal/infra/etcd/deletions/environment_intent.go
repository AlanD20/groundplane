package deletions

import (
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

const (
	environmentDeletionIntentPrefix = "/v1/runtime/environment-deletion-intents/"
	environmentDeletionWorkPrefix   = "/v1/runtime/environment-deletion-work/"
)

type EnvironmentDeletionCleanupPhase string

const (
	EnvironmentDeletionCleanupEnumerating EnvironmentDeletionCleanupPhase = "enumerating"
	EnvironmentDeletionCleanupComplete    EnvironmentDeletionCleanupPhase = "complete"
)

// EnvironmentDeletionIntentRecord pins the immutable Environment snapshot
// owned by one deletion operation. Retry transfers attempt ownership on the
// tombstone and lock; this operation-owned intent and its work remain stable.
type EnvironmentDeletionIntentRecord struct {
	EnvironmentID  string                          `json:"environment_id"`
	OperationID    string                          `json:"operation_id"`
	TaskID         string                          `json:"task_id"`
	TargetRevision int64                           `json:"target_revision"`
	CleanupPhase   EnvironmentDeletionCleanupPhase `json:"cleanup_phase"`
	CreatedAt      time.Time                       `json:"created_at"`
}

func EnvironmentDeletionIntentKey(operationID string) string {
	return environmentDeletionIntentPrefix + operationID
}

func EnvironmentDeletionWorkOperationPrefix(operationID string) string {
	return environmentDeletionWorkPrefix + operationID + "/"
}

func NewEnvironmentDeletionIntent(
	environmentID string,
	operationID string,
	taskID string,
	targetRevision int64,
	cleanupPhase EnvironmentDeletionCleanupPhase,
	createdAt time.Time,
) (EnvironmentDeletionIntentRecord, error) {
	record := EnvironmentDeletionIntentRecord{
		EnvironmentID: environmentID, OperationID: operationID, TaskID: taskID,
		TargetRevision: targetRevision, CleanupPhase: cleanupPhase, CreatedAt: createdAt,
	}
	if err := validateEnvironmentDeletionIntent(record); err != nil {
		return EnvironmentDeletionIntentRecord{}, err
	}
	return record, nil
}

func EncodeEnvironmentDeletionIntent(record EnvironmentDeletionIntentRecord) ([]byte, error) {
	if err := validateEnvironmentDeletionIntent(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("environment_deletion_intent", record)
}

func DecodeEnvironmentDeletionIntent(value []byte) (EnvironmentDeletionIntentRecord, error) {
	record, err := recordcodec.Decode[EnvironmentDeletionIntentRecord](
		value,
		"environment_deletion_intent",
	)
	if err != nil || validateEnvironmentDeletionIntent(record) != nil {
		return EnvironmentDeletionIntentRecord{}, corruptEnvironmentDeletionIntent()
	}
	return record, nil
}

func validateEnvironmentDeletionIntent(record EnvironmentDeletionIntentRecord) error {
	if recordcodec.ValidateID(ids.KindEnvironment, record.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindOperation, record.OperationID) != nil ||
		recordcodec.ValidateID(ids.KindTask, record.TaskID) != nil || record.TargetRevision <= 0 ||
		(record.CleanupPhase != EnvironmentDeletionCleanupEnumerating &&
			record.CleanupPhase != EnvironmentDeletionCleanupComplete) {
		return errs.New(
			errs.KindValidationFailed,
			"environment deletion intent identity is invalid",
		)
	}
	return recordcodec.ValidateTimestamp("environment deletion intent created_at", record.CreatedAt)
}

func corruptEnvironmentDeletionIntent() error {
	return errs.New(errs.KindInternal, "environment deletion intent is corrupt")
}
