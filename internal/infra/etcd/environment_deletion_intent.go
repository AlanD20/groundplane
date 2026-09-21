package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
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

func environmentDeletionIntentKey(operationID string) string {
	return environmentDeletionIntentPrefix + operationID
}

func environmentDeletionWorkOperationPrefix(operationID string) string {
	return environmentDeletionWorkPrefix + operationID + "/"
}

func newEnvironmentDeletionIntent(
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

func encodeEnvironmentDeletionIntent(record EnvironmentDeletionIntentRecord) ([]byte, error) {
	if err := validateEnvironmentDeletionIntent(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("environment_deletion_intent", record)
}

func decodeEnvironmentDeletionIntent(value []byte) (EnvironmentDeletionIntentRecord, error) {
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

func environmentDeletionIntentMatches(
	record EnvironmentDeletionIntentRecord,
	task TaskRecord,
	tombstone deletionrecord.DeletionTombstoneRecord,
) bool {
	return record.EnvironmentID == task.Target && record.OperationID == task.OperationID &&
		record.TaskID == task.ID && record.TargetRevision == tombstone.TargetRevision &&
		record.CreatedAt.Equal(tombstone.CreatedAt)
}

func loadEnvironmentDeletionIntent(
	ctx context.Context,
	store hierarchyStore,
	task TaskRecord,
	tombstone deletionrecord.DeletionTombstoneRecord,
	revision int64,
) (etcdstore.Versioned[EnvironmentDeletionIntentRecord], error) {
	result, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentDeletionIntentKey(task.OperationID)}, Revision: revision,
	})
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if result == nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindInternal,
			"environment deletion intent evidence is incomplete",
		)
	}
	if result.ReadRevision != revision || len(result.Values) != 1 {
		etcdstore.ClearValues(result.Values)
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindInternal,
			"environment deletion intent evidence is incomplete",
		)
	}
	defer etcdstore.ClearValues(result.Values)
	if result.Values[0] == nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion intent is missing",
		)
	}
	record, err := decodeEnvironmentDeletionIntent(result.Values[0].Value)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if !environmentDeletionIntentMatches(record, task, tombstone) {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion intent ownership changed",
		)
	}
	return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{
		Record: record, Revision: result.Values[0].ModRevision, ReadRevision: revision,
	}, nil
}

func classifyEnvironmentDeletionIntentStartConflict(
	base idempotencyPlanClassifier,
) idempotencyPlanClassifier {
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) == 0 {
			return errs.New(
				errs.KindInternal,
				"environment deletion compare evidence is incomplete",
			)
		}
		intent := values[len(values)-1]
		if intent != nil {
			return errs.New(errs.KindInternal, "environment deletion intent already exists")
		}
		return base(revision, values[:len(values)-1])
	}
}
