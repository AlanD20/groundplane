package etcd

import (
	"context"
	backuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
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
		clearKeyValues(result.Values)
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindInternal,
			"environment deletion intent evidence is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
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

func (repository *TaskRepository) prepareEnvironmentRemovalTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (environmentTaskChange, error) {
	if source.Executor != taskjournal.TaskExecutorAgent || source.Type != taskjournal.TaskRemove ||
		recordcodec.ValidateID(ids.KindEnvironment, source.Target) != nil {
		return environmentTaskChange{}, nil
	}
	if retry.Executor != source.Executor || retry.Type != source.Type ||
		retry.Target != source.Target ||
		retry.OperationID != source.OperationID ||
		retry.RetryOf != source.ID {
		return environmentTaskChange{}, errs.New(
			errs.KindInternal,
			"environment deletion retry changed its durable target",
		)
	}
	if source.RetainUntil == nil {
		return environmentTaskChange{}, errs.New(
			errs.KindInternal,
			"environment deletion retry source retention is missing",
		)
	}
	retentionKey := taskjournal.TaskRetentionIndexKey(source.ID, *source.RetainUntil)
	owner := environmentMutationFenceOwner{
		Kind: backupruntime.BackupOperationDeletion, OperationID: source.OperationID, TaskID: source.ID,
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		source.Target,
		revision,
		owner,
	)
	if err != nil {
		return environmentTaskChange{}, err
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), source.Target),
			hierarchyrecord.EnvironmentOperationLockKey(source.Target),
			retentionKey,
		},
		Revision: revision,
	})
	if err != nil {
		return environmentTaskChange{}, err
	}
	if state == nil {
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"environment deletion retry ownership is missing",
		)
	}
	if state.ReadRevision != revision || len(state.Values) != 3 || state.Values[0] == nil ||
		state.Values[1] == nil {
		clearKeyValues(state.Values)
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"environment deletion retry ownership is missing",
		)
	}
	defer clearKeyValues(state.Values)
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != source.Target ||
		tombstone.TaskID != source.ID {
		return environmentTaskChange{}, errs.New(
			errs.KindStateConflict,
			"environment deletion retry tombstone ownership changed",
		)
	}
	lock, err := decodeOwnedEnvironmentDeletionLock(state.Values[1], source)
	if err != nil {
		return environmentTaskChange{}, err
	}
	retentionCondition := etcdstore.Condition{Key: retentionKey}
	if state.Values[2] != nil {
		retainedTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
		if decodeErr != nil || retainedTaskID != source.ID {
			return environmentTaskChange{}, errs.New(
				errs.KindInternal,
				"environment deletion retry source retention is corrupt",
			)
		}
		retentionCondition.ModRevision = state.Values[2].ModRevision
	}
	intent, err := loadEnvironmentDeletionIntent(ctx, repository.store, source, tombstone, revision)
	if err != nil {
		return environmentTaskChange{}, err
	}
	tombstone.TaskID = retry.ID
	tombstone.UpdatedAt = retry.CreatedAt
	lock.TaskID = retry.ID
	lock.UpdatedAt = retry.CreatedAt
	intent.Record.TaskID = retry.ID
	tombstoneValue, err := deletionrecord.EncodeDeletionTombstone(tombstone)
	if err != nil {
		return environmentTaskChange{}, err
	}
	lockValue, err := backupruntime.EncodeBackupOperationLockRecord(lock)
	if err != nil {
		clear(tombstoneValue)
		return environmentTaskChange{}, err
	}
	intentValue, err := encodeEnvironmentDeletionIntent(intent.Record)
	if err != nil {
		clear(tombstoneValue)
		clear(lockValue)
		return environmentTaskChange{}, err
	}
	retentionValue, err := idempotencyrecord.EncodeTaskReference(source.ID)
	if err != nil {
		clear(tombstoneValue)
		clear(lockValue)
		clear(intentValue)
		return environmentTaskChange{}, err
	}
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		clear(tombstoneValue)
		clear(lockValue)
		clear(intentValue)
		clear(retentionValue)
		return environmentTaskChange{}, err
	}
	conditions := fence.transactionConditions()
	conditions = append(conditions,
		etcdstore.Condition{
			Key: environmentDeletionIntentKey(source.OperationID), ModRevision: intent.Revision,
		},
		retentionCondition,
	)
	return environmentTaskChange{
		applies:    true,
		conditions: conditions,
		mutations: []etcdstore.Mutation{
			{
				Type:  etcdstore.MutationPut,
				Key:   deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), source.Target),
				Value: tombstoneValue,
			},
			{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentOperationLockKey(source.Target), Value: lockValue},
			{
				Type:  etcdstore.MutationPut,
				Key:   environmentDeletionIntentKey(source.OperationID),
				Value: intentValue,
			},
			epochMutation,
			{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
		},
		values: [][]byte{
			tombstoneValue, lockValue, intentValue, epochMutation.Value, retentionValue,
		},
	}, nil
}

func (repository *TaskRepository) prepareEnvironmentDeletionIntentTerminal(
	ctx context.Context,
	task TaskRecord,
	tombstone deletionrecord.DeletionTombstoneRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) ([]etcdstore.Condition, []etcdstore.Mutation, error) {
	intent, err := loadEnvironmentDeletionIntent(ctx, repository.store, task, tombstone, revision)
	if err != nil {
		return nil, nil, err
	}
	conditions := []etcdstore.Condition{{
		Key: environmentDeletionIntentKey(task.OperationID), ModRevision: intent.Revision,
	}}
	if terminalStatus != taskjournal.TaskStatusCompleted {
		return conditions, nil, nil
	}
	if intent.Record.CleanupPhase != EnvironmentDeletionCleanupComplete {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup enumeration is incomplete",
		)
	}
	if err := requireEnvironmentDeletionBackupStateEmpty(
		ctx, repository.store, task.Target, task.OperationID, revision,
	); err != nil {
		return nil, nil, err
	}
	return conditions, []etcdstore.Mutation{{
		Type: etcdstore.MutationDelete, Key: environmentDeletionIntentKey(task.OperationID),
	}}, nil
}

func requireEnvironmentDeletionBackupStateEmpty(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) error {
	present, err := environmentDeletionBackupAuthorityPresent(
		ctx, store, environmentID, operationID, revision,
	)
	if err != nil {
		return err
	}
	if present {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion retained backup authority",
		)
	}
	return nil
}

func initialEnvironmentDeletionCleanupPhase(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) (EnvironmentDeletionCleanupPhase, error) {
	present, err := environmentDeletionBackupAuthorityPresent(
		ctx, store, environmentID, operationID, revision,
	)
	if err != nil {
		return "", err
	}
	if present {
		return EnvironmentDeletionCleanupEnumerating, nil
	}
	return EnvironmentDeletionCleanupComplete, nil
}

func environmentDeletionBackupAuthorityPresent(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) (bool, error) {
	direct, err := store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			backuppolicy.BackupPolicyKey(environmentID),
			backuppolicy.BackupKeyKey(environmentID),
			backuppolicy.BackupKeyValueKey(environmentID),
		},
		Revision: revision,
	})
	if err != nil {
		return false, err
	}
	if direct == nil {
		return false, errs.New(
			errs.KindInternal,
			"environment deletion backup authority evidence is incomplete",
		)
	}
	if direct.ReadRevision != revision || len(direct.Values) != 3 {
		clearKeyValues(direct.Values)
		return false, errs.New(
			errs.KindInternal,
			"environment deletion backup authority evidence is incomplete",
		)
	}
	directPresent := false
	for _, value := range direct.Values {
		directPresent = directPresent || value != nil
	}
	clearKeyValues(direct.Values)
	if directPresent {
		return true, nil
	}
	prefixes := []string{
		backuppolicy.BackupSourceEnvironmentPrefix(environmentID),
		connectorEnvironmentPrefix(environmentID),
		backupruntime.BackupScheduleCursorPrefix + environmentID + "/",
		backupruntime.BackupDueOutcomePrefix + environmentID + "/",
		backupruntime.BackupRecoveryPointEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRunEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupOrphanEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupRestoreEnvironmentPrefix + environmentID + "/",
		backupruntime.BackupKeyRotationEnvironmentPrefix + environmentID + "/",
		environmentDeletionWorkOperationPrefix(operationID),
	}
	for _, prefix := range prefixes {
		page, err := store.Range(ctx, etcdstore.RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return false, err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 1 {
			if page != nil {
				clearRangeValues(page.Values)
			}
			return false, errs.New(
				errs.KindInternal,
				"environment deletion backup authority evidence is incomplete",
			)
		}
		found := len(page.Values) != 0
		clearRangeValues(page.Values)
		if found {
			return true, nil
		}
	}
	return false, nil
}

// CompleteEnvironmentDeletionCleanupEnumeration records affirmative completion
// only while the exact deletion owner is retained and every Environment-
// addressable Backup authority is absent at one fixed revision.
func (repository *TaskRepository) CompleteEnvironmentDeletionCleanupEnumeration(
	ctx context.Context,
	task TaskRecord,
) (etcdstore.Versioned[EnvironmentDeletionIntentRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		recordcodec.ValidateID(ids.KindEnvironment, task.Target) != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindValidationFailed,
			"environment deletion cleanup task is invalid",
		)
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		deletionTombstoneKey(string(deletionrecord.DeletionTargetEnvironment), task.Target),
		taskjournal.TaskStorageKey(task.ID),
	}})
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if state == nil || state.ReadRevision <= 0 || len(state.Values) != 2 ||
		state.Values[0] == nil ||
		state.Values[1] == nil {
		if state != nil {
			clearKeyValues(state.Values)
		}
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup ownership is missing",
		)
	}
	defer clearKeyValues(state.Values)
	persistedTask, err := decodeTaskRecord(state.Values[1].Value)
	if err != nil || persistedTask.ID != task.ID || persistedTask.OperationID != task.OperationID ||
		persistedTask.Executor != task.Executor || persistedTask.Type != task.Type ||
		persistedTask.Target != task.Target || persistedTask.Status != task.Status {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup task ownership changed",
		)
	}
	if taskjournal.IsTerminalTaskStatus(persistedTask.Status) {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"terminal environment deletion task cannot complete cleanup enumeration",
		)
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(state.Values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != task.Target || tombstone.TaskID != task.ID {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup tombstone ownership changed",
		)
	}
	owner := environmentMutationFenceOwner{
		Kind: backupruntime.BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
	}
	fence, err := loadOwnedEnvironmentMutationFence(
		ctx, repository.store, task.Target, state.ReadRevision, owner,
	)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	intent, err := loadEnvironmentDeletionIntent(
		ctx, repository.store, task, tombstone, state.ReadRevision,
	)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	if intent.Record.CleanupPhase == EnvironmentDeletionCleanupComplete {
		return intent, nil
	}
	if err := requireEnvironmentDeletionBackupStateEmpty(
		ctx, repository.store, task.Target, task.OperationID, state.ReadRevision,
	); err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	next := intent.Record
	next.CleanupPhase = EnvironmentDeletionCleanupComplete
	intentValue, err := encodeEnvironmentDeletionIntent(next)
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	defer clear(intentValue)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	defer clear(epochMutation.Value)
	conditions := fence.transactionConditions()
	conditions = append(conditions,
		etcdstore.Condition{
			Key: environmentDeletionIntentKey(task.OperationID), ModRevision: intent.Revision,
		},
		etcdstore.Condition{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: state.Values[1].ModRevision},
	)
	transaction, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{
			Type:  etcdstore.MutationPut,
			Key:   environmentDeletionIntentKey(task.OperationID),
			Value: intentValue,
		},
		epochMutation,
	})
	if err != nil {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{}, errs.New(
			errs.KindStateConflict,
			"environment deletion cleanup completion changed",
		)
	}
	return etcdstore.Versioned[EnvironmentDeletionIntentRecord]{
		Record: next, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, nil
}

func environmentDeletionTaskPruneFence(
	task TaskRecord,
	values []*etcdstore.KeyValue,
) (bool, string, error) {
	if len(values) != 3 {
		return false, "", corruptTaskPruneIntent()
	}
	absent := 0
	for _, value := range values {
		if value == nil {
			absent++
		}
	}
	if absent == len(values) {
		return false, "", nil
	}
	if absent != 0 {
		return false, "", corruptTaskPruneIntent()
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(values[0].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetEnvironment ||
		tombstone.TargetID != task.Target {
		return false, "", corruptTaskPruneIntent()
	}
	lock, err := decodeEnvironmentOperationLock(values[1], task.Target)
	if err != nil || lock.Kind != backupruntime.BackupOperationDeletion || lock.EnvironmentID != task.Target ||
		lock.OperationID != task.OperationID {
		return false, "", corruptTaskPruneIntent()
	}
	intent, err := decodeEnvironmentDeletionIntent(values[2].Value)
	if err != nil || intent.EnvironmentID != task.Target ||
		intent.OperationID != task.OperationID ||
		intent.TargetRevision != tombstone.TargetRevision ||
		!intent.CreatedAt.Equal(tombstone.CreatedAt) ||
		tombstone.TaskID != lock.TaskID ||
		tombstone.TaskID != intent.TaskID {
		return false, "", corruptTaskPruneIntent()
	}
	return tombstone.TaskID == task.ID, tombstone.TaskID, nil
}

func (repository *TaskRepository) validateEnvironmentDeletionIntentReplay(
	ctx context.Context,
	task TaskRecord,
	tombstone *deletionrecord.DeletionTombstoneRecord,
	terminalStatus taskjournal.TaskStatus,
	revision int64,
) error {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentDeletionIntentKey(task.OperationID)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if result == nil {
		return errs.New(
			errs.KindInternal,
			"environment deletion replay intent evidence is incomplete",
		)
	}
	if result.ReadRevision != revision || len(result.Values) != 1 {
		clearKeyValues(result.Values)
		return errs.New(
			errs.KindInternal,
			"environment deletion replay intent evidence is incomplete",
		)
	}
	defer clearKeyValues(result.Values)
	if terminalStatus == taskjournal.TaskStatusCompleted {
		if result.Values[0] != nil {
			return errs.New(
				errs.KindStateConflict,
				"completed environment deletion retained its intent",
			)
		}
		return nil
	}
	if tombstone == nil || result.Values[0] == nil {
		return errs.New(errs.KindStateConflict, "environment deletion retry state is missing")
	}
	intent, err := decodeEnvironmentDeletionIntent(result.Values[0].Value)
	if err != nil {
		return err
	}
	if !environmentDeletionIntentMatches(intent, task, *tombstone) {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion retry intent ownership changed",
		)
	}
	return nil
}
