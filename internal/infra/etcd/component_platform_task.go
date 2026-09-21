package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const TaskPlatformComponentDesiredSHA256Param = "component_desired_sha256"
const TaskAutomaticReconcileParam = "automatic_reconcile"

func IsAutomaticReconcileTask(task TaskRecord) bool {
	return task.Actor == taskjournal.TaskActorSystem && task.Params[TaskAutomaticReconcileParam] == "true"
}

// ReplacePlatformComponentDesiredWithTask commits one typed desired-state
// replacement and its Agent Task in the same protected idempotency transaction.
func (repository *TaskRepository) ReplacePlatformComponentDesiredWithTask(
	ctx context.Context,
	current etcdstore.Versioned[componentrecord.Record],
	desired core.Component,
	task TaskRecord,
	renderInput PlatformComponentTaskRenderInput,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validatePlatformComponentRecord(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateComponentVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement, err := componentrecord.ReplaceDesired(current.Record, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validatePlatformComponentRecord(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if task.Target != replacement.Desired.ID || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskUpdate || task.Status != taskjournal.TaskStatusPending ||
		task.Owner != taskjournal.PlatformTaskOwner() || task.Actor != taskjournal.TaskActorOperator ||
		task.Params[TaskResourceKindParam] != TaskResourceComponent {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"platform Component Task shape is invalid",
		)
	}
	desiredDigest, err := PlatformComponentDesiredDigest(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task, err = bindPlatformComponentTaskMarker(task, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if task.Params == nil {
		task.Params = make(map[string]string, 2)
	}
	task.Params[TaskPlatformComponentDesiredSHA256Param] = desiredDigest
	if renderInput.PlanID != task.PlanID || renderInput.TaskID != task.ID ||
		renderInput.ComponentID != replacement.Desired.ID || renderInput.DesiredSHA256 != desiredDigest ||
		renderInput.ExecutionPlanSHA256 != task.PlanHash {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"platform Component Task render input does not match its Task",
		)
	}
	renderInputValue, err := encodePlatformComponentTaskRenderInput(renderInput)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(renderInputValue)
	initiation, err := newPlatformTaskInitiation(taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskInitiation(task, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			platformComponentOwnerKey(current.Record.Desired.ID),
			platformComponentKindKey(current.Record.Desired.Kind),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindInternal,
			"platform Component indexes are missing or corrupt",
		)
	}

	componentValue, err := componentrecord.EncodeRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(componentValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []etcdstore.Condition{
		{Key: componentrecord.RecordKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: platformComponentTaskRenderInputKey(task.PlanID)},
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		{Key: platformComponentTaskActiveKey(replacement.Desired.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(replacement.Desired.ID), Value: componentValue},
		componentrecord.WriteFenceMutation(replacement.Desired.ID),
		{Type: etcdstore.MutationPut, Key: platformComponentTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: platformComponentTaskActiveKey(replacement.Desired.ID), Value: []byte(task.ID)},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyPlatformComponentTaskConflict(current, indexes, task.OperationID),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func bindPlatformComponentTaskMarker(task TaskRecord, marker idempotencyrecord.IdempotencyMarker) (TaskRecord, error) {
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	if task.IdempotencyKey != marker.Locator.Key || marker.Kind != idempotencyrecord.IdempotencyMarkerTask ||
		marker.State != idempotencyrecord.IdempotencyMarkerPending || marker.TaskID != task.ID ||
		!marker.CreatedAt.Equal(task.CreatedAt) || !marker.UpdatedAt.Equal(marker.CreatedAt) {
		return TaskRecord{}, errs.New(
			errs.KindValidationFailed,
			"platform Component Task marker does not match its Task",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return TaskRecord{}, err
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	return task, nil
}

func requireAppliedPlatformComponentTask(result IdempotencyTransactionResult) error {
	outcome, existing, conflict, err := result.Classify()
	defer clear(existing.Intent.Ciphertext)
	defer clear(existing.Response.Body)
	if err != nil {
		return err
	}
	if conflict != nil {
		return conflict
	}
	if outcome != IdempotencyKnownApplied {
		return errs.New(errs.KindStateConflict, "platform Component Task publication already exists")
	}
	return nil
}

func PlatformComponentDesiredDigest(record componentrecord.Record) (string, error) {
	if err := validatePlatformComponentRecord(record); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(record.Desired)
	if err != nil {
		return "", errs.Wrap(errs.KindInternal, err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func classifyPlatformComponentTaskConflict(
	current etcdstore.Versioned[componentrecord.Record],
	indexes *etcdstore.GetManyResult,
	operationID string,
) idempotencyPlanClassifier {
	taskClassifier := classifyTaskCreateConflict(operationID)
	return func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != 9 {
			return errs.New(errs.KindInternal, "platform Component Task compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.New(errs.KindComponentNotFound, "platform Component was not found")
		}
		if values[0].ModRevision != current.Revision {
			return stateConflict("platform component", current.Record.Desired.ID)
		}
		for index := 1; index < 3; index++ {
			if values[index] == nil || values[index].ModRevision != indexes.Values[index-1].ModRevision ||
				string(values[index].Value) != current.Record.Desired.ID {
				return errs.New(errs.KindInternal, "platform Component indexes changed or are corrupt")
			}
		}
		if values[8] != nil {
			return errs.New(errs.KindStateConflict, "platform Component already has an active Task")
		}
		return taskClassifier(revision, values[4:8])
	}
}
