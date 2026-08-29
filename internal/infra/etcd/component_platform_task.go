package etcd

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const TaskPlatformComponentDesiredSHA256Param = "component_desired_sha256"

// ReplacePlatformComponentDesiredWithTask commits one typed desired-state
// replacement and its Agent Task in the same protected idempotency transaction.
func (repository *TaskRepository) ReplacePlatformComponentDesiredWithTask(
	ctx context.Context,
	current Versioned[ComponentRecord],
	desired core.Component,
	task TaskRecord,
	renderInput PlatformComponentTaskRenderInput,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validatePlatformComponentRecord(current.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateComponentVersion(current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	replacement, err := ReplaceComponentDesired(current.Record, desired)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validatePlatformComponentRecord(replacement); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if replacement.Desired.Kind != core.ComponentKindCoreDNS ||
		task.Target != replacement.Desired.ID || task.Executor != TaskExecutorAgent ||
		task.Type != TaskUpdate || task.Status != TaskStatusPending ||
		task.Owner != PlatformTaskOwner() || task.Actor != TaskActorOperator ||
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
	task = cloneTaskRecord(task)
	if task.Params == nil {
		task.Params = make(map[string]string, 2)
	}
	task.Params[TaskPlatformComponentDesiredSHA256Param] = desiredDigest
	if renderInput.PlanID != task.PlanID || renderInput.TaskID != task.ID ||
		renderInput.ComponentID != replacement.Desired.ID || renderInput.DesiredSHA256 != desiredDigest {
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
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"platform Component Task marker does not match its Task",
		)
	}
	initiation, err := newPlatformTaskInitiation(TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskInitiation(task, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
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

	componentValue, err := encodeComponentRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(componentValue)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []Condition{
		{Key: componentKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{Key: platformComponentOwnerKey(current.Record.Desired.ID), ModRevision: indexes.Values[0].ModRevision},
		{Key: platformComponentKindKey(current.Record.Desired.Kind), ModRevision: indexes.Values[1].ModRevision},
		{Key: platformComponentTaskRenderInputKey(task.PlanID)},
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: componentKey(replacement.Desired.ID), Value: componentValue},
		{Type: MutationPut, Key: platformComponentTaskRenderInputKey(task.PlanID), Value: renderInputValue},
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
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

func PlatformComponentDesiredDigest(record ComponentRecord) (string, error) {
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
	current Versioned[ComponentRecord],
	indexes *GetManyResult,
	operationID string,
) idempotencyPlanClassifier {
	taskClassifier := classifyTaskCreateConflict(operationID)
	return func(revision int64, values []*KeyValue) error {
		if len(values) != 8 {
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
		return taskClassifier(revision, values[4:])
	}
}
