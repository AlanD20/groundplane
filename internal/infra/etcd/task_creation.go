package etcd

import (
	"context"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateTask atomically claims idempotency and publishes the Task, immutable
// history index, active-operation index, and FIFO queue membership.
func (repository *TaskRepository) CreateTask(
	ctx context.Context,
	record TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != record.ID || !marker.CreatedAt.Equal(record.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) || record.Status != taskjournal.TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"task creation marker does not match its Task",
		)
	}
	initiation, err := newPlatformTaskInitiation(taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateTaskInitiation(record, initiation, true); err != nil {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"generic task creation accepts only platform operator tasks",
		)
	}
	record = cloneTaskRecord(record)
	if record.IdempotencyKey == "" {
		record.IdempotencyKey = marker.Locator.Key
	}
	record.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	taskValue, err := encodeTaskRecord(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(record.ID)},
		{Key: taskjournal.TaskOperationIndexKey(record.OperationID, record.ID)},
		{Key: taskjournal.TaskActiveOperationKey(record.OperationID)},
		{Key: taskjournal.TaskQueueKey(record.Executor, record.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(record.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskOperationIndexKey(record.OperationID, record.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(record.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(record.Executor, record.ID), Value: reference},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		record,
		initiation,
		conditions,
		mutations,
		classifyTaskCreateConflict(record.OperationID),
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

func classifyTaskCreateConflict(operationID string) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 4 {
			return errs.New(errs.KindInternal, "task creation compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				operationID,
				activeTaskID,
			)
		}
		for index, value := range values {
			if index != 2 && value != nil {
				return errs.New(errs.KindInternal, "task creation collided with durable Task state")
			}
		}
		return errs.New(errs.KindInternal, "task creation compare failure was not classified")
	}
}
