package etcd

import (
	"context"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateSoftwarePreparationTask atomically publishes a native Task, its
// accepted progress projection, queue membership, and idempotency marker.
func (repository *TaskRepository) CreateSoftwarePreparationTask(
	ctx context.Context,
	task TaskRecord,
	preparation preparationrecord.Record,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := preparationrecord.ValidateRecord(preparation); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if preparation.TaskID != task.ID || preparation.OperationID != task.OperationID ||
		preparation.InputSHA256 != task.PlanHash {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "software preparation record does not match its Task",
		)
	}
	value, err := preparationrecord.Encode(preparation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	key := preparationrecord.Key(task.ID)
	classifyTask := classifyTaskCreateConflict(task.OperationID)
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != 5 {
			return errs.New(errs.KindInternal, "software preparation creation evidence is incomplete")
		}
		if values[4] != nil {
			return errs.New(errs.KindInternal, "software preparation creation collided with durable progress")
		}
		return classifyTask(revision, values[:4])
	}
	return repository.createTask(
		ctx, task, marker,
		[]etcdstore.Condition{{Key: key}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: key, Value: value}},
		classify,
	)
}
