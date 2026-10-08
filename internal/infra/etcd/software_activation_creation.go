package etcd

import (
	"context"

	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateSoftwareActivationTask atomically publishes the parent Task and its
// immutable activation projection while fencing the exact verified preparation.
func (repository *TaskRepository) CreateSoftwareActivationTask(
	ctx context.Context,
	task TaskRecord,
	activation activationrecord.Record,
	preparation etcdstore.Versioned[preparationrecord.Record],
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := activationrecord.ValidateRecord(activation); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := preparationrecord.ValidateRecord(preparation.Record); err != nil || preparation.Revision <= 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"software preparation authority is invalid",
		)
	}
	if activation.TaskID != task.ID || activation.OperationID != task.OperationID ||
		activation.InputSHA256 != task.PlanHash || !sameSoftwarePreparation(activation.Input.Preparation, preparation.Record) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"software activation record does not match its Task",
		)
	}
	value, err := activationrecord.Encode(activation)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	activationKey := activationrecord.Key(task.ID)
	preparationKey := preparationrecord.Key(preparation.Record.TaskID)
	classifyTask := classifyTaskCreateConflict(task.OperationID)
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != 6 {
			return errs.New(errs.KindInternal, "software activation creation evidence is incomplete")
		}
		if values[4] != nil {
			return errs.New(errs.KindInternal, "software activation creation collided with durable progress")
		}
		if values[5] == nil || values[5].ModRevision != preparation.Revision {
			return errs.New(errs.KindStateConflict, "verified software preparation changed before activation")
		}
		return classifyTask(revision, values[:4])
	}
	return repository.createTask(
		ctx, task, marker,
		[]etcdstore.Condition{{Key: activationKey}, {Key: preparationKey, ModRevision: preparation.Revision}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: activationKey, Value: value}},
		classify,
	)
}

func sameSoftwarePreparation(left, right preparationrecord.Record) bool {
	return left.TaskID == right.TaskID && left.OperationID == right.OperationID &&
		left.InputSHA256 == right.InputSHA256 && left.Source.Equal(right.Source) &&
		left.Progress.Equal(right.Progress) && left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}
