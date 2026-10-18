package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	preparation "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Accepted input, independent artifact projection, native execution and request
// replay are one transaction. No generic TaskRecord or hidden child is created.
func (repository *TaskRepository) CreateSoftwarePreparationWork(ctx context.Context, capture preparation.Capture,
	record preparation.Record, marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	task, item, err := capture.Work()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := capture.Match(item, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	value, err := preparation.Encode(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	input, err := preparation.EncodeCapture(capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(input)
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	work.Conditions = append(work.Conditions, keyvalue.Condition{Key: preparation.Key(task.ID)}, keyvalue.Condition{Key: preparation.InputKey(item.ID)})
	work.Mutations = append(work.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: preparation.Key(task.ID), Value: value},
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: preparation.InputKey(item.ID), Value: input})
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "software preparation publication changed")
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, work.Conditions, work.Mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}
