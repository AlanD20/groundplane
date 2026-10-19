package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/imagefence"

	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) CreateAgentWork(
	ctx context.Context,
	capture agentoperations.Capture,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	task, item, err := capture.Work()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	input, err := agentoperations.EncodeCapture(capture)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(input)
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	work.Conditions = append(
		work.Conditions,
		keyvalue.Condition{Key: agentoperations.InputKey(item.ID)},
		keyvalue.Condition{Key: imagefence.Key},
	)
	work.Mutations = append(
		work.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: agentoperations.InputKey(item.ID), Value: input},
	)
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "Agent lifecycle admission changed")
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotency.IdempotencyMarkerTask,
		work.Conditions,
		work.Mutations,
		classify,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}
