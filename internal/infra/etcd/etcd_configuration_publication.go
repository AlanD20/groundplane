package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) CreateEtcdConfigurationWork(ctx context.Context, input etcdconfiguration.Input,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	task, item, conditions, mutations, err := etcdconfiguration.PreparePublication(input)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	work, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(work.Mutations)
	conditions, mutations = append(conditions, work.Conditions...), append(mutations, work.Mutations...)
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "etcd activation publication changed")
	}
	plan, err := newIdempotencyMutationPlanForMarker(idempotency.IdempotencyMarkerTask, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}
