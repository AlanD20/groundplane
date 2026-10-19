package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/imagefence"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	activation "github.com/AlanD20/groundplane/internal/infra/etcd/softwareactivation"
	preparation "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) CreateSoftwareActivationWork(
	ctx context.Context,
	record activation.Record,
	prepared keyvalue.Versioned[preparation.Record],
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	task, items, err := record.Graph()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if prepared.Revision <= 0 || !sameSoftwarePreparation(record.Input.Preparation, prepared.Record) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict,
			"software activation must retain its exact verified preparation",
		)
	}
	raw, err := activation.Encode(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(raw)
	change, err := workqueue.PrepareSmallGraph(task, items, marker)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: activation.Key(task.ID)},
		keyvalue.Condition{Key: preparation.Key(prepared.Record.TaskID), ModRevision: prepared.Revision},
		keyvalue.Condition{Key: imagefence.Key},
	)
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: activation.Key(task.ID), Value: raw},
	)
	classify := func(_ int64, _ []*keyvalue.KeyValue) error {
		return errs.New(errs.KindStateConflict, "software preparation or activation admission changed")
	}
	plan, err := newIdempotencyMutationPlanForMarker(
		idempotency.IdempotencyMarkerTask,
		change.Conditions,
		change.Mutations,
		classify,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return (&IdempotencyRepository{store: repository.store}).Apply(ctx, marker, plan)
}

func sameSoftwarePreparation(left, right preparation.Record) bool {
	return left.TaskID == right.TaskID && left.OperationID == right.OperationID &&
		left.InputSHA256 == right.InputSHA256 &&
		left.Source.Equal(right.Source) &&
		left.Progress.Equal(right.Progress) &&
		left.CreatedAt.Equal(right.CreatedAt) &&
		left.UpdatedAt.Equal(right.UpdatedAt)
}
