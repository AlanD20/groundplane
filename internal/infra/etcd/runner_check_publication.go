package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *RunnerRepository) CreateRunnerCheck(
	ctx context.Context,
	source RunnerCheckSource,
	record runnerchecks.Record,
	task TaskRecord,
	marker idempotency.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if source.readRevision <= 0 || record.Validate() != nil || record.Phase != runnerchecks.Ready ||
		!runnerCheckMatchesTask(record, task) ||
		task.Status != taskjournal.TaskStatusPending ||
		!record.CreatedAt.Equal(task.CreatedAt) ||
		record.Plan.Allocation != source.runner.Allocation {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Runner diagnostic publication differs from its captured input",
		)
	}
	rebuilt, err := runnerchecks.NewRecord(
		record.Owner,
		source.Configuration(),
		record.Plan,
		record.Request,
		record.CreatedAt,
	)
	if err != nil || rebuilt.InputSHA256 != record.InputSHA256 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Runner diagnostic source differs from its repository capture",
		)
	}
	target := idempotency.IdempotencyReplayTarget{Kind: idempotency.IdempotencyReplayTargetRunner, ID: task.Target}
	if err := validateRunnerOperationMarker(source.runner.Desired, task, marker, "POST", "/runners/{id}/checks", &target, true); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	task = bindRunnerTaskMarker(task, marker)
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	value, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(value)
	reference, err := idempotency.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []keyvalue.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
	}
	mutations := []keyvalue.Mutation{
		{
			Type:  keyvalue.MutationPut,
			Key:   conditions[0].Key,
			Value: value,
		}, {Type: keyvalue.MutationPut, Key: conditions[1].Key, Value: reference},
		{
			Type:  keyvalue.MutationPut,
			Key:   conditions[2].Key,
			Value: reference,
		}, {Type: keyvalue.MutationPut, Key: conditions[3].Key, Value: reference},
	}
	checkConditions, checkMutations, err := runnerchecks.PreparePublication(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer keyvalue.ClearMutationValues(checkMutations)
	conditions = append(conditions, slices.Clone(source.conditions)...)
	conditions = append(conditions, checkConditions...)
	mutations = append(mutations, checkMutations...)
	classify := func(_ int64, values []*keyvalue.KeyValue) error {
		if len(values) != len(conditions) {
			return errs.New(errs.KindInternal, "Runner diagnostic publication evidence is incomplete")
		}
		for index, condition := range conditions {
			if keyvalue.RevisionOf(values[index]) != condition.ModRevision {
				return errs.New(
					errs.KindStateConflict,
					"Runner runtime, ownership or another check changed before acceptance; refresh and submit a new check",
				)
			}
		}
		return errs.New(errs.KindStateConflict, "Runner diagnostic publication changed before acceptance")
	}
	initiation, err := newRunnerTaskInitiation(source.runner.Desired, source.parents, taskjournal.TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	publication, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	evidence, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return evidence.Apply(ctx, marker, publication)
}
