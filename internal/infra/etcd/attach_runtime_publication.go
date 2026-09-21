package etcd

import (
	"context"
	attachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// publishAttachRuntimeTask keeps the acknowledged source fences in the same
// publication as the Task, immutable preparation, Attach and Environment epoch.
func (repository *AttachRepository) publishAttachRuntimeTask(
	ctx context.Context, task TaskRecord, input attachrender.AttachTaskRenderInput, initiation TaskInitiation,
	marker idempotencyrecord.IdempotencyMarker, mutationContext *ordinaryEnvironmentMutationContext,
	conditions []etcdstore.Condition, mutations []etcdstore.Mutation, classifyConflict func(int64, []*etcdstore.KeyValue) error,
) (IdempotencyTransactionResult, error) {
	conditions = append(conditions, attachRuntimeSourceConditions(input)...)
	binding, err := mutationContext.bind(ctx, repository.store, conditions, mutations, true)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.clear()
	defer clearMutationValues(binding.mutations)
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		return binding.classify(revision, values, classifyConflict)
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, binding.conditions, binding.mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(func(conditions []etcdstore.Condition, mutations []etcdstore.Mutation) error {
		budget, err := repository.store.MeasureTransaction(ctx, conditions, mutations)
		if err != nil {
			return err
		}
		if !budget.Fits() {
			return errs.New(errs.KindValidationFailed, "Attach publication exceeds transaction limits")
		}
		return nil
	}); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}
