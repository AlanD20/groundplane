package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Retry publication and expiry compare the same active pin root. Neither may
// accept an attempt after releasing its sources or release a winning attempt.
func (repository *TaskRepository) newRetryTaskIdempotencyMutationPlan(
	ctx context.Context, task TaskRecord, initiation TaskInitiation,
	conditions []etcdstore.Condition, mutations []etcdstore.Mutation, classify idempotencyPlanClassifier,
) (*idempotencyMutationPlan, error) {
	if task.Configuration == nil || task.Configuration.SecretPins == nil {
		return newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classify)
	}
	root, err := repository.loadTaskSecretPinRoot(ctx, task)
	if err != nil {
		return nil, errs.Wrap(errs.KindTaskNotRetryable, err)
	}
	fragment, err := tasksecretpins.RetainForRetry(root, task.ID)
	if err != nil {
		return nil, err
	}
	defer fragment.Clear()
	compares, writes, err := recoverySecretPinFragment(fragment)
	if err != nil {
		return nil, err
	}
	conditions, mutations, classifier := bindRecoverySecretPinPublication(
		taskMaterializationProjectionChange{conditions: compares, mutations: writes}, conditions, mutations, classify,
	)
	return newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classifier)
}
