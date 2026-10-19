package servicelifecyclework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

func (repository *Repository) retireHookPins(
	ctx context.Context,
	revision int64,
	captured Input,
	guards []keyvalue.Condition,
) (bool, []keyvalue.Condition, error) {
	hook := captured.HookInputs
	absence := []keyvalue.Condition{
		{Key: tasksecretpins.RootKey(hook.OperationID)},
		{Key: tasksecretpins.PreparationKey(hook.OperationID)},
		{Key: tasksecretpins.ReversePrefix(hook.OperationID), Prefix: true},
	}
	if hook.SecretPins == nil {
		return true, absence, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, captured.Scope.ProjectID)
	if err != nil {
		return false, nil, err
	}
	root, found, err := pins.LoadActive(ctx, hook.OperationID, revision)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return true, absence, nil
	}
	_, releasing, err := root.RetentionCondition(*hook.SecretPins)
	if err != nil {
		return false, nil, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, hook.OperationID)
		return false, nil, err
	}
	release, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return false, nil, err
	}
	defer release.Clear()
	conditions, mutations, err := tasksecretpins.EtcdFragment(release)
	if err != nil {
		return false, nil, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	queue, err := workqueue.NewRepository(repository.store)
	if err != nil {
		return false, nil, err
	}
	result, err := queue.Commit(ctx, workqueue.Change{Conditions: append(guards, conditions...), Mutations: mutations})
	keyvalue.ClearValues(result.FailureReads)
	return false, nil, err
}
