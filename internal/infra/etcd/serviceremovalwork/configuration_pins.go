package serviceremovalwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

func (repository *Repository) configurationPinGuard(
	ctx context.Context,
	input Input,
	revision int64,
) ([]keyvalue.Condition, error) {
	if input.ConfigurationPins == nil {
		return nil, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
	if err != nil {
		return nil, err
	}
	root, found, err := pins.LoadActive(ctx, input.ConfigurationOperationID(), revision)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, conflict("Service removal no longer retains its captured Secret values")
	}
	guard, err := root.AdmissionCondition(*input.ConfigurationPins)
	if err != nil {
		return nil, err
	}
	return []keyvalue.Condition{{Key: guard.Key, ModRevision: guard.ModRevision}}, nil
}

// Retirement is bounded and starts only under the expired, settled work guards.
// A partial release keeps the input until every reverse membership is removed.
func (repository *Repository) retireConfigurationPins(ctx context.Context, input Input, revision int64,
	guards []keyvalue.Condition,
) (bool, []keyvalue.Condition, error) {
	operationID := input.ConfigurationOperationID()
	absence := []keyvalue.Condition{{Key: tasksecretpins.RootKey(operationID)},
		{
			Key: tasksecretpins.PreparationKey(operationID),
		}, {Key: tasksecretpins.ReversePrefix(operationID), Prefix: true}}
	if input.ConfigurationPins == nil {
		return true, absence, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, input.Scope.ProjectID)
	if err != nil {
		return false, nil, err
	}
	root, found, err := pins.LoadActive(ctx, operationID, revision)
	if err != nil {
		return false, nil, err
	}
	if !found {
		return true, absence, nil
	}
	_, releasing, err := root.RetentionCondition(*input.ConfigurationPins)
	if err != nil {
		return false, nil, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, operationID)
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
