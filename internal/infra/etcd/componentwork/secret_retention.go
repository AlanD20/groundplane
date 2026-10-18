package componentwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// Called only after expired, settled collection has fenced new execution. Each
// call advances at most one batch and keeps the input until release completes.
func (repository *InputRepository) retireSecretPins(ctx context.Context, view workqueue.View,
	binding tasksecretpins.Binding, guards []keyvalue.Condition,
) (bool, workqueue.Change, error) {
	if binding.Validate() != nil || binding.TaskID != view.Task.Task.ID || binding.WorkItemID != view.Item.Item.ID {
		return false, workqueue.Change{}, conflict("Component Secret retention belongs to different work")
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, view.Task.Task.Scope.ProjectID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	root, found, err := pins.LoadActive(ctx, binding.OperationID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if !found {
		return true, workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: tasksecretpins.RootKey(binding.OperationID)},
			{Key: tasksecretpins.PreparationKey(binding.OperationID)},
			{Key: tasksecretpins.ReversePrefix(binding.OperationID), Prefix: true},
		}}, nil
	}
	_, releasing, err := root.RetentionCondition(binding)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if releasing {
		_, _, err := pins.ReleaseBatch(ctx, binding.OperationID)
		return false, workqueue.Change{}, err
	}
	fragment, err := tasksecretpins.BeginRelease(root)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer fragment.Clear()
	conditions, mutations, err := tasksecretpins.EtcdFragment(fragment)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	defer keyvalue.ClearMutationValues(mutations)
	result, err := repository.store.Transact(ctx, append(append([]keyvalue.Condition(nil), guards...), conditions...), mutations)
	keyvalue.ClearValues(result.FailureReads)
	return false, workqueue.Change{}, err
}
