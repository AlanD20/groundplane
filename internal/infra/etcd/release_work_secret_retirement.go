package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetireReleaseWorkSecretPins releases one bounded batch only after Task
// collection fences Retry. It never removes Secret values or resource files.
func (ledger *ReleaseLedger) RetireReleaseWorkSecretPins(ctx context.Context, view workqueue.View,
	binding tasksecretpins.Binding,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if ctx == nil || ledger == nil || ledger.store == nil || task.Validate() != nil || item.Validate() != nil ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.Progress.Unsettled() != 0 ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Operation != workexecution.Release ||
		item.TaskID != task.ID || binding.Validate() != nil || binding.TaskID != task.ID || binding.WorkItemID != item.ID {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Secret retirement requires settled collecting Release work",
		)
	}
	guards := []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}
	repository, err := tasksecretpins.NewEtcdRepository(ledger.store, task.Scope.ProjectID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	root, found, err := repository.LoadActive(ctx, binding.OperationID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if !found {
		guards = append(guards, keyvalue.Condition{Key: tasksecretpins.RootKey(binding.OperationID)},
			keyvalue.Condition{Key: tasksecretpins.PreparationKey(binding.OperationID)},
			keyvalue.Condition{Key: tasksecretpins.ReversePrefix(binding.OperationID), Prefix: true})
		return true, workqueue.Change{Conditions: guards}, nil
	}
	_, releasing, err := root.RetentionCondition(binding)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if releasing {
		_, _, err := repository.ReleaseBatch(ctx, binding.OperationID)
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
	defer keyvalue.ZeroMutationBytes(mutations)
	result, err := ledger.store.Transact(ctx, append(guards, conditions...), mutations)
	keyvalue.ClearValues(result.FailureReads)
	return false, workqueue.Change{}, err
}
