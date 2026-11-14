package entrywork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/tasksecretpins"
)

// A saved preparation settlement is a distinct native state, not a missing
// executable capture. Pending pins are abandoned only under its collecting owner.
func (repository *InputRepository) prepareUnpreparedRetention(ctx context.Context,
	view workqueue.View, final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if item.Input != nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return false, workqueue.Change{}, conflict("Unprepared Entry retention has an execution attempt")
	}
	settlement, err := repository.transactions.ReadInputSettlement(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if settlement == nil {
		return false, workqueue.Change{}, conflict("Unprepared Entry has no saved settlement")
	}
	operationID := ids.DeriveAt(ids.KindOperation, item.CreatedAt, item.ID, "entry-operation")
	guards := []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true},
		{Key: InputKey(item.ID)},
	}
	if !final {
		return true, workqueue.Change{Conditions: guards}, nil
	}
	pins, err := tasksecretpins.NewEtcdRepository(repository.store, task.Scope.ProjectID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	owned := make([]tasksecretpins.Condition, len(guards))
	for index, guard := range guards {
		owned[index] = tasksecretpins.Condition{Key: guard.Key, ModRevision: guard.ModRevision, Prefix: guard.Prefix}
	}
	abandoned, err := pins.AbandonOwned(ctx, operationID, task.ID, item.ID, owned)
	if err != nil || !abandoned {
		return false, workqueue.Change{}, err
	}
	guards = append(guards, keyvalue.Condition{Key: tasksecretpins.RootKey(operationID)},
		keyvalue.Condition{Key: tasksecretpins.PreparationKey(operationID)},
		keyvalue.Condition{Key: tasksecretpins.ReversePrefix(operationID), Prefix: true})
	return true, workqueue.Change{Conditions: guards, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: InputPrefix(item.ID), Prefix: true},
	}}, nil
}
