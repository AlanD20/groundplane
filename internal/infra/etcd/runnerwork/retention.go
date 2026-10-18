package runnerwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareRetention collects execution history, never the Runner's reserved or
// applied runtime plan. A collecting Task cannot accept another Retry or report.
func (repository *Repository) PrepareRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Validate() != nil || item.Validate() != nil || view.Task.Revision <= 0 || view.Item.Revision <= 0 ||
		task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) ||
		item.Operation != workexecution.Runner || item.TaskID != task.ID || item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner history requires expired, fully settled execution")
	}
	input, err := repository.LoadTask(ctx, task.ID, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if err := input.Record.MatchWork(item); err != nil {
		return false, workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: InputKey(item.ID), ModRevision: input.Revision},
	}}
	if final {
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true})
		change.Mutations = []keyvalue.Mutation{
			{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)},
			{Type: keyvalue.MutationDelete, Key: TaskInputKey(task.ID)},
		}
	}
	return true, change, nil
}

func PrepareAttemptRetirement(view workqueue.View, snapshot workqueue.AttemptSnapshot) (workqueue.Change, error) {
	item, attempt := view.Item.Item, snapshot.Attempt
	if item.Operation != workexecution.Runner || item.Input == nil || attempt.Input != *item.Input ||
		attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Runner attempt history differs from its captured input")
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: prepared.Conditions, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationDelete, Key: ResultKey(attempt.ID)},
		{Type: keyvalue.MutationDelete, Key: "/v1/execution/runner/grants/" + attempt.ID + "/", Prefix: true},
	}}, nil
}
