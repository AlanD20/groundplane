package scriptremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareRetention(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !final || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) || item.Phase != workexecution.Settled ||
		item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Script removal history is not ready for collection")
	}
	_, capture, err := repository.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	return true, workqueue.Change{Conditions: []keyvalue.Condition{
		capture,
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true},
	}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: InputKey(item.ID)}}}, nil
}
