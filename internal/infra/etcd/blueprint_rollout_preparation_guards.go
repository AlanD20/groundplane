package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Refresh ownership only, never captured desired configuration. Every staging
// write joins these guards so cancelled/collected work cannot recreate captures.
func (ledger *ReleaseLedger) rolloutPreparationGuards(
	ctx context.Context,
	capture releaserender.ServiceRolloutCapture,
) ([]keyvalue.Condition, error) {
	queue, err := workqueue.NewRepository(ledger.store)
	if err != nil {
		return nil, err
	}
	view, err := queue.ReadWork(ctx, capture.TaskID, capture.WorkItemID, 0)
	if err != nil {
		return nil, err
	}
	if err := capture.MatchesWork(view.Task.Task, view.Item.Item); err != nil {
		return nil, err
	}
	if capture.SourceReadRevision > view.ReadRevision {
		return nil, errs.New(errs.KindStateConflict, "Release preparation cannot refer to a future resource snapshot")
	}
	task, item := view.Task.Task, view.Item.Item
	if task.Stop != nil || task.FinishedAt != nil || task.CollectionStartedAt != nil || item.ForwardRevoked ||
		item.Phase == workexecution.Settled || item.Input != nil || item.AttemptNumber != 0 {
		return nil, errs.New(errs.KindStateConflict, "Release work no longer permits runtime preparation")
	}
	return []keyvalue.Condition{{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}}, nil
}
