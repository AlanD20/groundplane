package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *RemovalWork) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	if finished.Task.Task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Connector removal requires one settled decision")
	}
	item := finished.Items[0]
	if item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.AttemptNumber != 0 ||
		item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Connector removal decision has not settled")
	}
	_, capture, err := owner.records.Load(ctx, workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	}}, nil
}
