package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CheckImageCompletion owns actions whose only resource settlement was already
// committed by their work owner. It is not a generic bypass for other actions.
type CheckImageCompletion struct{}

func (CheckImageCompletion) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	task := finished.Task.Task
	if task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"check or image task requires exactly one settled work item",
		)
	}
	item := finished.Items[0]
	switch item.Item.Operation {
	case workexecution.ConnectorCheck,
		workexecution.BackingCheck,
		workexecution.DNSCheck,
		workexecution.EdgeCheck,
		workexecution.RunnerCheck:
		if task.Action != workexecution.ActionCheck {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "check work does not match the task action")
		}
	case workexecution.Image:
		if task.Action != workexecution.ActionFetch && task.Action != workexecution.ActionRemove {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "image work does not match the task action")
		}
	default:
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"task requires its operation's aggregate settlement",
		)
	}
	if item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.TargetID != task.TargetID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "task has no matching settled result evidence")
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}},
	}, nil
}
