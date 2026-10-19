package scriptwork

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareFinish closes the visible Task only after its Script owner settled all
// effects. It does not derive success from the process exit or diagnostics again.
func (service *Service) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if err := ctx.Err(); err != nil {
		return workqueue.Change{}, err
	}
	task := finished.Task.Task
	if task.Action != workexecution.ActionScript || task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Script Task requires exactly one settled Script operation",
		)
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.Script || item.Item.TaskID != task.ID ||
		item.Item.TargetID != task.TargetID ||
		item.Item.Phase != workexecution.Settled ||
		item.Item.Result == nil ||
		item.Item.Input == nil ||
		*item.Item.Input != task.Intent {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Script Task has unresolved execution or cleanup")
	}
	return workqueue.Change{
		Conditions: []keyvalue.Condition{{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}},
	}, nil
}
