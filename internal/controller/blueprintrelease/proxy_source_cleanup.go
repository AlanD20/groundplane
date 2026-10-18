package blueprintrelease

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// DiscardProxyPreparation drains one bounded batch after the entire Apply has
// stopped. A single item's deadline does not authorize releasing shared inputs.
// Published membership remains owned by dependent work and normal retention.
func (service *Service) DiscardProxyPreparation(ctx context.Context, work workqueue.View) (bool, error) {
	task := work.Task.Task
	if task.Validate() != nil || task.Action != workexecution.ActionApply ||
		(task.Stop == nil && task.FinishedAt == nil) || work.Task.Revision <= 0 ||
		work.Item.Revision <= 0 || work.Item.Item.TaskID != task.ID {
		return false, errs.New(errs.KindStateConflict, "Proxy preparation cleanup requires a stopped Apply")
	}
	operationID, err := componentplanning.ProxySourceOperationID(task)
	if err != nil {
		return false, err
	}
	return service.sources.DiscardUnpublishedPreparation(ctx, operationID, []keyvalue.Condition{
		{Key: workqueue.TaskKey(task.ID), ModRevision: work.Task.Revision},
		{Key: workqueue.ItemKey(work.Item.Item.ID), ModRevision: work.Item.Revision},
	})
}
