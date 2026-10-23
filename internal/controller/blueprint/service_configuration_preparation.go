package blueprint

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Service Releases share one acknowledged Environment-file baseline. Keep its
// next preparation behind unfinished executable work and choose one eligible
// unprepared Service deterministically. Other resource operations are independent.
// Publication compares the same Task revision, fencing concurrent work changes.
func (owner *ServiceWorkPreparation) waitsForServiceConfigurationTurn(ctx context.Context,
	work workqueue.View,
) (bool, error) {
	items := make(map[string]workexecution.Item)
	after := ""
	for {
		page, err := owner.queue.ListWork(ctx, work.Task.Task.ID, after, 100, work.ReadRevision)
		if err != nil {
			return false, err
		}
		for _, item := range page.Items {
			items[item.Item.ID] = item.Item
		}
		if !page.More {
			break
		}
		after = page.LastID
	}
	return serviceConfigurationTurnWaits(work.Item.Item, items)
}

func serviceConfigurationTurnWaits(current workexecution.Item, items map[string]workexecution.Item) (bool, error) {
	for _, other := range items {
		if other.ID == current.ID || other.Operation != workexecution.Release || other.Phase == workexecution.Settled {
			continue
		}
		if other.TaskID != current.TaskID {
			return false, errs.New(errs.KindStateConflict, "Service configuration ordering contains another Task")
		}
		if other.Input != nil {
			return true, nil
		}
		if other.ID > current.ID {
			continue
		}
		eligible := true
		for _, id := range other.Dependencies {
			dependency, found := items[id]
			if !found || dependency.TaskID != current.TaskID {
				return false, errs.New(
					errs.KindStateConflict,
					"Service configuration ordering is missing a captured dependency",
				)
			}
			if dependency.Phase != workexecution.Settled || dependency.Outcome != workexecution.Succeeded {
				eligible = false
				break
			}
		}
		if eligible {
			return true, nil
		}
	}
	return false, nil
}
