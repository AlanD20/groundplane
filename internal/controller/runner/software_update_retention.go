package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Host receipts are retired before their captured database input, so a failed
// cleanup never loses the only authority identifying its owned directory.
func (executor *SoftwareExecutor) PruneInput(ctx context.Context, taskID string, at time.Time) (bool, error) {
	loaded, err := executor.records.Load(ctx, taskID)
	if err != nil {
		kind, _ := errs.KindOf(err)
		if kind == errs.KindRequestNotFound {
			return true, nil
		}
		return false, err
	}
	task, _, err := executor.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return false, err
	}
	if task.Task.CollectionStartedAt == nil || task.Task.FinishedAt == nil || task.Task.RetainUntil == nil ||
		at.Before(*task.Task.RetainUntil) ||
		loaded.Record.Phase != runnerupdates.Finished {
		return false, nil
	}
	view, err := executor.queue.ReadWork(ctx, taskID, loaded.Record.Owner.WorkItemID, 0)
	if err != nil {
		return false, err
	}
	if view.Item.Item.Phase != workexecution.Settled || view.Item.Item.TaskID != taskID {
		return false, errs.New(errs.KindStateConflict, "Runner update still has unsettled work")
	}
	if err := executor.runtime.Prune(ctx, loaded.Record.Plan, loaded.Record.Request); err != nil {
		return false, err
	}
	return executor.records.PruneInput(ctx, taskID, at)
}
