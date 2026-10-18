package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ContinueStop processes one bounded membership page. Publication completes before
// walking membership, but the Task-level stop gate already prevents execution.
// Active executors receive revocation through their normal ownership reads; this
// walk does not assume they disconnected or release any active reservation.
func (scheduler *Scheduler) ContinueStop(ctx context.Context, cursor workqueue.StopCursorSnapshot, at time.Time) error {
	task, revision, err := scheduler.queue.ReadTask(ctx, cursor.Record.TaskID, 0)
	if err != nil {
		return err
	}
	if !task.Task.PublicationComplete {
		return nil
	}
	page, err := scheduler.queue.ListWork(ctx, task.Task.ID, cursor.Record.After, 32, revision)
	if err != nil {
		return err
	}
	for _, item := range page.Items {
		current, err := scheduler.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
		if err != nil {
			return err
		}
		if current.Task.Task.Stop == nil {
			return nil
		}
		if current.Item.Item.Phase == workexecution.Settled || current.Item.Item.ForwardRevoked {
			continue
		}
		owner := scheduler.owner(current.Item.Item)
		if owner == nil {
			return errs.New(errs.KindInternal, "stopped work has no operation owner")
		}
		change, err := owner.Stop(ctx, current.Task, current.Item, at)
		if err != nil {
			return err
		}
		committed, err := scheduler.queue.Commit(ctx, change)
		if err != nil {
			return err
		}
		if !committed.Succeeded {
			// Leave the cursor before the contested item. A later bounded sweep
			// observes the concurrent completion rather than spinning here.
			return nil
		}
	}
	task, _, err = scheduler.queue.ReadTask(ctx, task.Task.ID, 0)
	if err != nil {
		return err
	}
	change, err := workqueue.AdvanceStop(cursor, task, page.LastID, !page.More)
	if err != nil {
		return err
	}
	_, err = scheduler.queue.Commit(ctx, change)
	return err
}
