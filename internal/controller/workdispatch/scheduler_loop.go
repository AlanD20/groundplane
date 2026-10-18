package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RunPlacement schedules a host/executor queue independently. A slow or blocked
// host cannot occupy another host's scheduler. The channel owner supplies the
// authenticated identity; this loop does not register or replace executors.
func (scheduler *Scheduler) RunPlacement(ctx context.Context, identity workexecution.Identity,
	interval time.Duration, report func(Sweep),
) error {
	if ctx == nil || identity.Validate() != nil || interval < 100*time.Millisecond || report == nil {
		return errs.New(errs.KindInternal, "scheduler loop configuration is invalid")
	}
	target := workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		registration, err := scheduler.queue.ReadExecutor(ctx, target)
		if err != nil {
			return err
		}
		if registration.Revision == 0 || registration.Record.Identity != identity {
			return errs.New(errs.KindStateConflict, "scheduler executor registration changed")
		}
		cursor, err := scheduler.queue.ReadSchedulerCursor(ctx, target)
		if err != nil {
			return err
		}
		result, err := scheduler.Schedule(ctx, identity, cursor.Record.After, time.Now().UTC())
		if err != nil {
			return err
		}
		report(result)
		// Saving the cursor after the sweep may replay a page after a crash, but
		// cannot skip unexamined work. Duplicate assignment is prevented by CAS.
		if result.Next != cursor.Record.After {
			change, err := workqueue.PrepareSchedulerCursor(cursor, registration, result.Next)
			if err != nil {
				return err
			}
			if _, err := scheduler.queue.Commit(ctx, change); err != nil {
				return err
			}
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// RunStops advances bounded cancellation pages independently of worker slots.
// Item-specific failures are reported while unrelated Tasks continue stopping.
func (scheduler *Scheduler) RunStops(ctx context.Context, interval time.Duration, report func(Issue)) error {
	if ctx == nil || interval < 100*time.Millisecond || report == nil {
		return errs.New(errs.KindInternal, "task stop loop configuration is invalid")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	after := ""
	for {
		page, err := scheduler.queue.Stopping(ctx, after)
		if err != nil {
			return err
		}
		for _, cursor := range page.Items {
			if err := scheduler.ContinueStop(ctx, cursor, time.Now().UTC()); err != nil {
				report(Issue{TaskID: cursor.Record.TaskID, Err: err})
			}
		}
		after = page.Next
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
