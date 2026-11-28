package remoteimage

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	plan, err := service.capture(ctx, task.Task, item.Item, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	selected, err := service.placement.ResolveRemote(
		ctx,
		plan.Record.Executor.HostID,
		revision,
		workexecution.RemoteImage,
	)
	if err != nil {
		kind, _ := errs.KindOf(err)
		if kind == errs.KindAgentReadinessChanged || kind == errs.KindStateConflict {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable,
				"Wait for the selected remote Agent, then retry or submit a new Fetch")
		}
		return workqueue.Change{}, err
	}
	if selected.Identity != plan.Record.Executor {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable,
			"The remote Agent was replaced. Submit a new Fetch for the same image digest")
	}
	var change workqueue.Change
	if previous.Revision == 0 {
		change, err = workqueue.PrepareUnstartedRetry(task, item, at.Add(timeout), at)
	} else {
		if previous.Attempt.Outcome != workexecution.Failed || previous.Attempt.Result == nil {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Remote Fetch has no settled failure to retry")
		}
		change, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
			Direction: workexecution.Forward, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(timeout),
		}, at)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, planGuard(plan).Conditions...)
	change.Conditions = append(change.Conditions, selected.Conditions...)
	return change, nil
}
