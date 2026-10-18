package servicelifecyclework

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *Repository) PrepareUnstartedRetry(ctx context.Context, view workqueue.View, at, deadline time.Time) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Stop != nil || task.CollectionStartedAt != nil || item.ForwardRevoked || item.AttemptNumber != 0 ||
		item.Phase != workexecution.Settled || item.Outcome != workexecution.Failed || item.Cleanup != workexecution.CleanupNone ||
		item.Input == nil || item.Result == nil || *item.Result != *item.Input {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Service lifecycle Retry requires a captured action that failed before execution")
	}
	captured, guard, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions, _, err := services.PrepareLifecycleRuntime(ctx, repository.store, captured.ServiceID,
		captured.Scope.EnvironmentID, captured.TaskID, captured.Action, view.ReadRevision, false)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareUnstartedRetry(view.Task, view.Item, deadline, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, conditions...)
	change.Conditions = append(change.Conditions, guard)
	return change, nil
}
