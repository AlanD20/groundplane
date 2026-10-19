package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareWorkRetry is invoked only by explicit operator Retry admission. The
// caller joins request replay before commit. It keeps the same public Task,
// work, captured credentials and exact object keys; it cannot rerun the probe.
func (executor *CheckExecutor) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if previous.Attempt.Result == nil || item.Item.Operation != workexecution.ConnectorCheck ||
		item.Item.TaskID != task.Task.ID || item.Item.Input == nil {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"connector cleanup has no recorded execution to retry",
		)
	}
	loaded, err := executor.repository.LoadAt(ctx, task.Task.ID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	record := loaded.Record
	defer record.Clear()
	digest, err := connectorchecks.PlanHash(record)
	if err != nil || record.Phase != connectorchecks.CleanupRequired || record.Owner.TaskID != task.Task.ID ||
		record.Owner.WorkItemID != item.Item.ID || record.Owner.PlanID != item.Item.Input.ID ||
		digest != item.Item.Input.SHA256 || record.Connector.Connector.ID != item.Item.TargetID {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"only captured pending Connector cleanup can be retried",
		)
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result,
		NotBefore: at, Deadline: at.Add(15 * time.Second),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(
		change.Conditions,
		keyvalue.Condition{Key: connectorchecks.Key(task.Task.ID), ModRevision: loaded.Revision},
	)
	return change, nil
}
