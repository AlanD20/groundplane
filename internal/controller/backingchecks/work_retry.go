package backingchecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingaccess"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/backingchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareWorkRetry is explicit operator admission, not an automatic replay.
// The caller commits this with request replay. The same helper, host, Task and
// captured input remain owned; cleanup carries no database credentials.
func (executor *Executor) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if previous.Attempt.Result == nil || item.Item.Operation != workexecution.BackingCheck ||
		item.Item.TaskID != task.Task.ID || item.Item.Input == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Backing cleanup has no recorded attempt to retry")
	}
	loaded, err := executor.repository.LoadAt(ctx, task.Task.ID, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer loaded.Record.Clear()
	record := loaded.Record
	digest, err := checkrecord.PlanHash(record)
	if err != nil || record.Phase != checkrecord.CleanupRequired || record.Owner.WorkItemID != item.Item.ID ||
		record.Owner.PlanID != item.Item.Input.ID || digest != item.Item.Input.SHA256 ||
		record.Source.Parameters.AttachID != item.Item.TargetID {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "Backing cleanup differs from its captured helper")
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at,
		Deadline: at.Add(backingaccess.ExchangeTimeout + 15*time.Second),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: checkrecord.Key(task.Task.ID), ModRevision: loaded.Revision})
	return change, nil
}
