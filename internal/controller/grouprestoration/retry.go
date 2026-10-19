package grouprestoration

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PrepareWorkRetry permits only explicit continuation of the same captured
// restoration. It neither retries the successful source nor selects new intent.
func (service *Service) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.ReleaseRestoration || item.Item.Phase != workexecution.Attention ||
		item.Item.ActiveAttemptID != "" ||
		item.Item.Cleanup != workexecution.CleanupRequired && item.Item.Cleanup != workexecution.CleanupStopped ||
		previous.Attempt.Number != item.Item.AttemptNumber ||
		previous.Attempt.Result == nil ||
		previous.Attempt.FinishedAt == nil {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Group restoration has no stopped captured effects to resume",
		)
	}
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	failed, err := service.groups.LoadRestorationFailure(ctx, view, previous.Attempt.Number)
	if err != nil {
		return workqueue.Change{}, err
	}
	if failed.Attempt.Revision != previous.Revision ||
		!workexecution.SameAssignment(failed.Attempt.Attempt, previous.Attempt) ||
		failed.Attempt.Attempt.Result == nil ||
		*failed.Attempt.Attempt.Result != *previous.Attempt.Result {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group restoration Retry selected a different failure receipt",
		)
	}
	root, err := service.groups.Load(ctx, task, revision)
	if err != nil {
		return workqueue.Change{}, err
	}
	conditions, err := service.ledger.GroupRestorationConditions(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	budget := root.Record.Deadline.Sub(root.Record.CreatedAt)
	if budget <= 0 || budget > 24*time.Hour {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Group restoration has no valid captured Retry budget",
		)
	}
	change, err := workqueue.PrepareRetry(
		task,
		item,
		previous,
		workexecution.RetryDecision{Direction: workexecution.Recovery,
			Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(budget)},
		at,
	)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, failed.Conditions...)
	change.Conditions = append(change.Conditions, conditions...)
	return change, nil
}
