package runnermigrationoperator

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerscoped"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (work *Work) AuthorizeStart(ctx context.Context, delivery workqueue.Delivery) (workqueue.Change, error) {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if workexecution.ValidateDelivery(item, attempt) != nil || attempt.StartedAt != nil {
		return workqueue.Change{}, conflict("Runner migration has no fresh assignment")
	}
	state, err := work.load(ctx, item)
	if err != nil {
		return workqueue.Change{}, err
	}
	if attempt.Direction == workexecution.Forward && state.Record.Phase != runnerscoped.Pending ||
		attempt.Direction == workexecution.Recovery && state.Record.Phase == runnerscoped.Pending {
		return workqueue.Change{}, conflict("Runner migration assignment no longer owns this phase")
	}
	return stateGuard(state), nil
}

func (work *Work) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	if workexecution.ValidateDelivery(delivery.Item.Item, delivery.Attempt.Attempt) != nil ||
		delivery.Attempt.Attempt.StartedAt != nil {
		return conflict("Runner migration has already started")
	}
	_, err := work.load(ctx, delivery.Item.Item)
	return err
}

func (work *Work) Execute(ctx context.Context, delivery workqueue.Delivery) error {
	return work.execute(ctx, delivery, false)
}

func (work *Work) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	return work.execute(ctx, delivery, true)
}

func (work *Work) execute(ctx context.Context, delivery workqueue.Delivery, observeOnly bool) error {
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	if workexecution.ValidateDelivery(item, attempt) != nil || attempt.StartedAt == nil {
		return conflict("Runner migration has no current started assignment")
	}
	if _, err := work.load(ctx, item); err != nil {
		return err
	}
	if observeOnly {
		return work.operator.migration.Execute(ctx, item.TargetID, item.TaskID, nil)
	}
	return work.operator.migration.Execute(ctx, item.TargetID, item.TaskID,
		func(ctx context.Context, migration runnerscoped.Migration) ([]keyvalue.Condition, error) {
			return work.captureForward(ctx, delivery, migration)
		})
}

func (work *Work) captureForward(ctx context.Context, delivery workqueue.Delivery,
	migration runnerscoped.Migration,
) ([]keyvalue.Condition, error) {
	if err := work.operator.readiness(ctx); err != nil {
		return nil, err
	}
	item, attempt := delivery.Item.Item, delivery.Attempt.Attempt
	proof, err := work.operator.verification.VerifyRunnerRegistration(ctx, item.TargetID,
		migration.Request.ConnectionID, migration.Request.RepositoryID)
	if err != nil {
		return nil, err
	}
	if proof.GitHubID != migration.GitHubID || len(proof.Conditions) == 0 {
		return nil, conflict("Verified GitHub Runner registration changed after acceptance")
	}
	view, err := work.queue.ReadResume(ctx, item.TaskID, item.ID, attempt.ID, attempt.Executor)
	if err != nil {
		return nil, err
	}
	if !view.ActiveAttempt || !view.CurrentExecutor || view.Attempt.Attempt.Direction != attempt.Direction ||
		!time.Now().UTC().Before(attempt.Deadline) ||
		attempt.Direction == workexecution.Forward &&
			(view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked) {
		return nil, conflict("Runner migration lost forward authority while waiting for an idle Runner")
	}
	return append(proof.Conditions,
		keyvalue.Condition{Key: workqueue.TaskKey(item.TaskID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		keyvalue.Condition{Key: workqueue.AttemptKey(attempt.ID), ModRevision: view.Attempt.Revision},
		keyvalue.Condition{Key: workqueue.ExecutorKey(attempt.Executor), ModRevision: view.ExecutorRevision},
	), nil
}

func (work *Work) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	attempt := delivery.Attempt.Attempt
	request, recorded, err := queue.ReadCompletion(
		ctx,
		attempt.TaskID,
		attempt.WorkItemID,
		attempt.ID,
		attempt.Executor,
	)
	if err != nil || recorded {
		return err
	}
	state, err := work.load(ctx, request.Item.Item)
	if err != nil {
		return err
	}
	authority := stateGuard(state)
	result := workexecution.Completion{Evidence: *request.Item.Item.Input, Outcome: failure(request.Task, request.Item)}
	switch state.Record.Phase {
	case runnerscoped.Pending:
		result.EffectsSettled = true
		authority, err = work.operator.records.PrepareCancel(ctx, state)
	case runnerscoped.Complete:
		result.EffectsSettled = true
		if request.Attempt.Attempt.StartedAt != nil {
			result.Outcome = workexecution.Succeeded
		}
	}
	if err != nil {
		return err
	}
	request.Result, request.At = result, at
	completion, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	committed, err := queue.Commit(ctx, authority, completion)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return conflict("Runner migration completion changed concurrently")
	}
	return nil
}

func (work *Work) PrepareFinish(
	_ context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if finished.Task.Task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, conflict("Runner migration Task has an unexpected work graph")
	}
	item := finished.Items[0]
	if item.Item.Operation != workexecution.RunnerScopedMigration ||
		item.Item.TargetID != finished.Task.Task.TargetID ||
		item.Item.Phase != workexecution.Settled ||
		item.Item.ActiveAttemptID != "" ||
		item.Item.Result == nil {
		return workqueue.Change{}, conflict("Runner migration Task still has unresolved effects")
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision},
	}}, nil
}

func (work *Work) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, _ int64, at time.Time,
) (workqueue.Change, error) {
	if task.Task.CollectionStartedAt != nil || item.Item.Phase != workexecution.Attention ||
		previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil ||
		previous.Attempt.TaskID != task.Task.ID || previous.Attempt.WorkItemID != item.Item.ID ||
		previous.Attempt.Number != item.Item.AttemptNumber {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Runner migration recovery requires the exact failed attempt",
		)
	}
	state, err := work.load(ctx, item.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	if state.Record.Phase != runnerscoped.Cutover && state.Record.Phase != runnerscoped.Complete {
		return workqueue.Change{}, errs.New(
			errs.KindTaskNotRetryable,
			"Runner migration has no issued cutover to reconcile",
		)
	}
	change, err := workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
		Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: at.Add(activationTimeout),
	}, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, stateGuard(state).Conditions...)
	return change, nil
}

func (work *Work) RetireWork(
	ctx context.Context,
	view workqueue.View,
	_ time.Time,
	_ bool,
) (bool, workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if item.Operation != workexecution.RunnerScopedMigration || item.TaskID != task.ID ||
		task.CollectionStartedAt == nil || item.Phase != workexecution.Settled {
		return false, workqueue.Change{}, conflict("Runner migration has no Task collection fence")
	}
	state, exists, err := work.operator.records.Load(ctx, item.TargetID)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	if exists && state.Record.ID == task.ID && state.Record.Phase != runnerscoped.Complete {
		return false, workqueue.Change{}, nil
	}
	return true, workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: runnerscoped.Key(item.TargetID), ModRevision: state.Revision},
	}}, nil
}

func (work *Work) PrepareAttemptRetirement(
	view workqueue.View,
	attempt workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	item, task := view.Item.Item, view.Task.Task
	if task.CollectionStartedAt == nil || item.Operation != workexecution.RunnerScopedMigration ||
		attempt.Attempt.TaskID != task.ID || attempt.Attempt.WorkItemID != item.ID || attempt.Attempt.FinishedAt == nil {
		return workqueue.Change{}, conflict("Runner migration attempt has no Task collection fence")
	}
	return workqueue.Change{}, nil
}
