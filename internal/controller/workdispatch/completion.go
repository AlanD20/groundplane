package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CompletionOwner prepares action-specific aggregate resource settlement from
// finished work. It performs no external effects and commits nothing itself.
// Its comparisons/writes join the Task outcome and request receipt atomically.
type CompletionOwner interface {
	PrepareFinish(context.Context, workqueue.FinishedWork, time.Time) (workqueue.Change, error)
}

type CompletionRegistration struct {
	Action workexecution.Action
	// Operation selects a single-item action; empty selects an aggregate owner.
	Operation workexecution.Operation
	Owner     CompletionOwner
}

type completionKey struct {
	action    workexecution.Action
	operation workexecution.Operation
}

type Completions struct {
	queue  *workqueue.Repository
	owners map[completionKey]CompletionOwner
}

func NewCompletions(queue *workqueue.Repository, registrations []CompletionRegistration) (*Completions, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "task completion requires storage and action owners")
	}
	owners := make(map[completionKey]CompletionOwner, len(registrations))
	for _, registration := range registrations {
		key := completionKey{registration.Action, registration.Operation}
		if !workexecution.ValidAction(registration.Action) || registration.Operation != "" && !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil || owners[key] != nil {
			return nil, errs.New(errs.KindInternal, "task completion action is invalid or duplicated")
		}
		owners[key] = registration.Owner
	}
	return &Completions{queue: queue, owners: owners}, nil
}

func (completions *Completions) Finish(ctx context.Context, taskID string, at time.Time, retention time.Duration) error {
	if at.IsZero() || retention <= 0 {
		return errs.New(errs.KindInternal, "task completion requires a timestamp and positive retention")
	}
	finished, err := completions.queue.ReadFinishedWork(ctx, taskID)
	if err != nil {
		return err
	}
	if finished.Task.Task.FinishedAt != nil {
		return nil
	}
	key := completionKey{action: finished.Task.Task.Action}
	owner := completions.owners[key]
	if finished.Task.Task.ExpectedItems == 1 && len(finished.Items) == 1 {
		key.operation = finished.Items[0].Item.Operation
		if single := completions.owners[key]; single != nil {
			owner = single
		}
	}
	if owner == nil {
		return errs.New(errs.KindStateConflict, "task action has no aggregate completion owner")
	}
	aggregate, err := owner.PrepareFinish(ctx, finished, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(aggregate.Mutations)
	outcome, err := completions.queue.PrepareTaskFinish(ctx, finished.Task, at, at.Add(retention))
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(outcome.Mutations)
	result, err := completions.queue.Commit(ctx, aggregate, outcome)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "task or aggregate state changed before completion")
	}
	return nil
}
