package releasegroup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type RemovalWork struct {
	records *releasegroups.RemovalRepository
	queue   *workqueue.Repository
}

func NewRemovalWork(records *releasegroups.RemovalRepository, queue *workqueue.Repository) (*RemovalWork, error) {
	if records == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Release Group removal storage is required")
	}
	return &RemovalWork{records: records, queue: queue}, nil
}

func (owner *RemovalWork) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	if !request.DecisionOnly || request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "Release Group removal requires an atomic Controller decision")
	}
	task, item := request.Task.Task, request.Item.Item
	outcome := workexecution.Succeeded
	if task.Stop != nil {
		outcome = task.Stop.Outcome
	} else if item.ForwardRevoked {
		outcome = workexecution.Superseded
	} else if !at.Before(task.Deadline) || !at.Before(item.Deadline) {
		outcome = workexecution.Failed
	}
	change, err := owner.records.PrepareDecision(ctx, workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}, outcome, at)
	return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
}

func (owner *RemovalWork) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal has no stop request")
	}
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal changed before cancellation")
	}
	return owner.records.PrepareDecision(ctx, view, task.Task.Stop.Outcome, at)
}

func (owner *RemovalWork) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	if finished.Task.Task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal requires one settled decision")
	}
	item := finished.Items[0]
	if item.Item.Phase != workexecution.Settled || item.Item.Result == nil || item.Item.Input == nil ||
		*item.Item.Result != *item.Item.Input || item.Item.AttemptNumber != 0 || item.Item.ActiveAttemptID != "" || item.Item.Cleanup != workexecution.CleanupNone {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal decision has not settled")
	}
	_, capture, err := owner.records.Load(ctx, workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision})
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{capture, {Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}}}, nil
}

func (owner *RemovalWork) RetireWork(ctx context.Context, view workqueue.View, at time.Time, final bool) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if !final || task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.CollectionStartedAt) || at.Before(*task.RetainUntil) || item.Phase != workexecution.Settled || item.AttemptNumber != 0 || item.ActiveAttemptID != "" {
		return false, workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal history is not ready for collection")
	}
	_, capture, err := owner.records.Load(ctx, view)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	return true, workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision}, {Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision},
		{Key: workqueue.AttemptIndexPrefix(item.ID), Prefix: true}}, Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: releasegroups.RemovalInputKey(item.ID)}}}, nil
}

func (owner *RemovalWork) PrepareAttemptRetirement(workqueue.View, workqueue.AttemptSnapshot) (workqueue.Change, error) {
	return workqueue.Change{}, errs.New(errs.KindStateConflict, "Release Group removal cannot own execution attempts")
}
