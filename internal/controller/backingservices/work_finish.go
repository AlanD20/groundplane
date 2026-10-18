package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	if len(finished.Items) != 1 || finished.Task.Task.ExpectedItems != 1 || finished.Task.Task.Action != workexecution.ActionCreate {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation requires one settled native work item")
	}
	item := finished.Items[0]
	if item.Item.Phase != workexecution.Settled || item.Item.ActiveAttemptID != "" || item.Item.Result == nil ||
		item.Item.Operation != workexecution.BackingProvision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation still has unresolved execution effects")
	}
	view := workqueue.View{Task: finished.Task, Item: item, ReadRevision: finished.ReadRevision}
	input, capture, err := owner.records.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(input.Wire)
	change := workqueue.Change{Conditions: []keyvalue.Condition{capture,
		{Key: workqueue.TaskKey(finished.Task.Task.ID), ModRevision: finished.Task.Revision},
		{Key: workqueue.ItemKey(item.Item.ID), ModRevision: item.Revision}}}
	if item.Item.AttemptNumber == 0 {
		_, descriptor, err := input.Work()
		if err != nil || *item.Item.Result != *descriptor.Input || item.Item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "unexecuted backing creation cannot prove runtime application")
		}
		return change, nil
	}
	_, latest, err := owner.queue.ReadAttempt(ctx, finished.Task.Task.ID, item.Item.ID, item.Item.AttemptNumber, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	resume, err := owner.queue.ReadResumeAt(ctx, latest.Attempt.TaskID, latest.Attempt.WorkItemID, latest.Attempt.ID,
		latest.Attempt.Executor, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if latest.Attempt.FinishedAt == nil || latest.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation has no accepted terminal execution receipt")
	}
	if latest.Attempt.Direction == workexecution.Recovery {
		if item.Item.Cleanup != workexecution.CleanupComplete || latest.Attempt.Outcome != workexecution.Succeeded {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation cleanup remains incomplete")
		}
		if _, err := owner.records.ReadRecoveryResult(ctx, resume); err != nil {
			return workqueue.Change{}, err
		}
	} else if latest.Attempt.Result.ID == latest.Attempt.Input.ID {
		_, receipt, err := owner.records.ReadCompleted(ctx, latest.Attempt, finished.ReadRevision)
		if err != nil {
			return workqueue.Change{}, err
		}
		change.Conditions = append(change.Conditions, receipt)
	} else if _, _, err := owner.records.ReadFailure(ctx, resume); err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(latest.Attempt.ID), ModRevision: latest.Revision})
	return change, nil
}
