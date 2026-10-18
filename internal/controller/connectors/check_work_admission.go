package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Admit binds assignment to the captured permission check. It never reloads the
// current Connector or replaces credentials with a newly edited value.
func (executor *CheckExecutor) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item := request.Item.Item
	if item.Operation != workexecution.ConnectorCheck || item.Placement.Executor != workexecution.Controller || item.Input == nil {
		return workdispatch.Decision{}, errs.New(errs.KindValidationFailed, "connector check work is invalid")
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	record := loaded.Record
	defer record.Clear()
	digest, err := connectorchecks.PlanHash(record)
	if err != nil || record.Owner.TaskID != item.TaskID || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != item.Input.ID || digest != item.Input.SHA256 || record.Connector.Connector.ID != item.TargetID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "connector check captured input does not match its work")
	}
	if !item.Deadline.After(at) {
		if item.AttemptNumber == 0 {
			change, err := executor.settleUnstarted(ctx, request.Task, request.Item, workexecution.Failed, at)
			if err != nil {
				return workdispatch.Decision{}, err
			}
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
		}
		if record.Phase != connectorchecks.CleanupRequired || request.Direction != workexecution.Recovery {
			return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "expired connector work does not match captured cleanup")
		}
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		change.Conditions = append(change.Conditions, keyvalue.Condition{Key: connectorchecks.Key(item.TaskID), ModRevision: loaded.Revision})
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, nil
	}
	if request.Direction == workexecution.Recovery && record.Phase == connectorchecks.Ready {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "connector check has no started probe to clean up")
	}
	return workdispatch.Decision{
		Disposition: workdispatch.Dispatch,
		Deadline:    item.Deadline,
		Change:      workqueue.Change{Conditions: []keyvalue.Condition{{Key: connectorchecks.Key(item.TaskID), ModRevision: loaded.Revision}}},
	}, nil
}

func (executor *CheckExecutor) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "connector check has no stop request")
	}
	if item.Item.AttemptNumber == 0 && item.Item.ActiveAttemptID == "" && item.Item.Phase != workexecution.Settled {
		return executor.settleUnstarted(ctx, task, item, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (executor *CheckExecutor) settleUnstarted(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	operation, result, err := executor.repository.PrepareUnstarted(ctx, task, item, outcome, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	settlement, err := workqueue.PrepareUnstartedSettlement(task, item, result, at)
	if err != nil {
		return workqueue.Change{}, err
	}
	operation.Conditions = append(operation.Conditions, settlement.Conditions...)
	operation.Mutations = append(operation.Mutations, settlement.Mutations...)
	return operation, nil
}
