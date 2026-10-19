package edgechecks

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	if item.Operation != workexecution.EdgeCheck || item.Placement.Executor != workexecution.Controller ||
		item.Input == nil {
		return workdispatch.Decision{}, errs.New(errs.KindValidationFailed, "edge check work is invalid")
	}
	loaded, err := executor.repository.Load(ctx, item.TaskID)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	record := loaded.Record
	digest, err := checkrecord.PlanHash(record)
	if err != nil || record.Owner.WorkItemID != item.ID || record.Owner.PlanID != item.Input.ID ||
		digest != item.Input.SHA256 || record.Source.Request.TargetID() != item.TargetID {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "edge check work differs from captured input")
	}
	if !item.Deadline.After(at) {
		if item.AttemptNumber == 0 {
			change, err := executor.settleUnstarted(ctx, request.Task, request.Item, workexecution.Failed, at)
			if err != nil {
				return workdispatch.Decision{}, err
			}
			return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
		}
		if request.Direction != workexecution.Recovery || record.Phase != checkrecord.CleanupRequired {
			return workdispatch.Decision{}, errs.New(
				errs.KindStateConflict,
				"expired edge work does not match retained cleanup",
			)
		}
		change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: checkrecord.Key(item.TaskID), ModRevision: loaded.Revision},
		)
		return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, nil
	}
	if request.Direction == workexecution.Forward && record.Phase != checkrecord.Ready ||
		request.Direction == workexecution.Recovery && record.Phase != checkrecord.CleanupRequired {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"edge check phase does not permit this execution",
		)
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: checkrecord.Key(item.TaskID), ModRevision: loaded.Revision}},
		}}, nil
}

func (executor *Executor) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "edge check has no stop request")
	}
	if item.Item.AttemptNumber == 0 && item.Item.ActiveAttemptID == "" && item.Item.Phase != workexecution.Settled {
		return executor.settleUnstarted(ctx, task, item, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (executor *Executor) settleUnstarted(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	outcome workexecution.Outcome, at time.Time,
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
