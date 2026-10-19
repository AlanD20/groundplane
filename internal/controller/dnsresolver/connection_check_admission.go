package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *ConnectionCheckExecutor) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	item := request.Item.Item
	if item.Operation != workexecution.DNSCheck || item.Placement.Executor != workexecution.Controller ||
		item.Input == nil ||
		request.Direction != workexecution.Forward ||
		item.AttemptNumber != 0 {
		return workdispatch.Decision{}, errs.New(errs.KindValidationFailed, "DNS checks cannot retry captured queries")
	}
	loaded, err := executor.records.Load(ctx, item.TaskID)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	record := loaded.Record
	digest, err := dnschecks.PlanHash(record)
	if err != nil || record.Phase != dnschecks.Ready || record.Owner.WorkItemID != item.ID ||
		record.Owner.PlanID != item.Input.ID ||
		digest != item.Input.SHA256 ||
		record.Source.ComponentID != item.TargetID {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"DNS work differs from captured resolver input",
		)
	}
	if !item.Deadline.After(at) {
		change, err := executor.settleUnstartedDNS(ctx, request.Task, request.Item, workexecution.Failed, at)
		if err != nil {
			return workdispatch.Decision{}, err
		}
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, nil
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
		Change: workqueue.Change{
			Conditions: []keyvalue.Condition{{Key: dnschecks.RecordKey(item.TaskID), ModRevision: loaded.Revision}},
		}}, nil
}

func (executor *ConnectionCheckExecutor) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "DNS check has no stop request")
	}
	if item.Item.AttemptNumber == 0 && item.Item.ActiveAttemptID == "" && item.Item.Phase != workexecution.Settled {
		return executor.settleUnstartedDNS(ctx, task, item, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (executor *ConnectionCheckExecutor) settleUnstartedDNS(ctx context.Context, task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot, outcome workexecution.Outcome, at time.Time,
) (workqueue.Change, error) {
	operation, result, err := executor.records.PrepareUnstarted(ctx, task, item, outcome, at)
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
