package remoteimage

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/remotefetch"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/remoteimages"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (service *Service) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, conflict()
	}
	plan, err := service.capture(ctx, task, item, 0)
	if err != nil {
		return workdispatch.Decision{}, err
	}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) ||
		request.Executor.Record.Identity != plan.Record.Executor {
		change, err := unstarted(request.Task, request.Item, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch,
		Deadline: item.Deadline, Change: planGuard(plan)}, nil
}

func (service *Service) Stop(_ context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil || !matches(task.Task, item.Item) {
		return workqueue.Change{}, conflict()
	}
	if item.Item.ActiveAttemptID == "" && item.Item.Phase != workexecution.Settled {
		return unstarted(task, item, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func unstarted(task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	outcome := workexecution.Failed
	if task.Task.Stop != nil {
		outcome = task.Task.Stop.Outcome
	} else if item.Item.ForwardRevoked {
		outcome = workexecution.Superseded
	}
	return workqueue.PrepareUnstartedSettlement(task, item, workexecution.Completion{
		Outcome: outcome, Evidence: *item.Item.Input, EffectsSettled: true,
	}, at)
}

func (service *Service) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	plan, err := service.openAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Attempt.Attempt.StartedAt != nil {
		return workqueue.Change{}, conflict()
	}
	return planGuard(plan), nil
}

func (service *Service) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.RemoteImageAssignment, error) {
	plan, err := service.openAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	return remotefetch.EncodeAssignment(view.Attempt.Attempt, plan.Record.Image)
}

func (service *Service) openAssignment(ctx context.Context,
	view workqueue.ResumeView) (keyvalue.Versioned[remoteimages.Plan], error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || attempt.Direction != workexecution.Forward ||
		workexecution.ValidateDelivery(item, attempt) != nil {
		return keyvalue.Versioned[remoteimages.Plan]{}, conflict()
	}
	plan, err := service.capture(ctx, task, item, 0)
	if err == nil && plan.Record.Executor != attempt.Executor {
		err = conflict()
	}
	return plan, err
}

func (service *Service) capture(ctx context.Context, task workexecution.Task, item workexecution.Item,
	revision int64) (keyvalue.Versioned[remoteimages.Plan], error) {
	if !matches(task, item) {
		return keyvalue.Versioned[remoteimages.Plan]{}, conflict()
	}
	plan, err := remoteimages.ReadPlan(ctx, service.reader, task.ID, revision)
	if err != nil {
		return plan, err
	}
	if plan.Revision == 0 || plan.Record.Executor.HostID != task.TargetID ||
		plan.Record.Image.Reference() != task.TargetName {
		return keyvalue.Versioned[remoteimages.Plan]{}, conflict()
	}
	_, digest, err := imagefetch.Encode(plan.Record.Image)
	if err != nil || item.Input.SHA256 != digest {
		return keyvalue.Versioned[remoteimages.Plan]{}, conflict()
	}
	return plan, nil
}

func matches(task workexecution.Task, item workexecution.Item) bool {
	return task.Action == workexecution.ActionFetch && item.Operation == workexecution.RemoteImage &&
		item.TaskID == task.ID && item.TargetID == task.TargetID && item.Input != nil && *item.Input == task.Intent &&
		item.Placement.Executor == workexecution.Agent && item.Placement.HostID == task.TargetID
}

func planGuard(plan keyvalue.Versioned[remoteimages.Plan]) workqueue.Change {
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: remoteimages.PlanKey(plan.Record.TaskID),
		ModRevision: plan.Revision}}}
}

func (service *Service) PrepareFinish(_ context.Context, finished workqueue.FinishedWork,
	_ time.Time) (workqueue.Change, error) {
	if finished.Task.Task.ExpectedItems != 1 || len(finished.Items) != 1 {
		return workqueue.Change{}, conflict()
	}
	item := finished.Items[0]
	if !matches(finished.Task.Task, item.Item) || item.Item.Phase != workexecution.Settled ||
		item.Item.Result == nil || item.Item.ActiveAttemptID != "" {
		return workqueue.Change{}, conflict()
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: workqueue.ItemKey(item.Item.ID),
		ModRevision: item.Revision}}}, nil
}

func (service *Service) RetireWork(ctx context.Context, view workqueue.View, _ time.Time,
	_ bool) (bool, workqueue.Change, error) {
	if !matches(view.Task.Task, view.Item.Item) || view.Task.Task.CollectionStartedAt == nil {
		return false, workqueue.Change{}, conflict()
	}
	taskID := view.Task.Task.ID
	plan, err := remoteimages.ReadPlan(ctx, service.reader, taskID, 0)
	if err != nil || plan.Revision == 0 {
		return err == nil, workqueue.Change{}, err
	}
	change := planGuard(plan)
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: remoteimages.PlanKey(taskID)}}
	return true, change, nil
}

func (service *Service) PrepareAttemptRetirement(view workqueue.View,
	attempt workqueue.AttemptSnapshot) (workqueue.Change, error) {
	if !matches(view.Task.Task, view.Item.Item) || view.Task.Task.CollectionStartedAt == nil ||
		attempt.Attempt.TaskID != view.Task.Task.ID || attempt.Attempt.WorkItemID != view.Item.Item.ID ||
		attempt.Attempt.FinishedAt == nil {
		return workqueue.Change{}, conflict()
	}
	return workqueue.Change{}, nil
}
