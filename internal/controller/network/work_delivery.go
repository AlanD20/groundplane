package network

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AgentWork struct {
	inputs *networkwork.InputRepository
	queue  *workqueue.Repository
}

func NewAgentWork(inputs *networkwork.InputRepository, queue *workqueue.Repository) (*AgentWork, error) {
	if inputs == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Network execution requires captured input storage")
	}
	return &AgentWork{inputs: inputs, queue: queue}, nil
}

func (owner *AgentWork) LoadWorkAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.NetworkWorkAssignment, error) {
	input, _, err := owner.capturedAssignment(ctx, view)
	if err != nil {
		return nil, err
	}
	return input, nil
}

func (owner *AgentWork) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.StartedAt != nil || view.Attempt.Attempt.Direction == workexecution.Forward && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Network execution cannot obtain a new start grant")
	}
	_, guards, err := owner.capturedAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Attempt.Attempt.Direction == workexecution.Forward {
		current, desired, err := owner.inputs.ReadDesiredMatch(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		if !current {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Network configuration changed before execution could start")
		}
		guards = append(guards, desired...)
	}
	guards = append(guards, keyvalue.Condition{Key: workqueue.TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(view.Work.Item.Item.ID), ModRevision: view.Work.Item.Revision})
	return workqueue.Change{Conditions: guards}, nil
}

func (owner *AgentWork) capturedAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.NetworkWorkAssignment, []keyvalue.Condition, error) {
	item, task, attempt := view.Work.Item.Item, view.Work.Task.Task, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.Network ||
		attempt.Executor.Executor != workexecution.Agent ||
		workexecution.ValidateDelivery(item, attempt) != nil || !task.PublicationComplete ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Network assignment no longer owns active host execution")
	}
	if attempt.Direction == workexecution.Recovery {
		captured, err := owner.inputs.LoadRecoveryInput(ctx, view.Work, attempt)
		if err != nil {
			return nil, nil, err
		}
		source, err := networkexecution.EncodeRecovery(captured.Source)
		if err != nil {
			return nil, nil, err
		}
		boundary, err := networkexecution.EncodeProgress(captured.Source.Forward, captured.Input.Action, captured.Input.NetworkId, captured.Boundary)
		if err != nil {
			return nil, nil, err
		}
		identity, err := workexecution.EncodeAgentAttempt(attempt)
		if err != nil {
			return nil, nil, err
		}
		return &agentpb.NetworkWorkAssignment{Attempt: identity, Input: captured.Input, RecoverySource: source, RecoveryBoundary: boundary}, captured.Conditions, nil
	}
	input, guards, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, nil, err
	}
	if input.EnvironmentId != task.Scope.EnvironmentID || input.NetworkId != item.TargetID || (task.Action != workexecution.ActionApply && item.TargetID != task.TargetID) {
		return nil, nil, errs.New(errs.KindStateConflict, "Network assignment targets different captured resources")
	}
	owned, err := executionplan.OpenNetworkWorkInput(attempt, input)
	if err != nil {
		return nil, nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, nil, err
	}
	return &agentpb.NetworkWorkAssignment{Attempt: identity, Input: owned}, guards, nil
}
