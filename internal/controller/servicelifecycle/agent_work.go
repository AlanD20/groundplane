package servicelifecycle

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/servicelifecyclework"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AgentWork struct {
	inputs *servicelifecyclework.Repository
	queue  *workqueue.Repository
}

func NewAgentWork(inputs *servicelifecyclework.Repository, queue *workqueue.Repository) (*AgentWork, error) {
	if inputs == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle assignments require captured input storage")
	}
	return &AgentWork{inputs: inputs, queue: queue}, nil
}

func (owner *AgentWork) LoadWorkAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ServiceLifecycleAssignment, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		assignment, _, err := owner.recoveryAssignment(ctx, view)
		return assignment, err
	}
	assignment, _, err := owner.capturedAssignment(ctx, view)
	return assignment, err
}

func (owner *AgentWork) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		if view.Attempt.Attempt.StartedAt != nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service lifecycle recovery already started")
		}
		_, change, err := owner.recoveryAssignment(ctx, view)
		return change, err
	}
	if view.Attempt.Attempt.StartedAt != nil || view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service lifecycle cannot obtain a new start grant")
	}
	assignment, guard, err := owner.capturedAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	_, continuationGuard, err := owner.inputs.LoadContinuation(ctx, view.Work, view.Attempt.Attempt, assignment.Input)
	if err != nil {
		return workqueue.Change{}, err
	}
	_, change, err := owner.inputs.PrepareAssignment(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guard, continuationGuard,
		keyvalue.Condition{Key: workqueue.TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(view.Work.Item.Item.ID), ModRevision: view.Work.Item.Revision},
	)
	return change, nil
}

func (owner *AgentWork) capturedAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ServiceLifecycleAssignment, keyvalue.Condition, error) {
	item, task, attempt := view.Work.Item.Item, view.Work.Task.Task, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.ServiceLifecycle ||
		attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Agent ||
		workexecution.ValidateDelivery(item, attempt) != nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return nil, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service lifecycle assignment no longer owns active host execution")
	}
	captured, guard, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	if captured.HostID != attempt.Executor.HostID || captured.Runtime == nil {
		return nil, keyvalue.Condition{}, errs.New(errs.KindStateConflict, "Service lifecycle capture does not authorize this host")
	}
	input, err := BuildWorkInput(captured)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	continuation, _, err := owner.inputs.LoadContinuation(ctx, view.Work, attempt, input)
	if err != nil {
		return nil, keyvalue.Condition{}, err
	}
	assignment := &agentpb.ServiceLifecycleAssignment{Attempt: identity, Input: input}
	if continuation != nil {
		assignment.Continuation, err = lifecycleexecution.EncodeContinuation(input, *continuation)
		if err != nil {
			return nil, keyvalue.Condition{}, err
		}
	}
	return assignment, guard, nil
}
