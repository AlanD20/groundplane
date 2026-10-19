package serviceremoval

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/lifecycleexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AgentWork struct {
	inputs  *serviceremovalwork.Repository
	queue   *workqueue.Repository
	records SettlementRecords
}

type SettlementRecords interface {
	PrepareServiceRemovalDecision(context.Context, workqueue.View, time.Time) (workqueue.Change, error)
	PrepareServiceRemovalCompletion(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
		time.Time,
	) (workqueue.Change, error)
	PrepareRecoveredServiceRemoval(
		context.Context,
		workqueue.ResumeView,
		*agentpb.ServiceLifecycleWorkInput,
		time.Time,
	) (workqueue.Change, workexecution.Completion, error)
}

func NewAgentWork(
	inputs *serviceremovalwork.Repository,
	queue *workqueue.Repository,
	records SettlementRecords,
) (*AgentWork, error) {
	if inputs == nil || queue == nil || records == nil {
		return nil, errs.New(errs.KindInternal, "Service removal requires captured input and work storage")
	}
	return &AgentWork{inputs: inputs, queue: queue, records: records}, nil
}

func (owner *AgentWork) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ServiceLifecycleAssignment, error) {
	assignment, _, err := owner.capturedAssignment(ctx, view)
	return assignment, err
}

func (owner *AgentWork) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		if view.Attempt.Attempt.StartedAt != nil {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal recovery already started")
		}
		_, change, err := owner.recoveryAssignment(ctx, view)
		return change, err
	}
	return owner.authorizeForwardStart(ctx, view)
}

// capturedAssignment selects only the frozen runtime; neither reconnect nor
// delayed delivery may rebuild a removal from current desired state.
func (owner *AgentWork) capturedAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.ServiceLifecycleAssignment, workqueue.Change, error) {
	item, task, attempt := view.Work.Item.Item, view.Work.Task.Task, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.ServiceRemoval ||
		attempt.Direction != workexecution.Forward || attempt.Executor.Executor != workexecution.Agent ||
		workexecution.ValidateDelivery(
			item,
			attempt,
		) != nil || !task.PublicationComplete || task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return nil, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Service removal no longer owns this host assignment",
		)
	}
	captured, guard, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if captured.Runtime == nil || captured.HostID != attempt.Executor.HostID {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal belongs to another host")
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	continuation, continuationGuard, err := owner.inputs.LoadContinuation(ctx, view.Work, attempt, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	assignment := &agentpb.ServiceLifecycleAssignment{Attempt: identity, Input: input}
	if continuation != nil {
		assignment.Continuation, err = lifecycleexecution.EncodeContinuation(input, *continuation)
		if err != nil {
			return nil, workqueue.Change{}, err
		}
	}
	return assignment, workqueue.Change{Conditions: []keyvalue.Condition{guard, continuationGuard}}, nil
}

func (owner *AgentWork) authorizeForwardStart(
	ctx context.Context,
	view workqueue.ResumeView,
) (workqueue.Change, error) {
	if view.Attempt.Attempt.StartedAt != nil || view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal cannot obtain a new start grant")
	}
	_, change, err := owner.capturedAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	input, _, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	boundary, err := owner.inputs.PrepareForwardBoundary(ctx, view.Work, input)
	if err != nil {
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, boundary.Conditions...)
	return change, nil
}
