package workdispatch

import (
	"context"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// AgentStartOwner compares operation-owned captured sources and current effect
// authority with Start. It does not execute host effects or select new intent.
type AgentStartOwner interface {
	AuthorizeAgentStart(context.Context, workqueue.ResumeView) (workqueue.Change, error)
}

type AgentStartRegistration struct {
	Operation workexecution.Operation
	Owner     AgentStartOwner
}

type AgentStarts struct {
	queue  *workqueue.Repository
	owners map[workexecution.Operation]AgentStartOwner
}

// Operations is the closed catalog admitted by the composed start owners.
func (starts *AgentStarts) Operations() []workexecution.Operation {
	operations := make([]workexecution.Operation, 0, len(starts.owners))
	for operation := range starts.owners {
		operations = append(operations, operation)
	}
	slices.Sort(operations)
	return operations
}

func NewAgentStarts(queue *workqueue.Repository, registrations []AgentStartRegistration) (*AgentStarts, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "Agent start requires storage and operation owners")
	}
	owners := make(map[workexecution.Operation]AgentStartOwner, len(registrations))
	for _, registration := range registrations {
		if !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil ||
			owners[registration.Operation] != nil {
			return nil, errs.New(errs.KindInternal, "Agent start operation is invalid or duplicated")
		}
		owners[registration.Operation] = registration.Owner
	}
	return &AgentStarts{queue: queue, owners: owners}, nil
}

func (starts *AgentStarts) Start(ctx context.Context, authorization agentregistration.WorkAuthorization,
	request *agentpb.WorkStartRequest,
) (*agentpb.WorkStartAck, error) {
	if ctx == nil || starts == nil || request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, errs.New(errs.KindValidationFailed, "Agent start request is invalid")
	}
	guards, err := authorization.Conditions()
	if err != nil {
		return nil, err
	}
	authenticated := authorization.Identity()
	claimed, err := workexecution.DecodeAgentAttempt(request.Attempt, authenticated)
	if err != nil {
		return nil, err
	}
	view, err := starts.queue.ReadResume(ctx, claimed.TaskID, claimed.WorkItemID, claimed.ID, authenticated)
	if err != nil {
		return nil, err
	}
	attempt := view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || !workexecution.SameAssignment(claimed, attempt) ||
		workexecution.ValidateDelivery(view.Work.Item.Item, attempt) != nil ||
		view.Work.Task.Task.CollectionStartedAt != nil || view.Work.Task.Task.FinishedAt != nil ||
		!view.Work.Task.Task.PublicationComplete {
		return nil, errs.New(errs.KindStateConflict, "Agent start no longer owns this active assignment")
	}
	owner := starts.owners[view.Work.Item.Item.Operation]
	if owner == nil {
		return nil, errs.New(errs.KindStateConflict, "Agent operation has no start owner")
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, err
	}
	if attempt.StartedAt != nil {
		var sequence uint64
		if view.Checkpoint.Record != nil {
			sequence = view.Checkpoint.Record.Sequence
		}
		return &agentpb.WorkStartAck{
			Attempt:            identity,
			StartedAt:          timestamppb.New(*attempt.StartedAt),
			Reconcile:          true,
			CheckpointSequence: sequence,
		}, nil
	}
	authority, err := owner.AuthorizeAgentStart(ctx, view)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(authority.Mutations)
	if len(authority.Conditions) == 0 {
		return nil, errs.New(errs.KindInternal, "Agent start requires operation authority comparisons")
	}
	executor, err := starts.queue.ReadExecutor(ctx, view.Work.Item.Item.Placement)
	if err != nil {
		return nil, err
	}
	at := time.Now().UTC()
	change, err := workqueue.PrepareStart(view.Work.Task, view.Work.Item, view.Attempt, executor, at)
	if err != nil {
		return nil, err
	}
	defer keyvalue.ZeroMutationBytes(change.Mutations)
	result, err := starts.queue.Commit(ctx, workqueue.Change{Conditions: guards}, authority, change)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return nil, err
	}
	if !result.Succeeded {
		return nil, errs.New(errs.KindStateConflict, "Agent start authority changed; reload the saved attempt")
	}
	return &agentpb.WorkStartAck{Attempt: identity, StartedAt: timestamppb.New(at)}, nil
}
