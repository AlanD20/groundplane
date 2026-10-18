package entrywork

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/entryexecution"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entrywork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type AgentWork struct {
	inputs *entryrecord.InputRepository
	queue  *workqueue.Repository
}

func NewAgentWork(inputs *entryrecord.InputRepository, queue *workqueue.Repository) (*AgentWork, error) {
	if inputs == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "Entry execution requires captured input storage")
	}
	return &AgentWork{inputs: inputs, queue: queue}, nil
}

func (owner *AgentWork) LoadWorkAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.EntryConfigurationWorkAssignment, error) {
	assignment, _, err := owner.capturedAssignment(ctx, view)
	return assignment, err
}

func (owner *AgentWork) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.StartedAt != nil ||
		(view.Attempt.Attempt.Direction == workexecution.Forward && (view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked)) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry execution cannot obtain a new start grant")
	}
	_, guards, err := owner.capturedAssignment(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		resources, err := owner.inputs.AdmissionConditions(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		guards = append(guards, resources...)
		guards = append(guards,
			keyvalue.Condition{Key: workqueue.TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision},
			keyvalue.Condition{Key: workqueue.ItemKey(view.Work.Item.Item.ID), ModRevision: view.Work.Item.Revision})
		return workqueue.Change{Conditions: guards}, nil
	}
	current, desired, err := owner.inputs.ReadDesiredMatch(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !current {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Entry configuration changed before execution could start")
	}
	resources, err := owner.inputs.AdmissionConditions(ctx, view.Work)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards = append(guards, desired...)
	guards = append(guards, resources...)
	guards = append(guards, keyvalue.Condition{Key: workqueue.TaskKey(view.Work.Task.Task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(view.Work.Item.Item.ID), ModRevision: view.Work.Item.Revision})
	return workqueue.Change{Conditions: guards}, nil
}

func (owner *AgentWork) capturedAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.EntryConfigurationWorkAssignment, []keyvalue.Condition, error) {
	item, task, attempt := view.Work.Item.Item, view.Work.Task.Task, view.Attempt.Attempt
	if !view.CurrentExecutor || !view.ActiveAttempt || item.Operation != workexecution.EntryConfiguration ||
		attempt.Executor.Executor != workexecution.Agent ||
		workexecution.ValidateDelivery(item, attempt) != nil || !task.PublicationComplete ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return nil, nil, errs.New(errs.KindStateConflict, "Entry assignment no longer owns active host execution")
	}
	input, _, guards, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, nil, err
	}
	if input.EnvironmentId != task.Scope.EnvironmentID || input.EnvironmentId != item.TargetID {
		return nil, nil, errs.New(errs.KindStateConflict, "Entry assignment targets a different Environment")
	}
	owned, err := executionplan.OpenEntryConfigurationWorkInput(attempt, input)
	if err != nil {
		return nil, nil, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, nil, err
	}
	assignment := &agentpb.EntryConfigurationWorkAssignment{Attempt: identity, Input: owned}
	if attempt.Direction == workexecution.Recovery {
		captured, err := owner.inputs.LoadRecoveryInput(ctx, view.Work, attempt)
		if err != nil {
			return nil, nil, err
		}
		assignment.RecoverySource, err = entryexecution.EncodeRecovery(captured.Source, owned)
		if err != nil {
			return nil, nil, err
		}
		assignment.RecoveryBoundary, err = entryexecution.EncodeProgress(captured.Source.Forward, owned, captured.Boundary)
		if err != nil {
			return nil, nil, err
		}
		guards = append(guards, captured.Conditions...)
	}
	return assignment, guards, nil
}
