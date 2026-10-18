package serviceremoval

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/removalexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/serviceremovalwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (owner *AgentWork) LoadRemovalRecoveryAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ServiceRemovalRecoveryAssignment, error) {
	assignment, _, err := owner.recoveryAssignment(ctx, view)
	return assignment, err
}

func (owner *AgentWork) recoveryAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.ServiceRemovalRecoveryAssignment, workqueue.Change, error) {
	task, item, attempt := view.Work.Task.Task, view.Work.Item.Item, view.Attempt.Attempt
	if !view.ActiveAttempt || !view.CurrentExecutor || item.Operation != workexecution.ServiceRemoval ||
		attempt.Direction != workexecution.Recovery || attempt.Executor.Executor != workexecution.Agent ||
		workexecution.ValidateDelivery(item, attempt) != nil || !task.PublicationComplete ||
		task.FinishedAt != nil || task.CollectionStartedAt != nil {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal recovery no longer owns this host assignment")
	}
	captured, guard, err := owner.inputs.Load(ctx, view.Work)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if captured.Runtime == nil || captured.HostID != attempt.Executor.HostID {
		return nil, workqueue.Change{}, errs.New(errs.KindStateConflict, "Service removal recovery belongs to another host")
	}
	input, err := BuildHostInput(captured)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	saved, err := owner.inputs.LoadRecovery(ctx, view.Work, input)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	if err := saved.Capture.ValidateFor(attempt, input); err != nil {
		return nil, workqueue.Change{}, err
	}
	raw, err := removalexecution.EncodeCapture(input, saved.Capture)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	identity, err := workexecution.EncodeAgentAttempt(attempt)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	assignment := &agentpb.ServiceRemovalRecoveryAssignment{Attempt: identity, Removal: input, CapturedFailure: raw}
	if reference := captured.Configuration; reference != nil {
		digest, err := hex.DecodeString(reference.SHA256)
		if err != nil {
			return nil, workqueue.Change{}, err
		}
		assignment.Configuration = &agentpb.WorkConfigurationReference{ConfigurationId: reference.ID,
			EnvironmentId: reference.EnvironmentID, Generation: reference.Generation, Sha256: digest}
	}
	conditions, err := owner.inputs.RuntimeGuards(ctx, captured, view.Work.ReadRevision)
	if err != nil {
		return nil, workqueue.Change{}, err
	}
	conditions = append(conditions, guard,
		keyvalue.Condition{Key: serviceremovalwork.RecoveryKey(item.ID), ModRevision: saved.Revision},
		keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision})
	return assignment, workqueue.Change{Conditions: conditions}, nil
}
