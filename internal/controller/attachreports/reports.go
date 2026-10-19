// Package attachreports authenticates and settles the native Attach-removal
// report family. Direct Detach and hierarchy removal supply their own captured
// resource guards; neither report path interprets an old Task or generic plan.
package attachreports

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/attachexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Owner is the effect boundary, not a report-storage facade: hierarchy owns
// frozen deletion actions, while direct Detach owns its binding and runtime.
type Owner interface {
	RemovalInput(context.Context, workqueue.ResumeView) (attachexecution.RemovalInput, error)
	RecoveryInput(context.Context, workqueue.ResumeView) (attachexecution.RecoveryInput, error)
	PrepareRemovalCheckpoint(
		context.Context,
		workqueue.ResumeView,
		attachexecution.RemovalInput,
		attachexecution.RemovalReport,
	) (workqueue.Change, error)
	PrepareRemovalCompletion(
		context.Context,
		workqueue.ResumeView,
		attachexecution.RemovalInput,
		time.Time,
	) (workqueue.Change, error)
	PrepareRemovalFailure(
		context.Context,
		workqueue.ResumeView,
		attachexecution.RemovalInput,
		attachexecution.FailureReason,
		time.Time,
	) (workqueue.Change, workexecution.Completion, error)
	LoadRemovalFailure(context.Context, workqueue.ResumeView) ([]byte, error)
	PrepareRemovalInspection(
		context.Context,
		workqueue.ResumeView,
		attachexecution.RemovalInspection,
		time.Time,
	) (workqueue.Change, workexecution.Completion, error)
	LoadRemovalInspection(context.Context, workqueue.ResumeView) ([]byte, error)
}

type Registration struct {
	Operation workexecution.Operation
	Owner     Owner
}

type Reports struct {
	queue  *workqueue.Repository
	owners map[workexecution.Operation]Owner
}

func New(queue *workqueue.Repository, registrations []Registration) (*Reports, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "Attach reporting requires execution storage and operation owners")
	}
	owners := make(map[workexecution.Operation]Owner, len(registrations))
	for _, registration := range registrations {
		if (registration.Operation != workexecution.Hierarchy && registration.Operation != workexecution.Detach) ||
			registration.Owner == nil || owners[registration.Operation] != nil {
			return nil, errs.New(errs.KindInternal, "Attach report operation is invalid or duplicated")
		}
		owners[registration.Operation] = registration.Owner
	}
	return &Reports{queue: queue, owners: owners}, nil
}

func (service *Reports) readView(ctx context.Context, authorization agentregistration.WorkAuthorization,
	identity *agentpb.WorkAttemptIdentity, direction workexecution.Direction,
) (workqueue.ResumeView, Owner, []keyvalue.Condition, error) {
	guards, err := authorization.Conditions()
	if err != nil {
		return workqueue.ResumeView{}, nil, nil, err
	}
	claimed, err := workexecution.DecodeAgentAttempt(identity, authorization.Identity())
	if err != nil {
		return workqueue.ResumeView{}, nil, nil, err
	}
	view, err := authorization.ReadResult(ctx, service.queue, claimed.TaskID, claimed.WorkItemID, claimed.ID)
	if err != nil {
		return workqueue.ResumeView{}, nil, nil, err
	}
	owner := service.owners[view.Work.Item.Item.Operation]
	if owner == nil || !workexecution.SameAssignment(claimed, view.Attempt.Attempt) ||
		view.Attempt.Attempt.Direction != direction ||
		view.Work.Task.Task.CollectionStartedAt != nil ||
		!view.Work.Task.Task.PublicationComplete {
		return workqueue.ResumeView{}, nil, nil, errs.New(
			errs.KindStateConflict,
			"Attach report has no retained matching assignment",
		)
	}
	return view, owner, guards, nil
}

func (service *Reports) commit(ctx context.Context, guards []keyvalue.Condition, changes ...workqueue.Change) error {
	result, err := service.queue.Commit(ctx, append([]workqueue.Change{{Conditions: guards}}, changes...)...)
	keyvalue.ClearValues(result.FailureReads)
	if err != nil {
		return err
	}
	if !result.Succeeded {
		return errs.New(errs.KindStateConflict, "Attach execution ownership changed; resend the saved report")
	}
	return nil
}

func (service *Reports) commitCompletion(ctx context.Context, view workqueue.ResumeView, guards []keyvalue.Condition,
	proof workqueue.Change, result workexecution.Completion, at time.Time,
) error {
	completion, err := service.queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	completion.Result, completion.At = result, at
	var change workqueue.Change
	if view.Attempt.Attempt.Direction == workexecution.Forward && !result.EffectsSettled {
		change, err = workqueue.PrepareCompletionWithRecovery(completion, at.Add(attachexecution.InspectionBudget))
	} else {
		change, err = workqueue.PrepareCompletion(completion)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	return service.commit(ctx, guards, proof, change)
}

func completedOutcome(view workqueue.View) workexecution.Outcome {
	if view.Task.Task.Stop != nil {
		return view.Task.Task.Stop.Outcome
	}
	if view.Item.Item.ForwardRevoked {
		return workexecution.Superseded
	}
	return workexecution.Succeeded
}
