package backingservices

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/backingprovision"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backingprovisionwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Work owns scheduling and captured delivery for backing facade creation. It
// does not render configuration, choose an image or reconstruct latest intent.
type Work struct {
	records *backingprovisionwork.Repository
	queue   *workqueue.Repository
}

func NewWork(records *backingprovisionwork.Repository, queue *workqueue.Repository) (*Work, error) {
	if records == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "backing creation requires its native input and execution repositories")
	}
	return &Work{records: records, queue: queue}, nil
}

func (owner *Work) Admit(ctx context.Context, request workqueue.Assignment, at time.Time) (workdispatch.Decision, error) {
	item, task := request.Item.Item, request.Task.Task
	if request.DecisionOnly || item.Operation != workexecution.BackingProvision || item.Placement.Executor != workexecution.Agent ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID || item.Input == nil {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "backing creation cannot acquire this host assignment")
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if request.Direction == workexecution.Recovery {
		if !at.Before(item.Deadline) {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		source, err := owner.records.LoadRecovery(ctx, view)
		return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline,
			Change: workqueue.Change{Conditions: source.Conditions}}, err
	}
	if request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(errs.KindStateConflict, "backing creation assignment direction is invalid")
	}
	if task.Stop != nil || item.ForwardRevoked || !at.Before(item.Deadline) {
		outcome := workexecution.Failed
		if task.Stop != nil {
			outcome = task.Stop.Outcome
		} else if item.ForwardRevoked {
			outcome = workexecution.Superseded
		}
		change, err := owner.records.PrepareQueuedStop(ctx, view, outcome, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	input, change, current, err := owner.records.ReadAdmission(ctx, view)
	clear(input.Wire)
	if err == nil && !current {
		settled, err := owner.records.PrepareQueuedStop(ctx, view, workexecution.Superseded, at)
		settled.Conditions = append(settled.Conditions, change.Conditions...)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: settled}, err
	}
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: change}, err
}

func (owner *Work) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		source, err := owner.records.LoadRecovery(ctx, view.Work)
		return workqueue.Change{Conditions: source.Conditions}, err
	}
	input, change, current, err := owner.records.ReadAdmission(ctx, view.Work)
	clear(input.Wire)
	if err == nil && !current {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation source changed before Start; submit a new action")
	}
	return change, err
}

func (owner *Work) LoadWorkAssignment(ctx context.Context, view workqueue.ResumeView) (*agentpb.BackingProvisionAssignment, error) {
	if !view.ActiveAttempt || view.Work.Item.Item.Operation != workexecution.BackingProvision ||
		workexecution.ValidateDelivery(view.Work.Item.Item, view.Attempt.Attempt) != nil {
		return nil, errs.New(errs.KindStateConflict, "backing creation delivery has no retained active assignment")
	}
	input, _, err := owner.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return nil, err
	}
	assignment := backingprovision.Assignment{Attempt: view.Attempt.Attempt, Procedure: procedure}
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		source, err := owner.records.LoadRecovery(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		assignment.Recovery = &source.Recovery
	}
	return backingprovision.EncodeAssignment(assignment)
}

func (owner *Work) Stop(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot, at time.Time) (workqueue.Change, error) {
	if task.Task.Stop == nil || item.Item.Operation != workexecution.BackingProvision || item.Item.TaskID != task.Task.ID {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation has no matching stop request")
	}
	view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
	if err != nil {
		return workqueue.Change{}, err
	}
	if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "backing creation changed before cancellation")
	}
	return owner.records.PrepareQueuedStop(ctx, view, task.Task.Stop.Outcome, at)
}

func (owner *Work) PrepareWorkRetry(ctx context.Context, task workqueue.TaskSnapshot, item workqueue.ItemSnapshot,
	previous workqueue.AttemptSnapshot, revision int64, at time.Time,
) (workqueue.Change, error) {
	if item.Item.Operation != workexecution.BackingProvision || item.Item.ActiveAttemptID != "" || task.Task.CollectionStartedAt != nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "backing creation still has active execution or expired input")
	}
	view := workqueue.View{Task: task, Item: item, ReadRevision: revision}
	input, guard, err := owner.records.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer clear(input.Wire)
	procedure, err := input.Procedure()
	if err != nil {
		return workqueue.Change{}, err
	}
	deadline := at.Add(procedure.Deadline.AsTime().Sub(procedure.CreatedAt.AsTime()))
	if item.Item.AttemptNumber != 0 && (previous.Attempt.Result == nil || previous.Attempt.FinishedAt == nil ||
		previous.Attempt.Number != item.Item.AttemptNumber || previous.Attempt.TaskID != task.Task.ID || previous.Attempt.WorkItemID != item.Item.ID) {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "backing creation Retry needs its exact previous attempt")
	}
	authority := workqueue.Change{Conditions: []keyvalue.Condition{guard}}
	var retry workqueue.Change
	if item.Item.Phase == workexecution.Attention {
		if item.Item.AttemptNumber == 0 || previous.Attempt.Result == nil {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "backing creation cleanup has no preceding attempt result")
		}
		source, sourceErr := owner.records.LoadRecovery(ctx, view)
		if sourceErr != nil {
			return workqueue.Change{}, sourceErr
		}
		authority.Conditions = append(authority.Conditions, source.Conditions...)
		retry, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
			Direction: workexecution.Recovery, Result: *previous.Attempt.Result, NotBefore: at, Deadline: deadline}, at)
	} else {
		if task.Task.Stop != nil || item.Item.ForwardRevoked || item.Item.Phase != workexecution.Settled || item.Item.Outcome != workexecution.Failed {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "backing creation requires settled failed effects before another forward attempt")
		}
		captured, current, matches, sourceErr := owner.records.ReadAdmission(ctx, view)
		clear(captured.Wire)
		if sourceErr != nil {
			return workqueue.Change{}, sourceErr
		}
		if !matches {
			return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "backing creation source changed; Retry cannot select new input")
		}
		authority.Conditions = append(authority.Conditions, current.Conditions...)
		if item.Item.AttemptNumber == 0 {
			retry, err = workqueue.PrepareUnstartedRetry(task, item, deadline, at)
		} else {
			retry, err = workqueue.PrepareRetry(task, item, previous, workexecution.RetryDecision{
				Direction: workexecution.Forward, Result: *previous.Attempt.Result, NotBefore: at, Deadline: deadline}, at)
		}
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	authority.Conditions = append(authority.Conditions, retry.Conditions...)
	authority.Mutations = append(authority.Mutations, retry.Mutations...)
	return authority, nil
}
