package environment

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/environmentexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/controller/workdispatch"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentprovision"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type Work struct {
	records *environmentprovision.Repository
	queue   *workqueue.Repository
}

func NewWork(records *environmentprovision.Repository, queue *workqueue.Repository) (*Work, error) {
	if records == nil || queue == nil {
		return nil, errs.New(errs.KindInternal, "environment provisioning requires native execution storage")
	}
	return &Work{records: records, queue: queue}, nil
}

func (owner *Work) Admit(
	ctx context.Context,
	request workqueue.Assignment,
	at time.Time,
) (workdispatch.Decision, error) {
	task, item := request.Task.Task, request.Item.Item
	if request.DecisionOnly || item.Operation != workexecution.Environment ||
		item.Placement.Executor != workexecution.Agent ||
		request.Executor.Record.Identity.HostID != item.Placement.HostID {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning assignment belongs to another host",
		)
	}
	view := workqueue.View{Task: request.Task, Item: request.Item, ReadRevision: request.ReadRevision}
	if request.Direction == workexecution.Recovery {
		if !at.Before(item.Deadline) {
			change, err := workqueue.PrepareCleanupRetryExpiry(request.Task, request.Item, at)
			return workdispatch.Decision{Disposition: workdispatch.NeedsAttention, Change: change}, err
		}
		_, guard, err := owner.records.LoadRecovery(ctx, view)
		return workdispatch.Decision{
			Disposition: workdispatch.Dispatch,
			Deadline:    item.Deadline,
			Change:      workqueue.Change{Conditions: []keyvalue.Condition{guard}},
		}, err
	}
	if request.Direction != workexecution.Forward {
		return workdispatch.Decision{}, errs.New(
			errs.KindStateConflict,
			"environment provisioning direction is invalid",
		)
	}
	if task.Stop != nil || !at.Before(item.Deadline) || !at.Before(task.Deadline) {
		outcome := workexecution.Failed
		if task.Stop != nil {
			outcome = task.Stop.Outcome
		}
		change, err := owner.settleUnstarted(ctx, view, outcome, at)
		return workdispatch.Decision{Disposition: workdispatch.Settle, Change: change}, err
	}
	_, change, err := owner.records.ReadyForHost(ctx, view)
	return workdispatch.Decision{Disposition: workdispatch.Dispatch, Deadline: item.Deadline, Change: change}, err
}

func (owner *Work) AuthorizeAgentStart(ctx context.Context, view workqueue.ResumeView) (workqueue.Change, error) {
	if view.Attempt.Attempt.Direction == workexecution.Recovery {
		_, capture, err := owner.records.Load(ctx, view.Work)
		if err != nil {
			return workqueue.Change{}, err
		}
		_, source, err := owner.records.LoadRecovery(ctx, view.Work)
		return workqueue.Change{Conditions: []keyvalue.Condition{capture, source}}, err
	}
	_, change, err := owner.records.ReadyForHost(ctx, view.Work)
	return change, err
}

func (owner *Work) LoadWorkAssignment(
	ctx context.Context,
	view workqueue.ResumeView,
) (*agentpb.EnvironmentProvisionAssignment, error) {
	if attempt := view.Attempt.Attempt; attempt.StartedAt == nil && attempt.Direction == workexecution.Forward &&
		(view.Work.Task.Task.Stop != nil || view.Work.Item.Item.ForwardRevoked || !time.Now().UTC().Before(attempt.Deadline)) {
		if err := owner.rejectBeforeStart(ctx, view); err != nil {
			return nil, err
		}
		return nil, errs.New(
			errs.KindStateConflict,
			"environment provisioning stopped before its directory effect started",
		)
	}
	capture, _, err := owner.records.Load(ctx, view.Work)
	if err != nil {
		return nil, err
	}
	input := environmentexecution.Input{Attempt: view.Attempt.Attempt, Creation: capture.Procedure()}
	if input.Attempt.Direction == workexecution.Recovery {
		source, _, err := owner.records.LoadRecovery(ctx, view.Work)
		if err != nil {
			return nil, err
		}
		input.RecoverySource = &source
	}
	return environmentexecution.EncodeAssignment(input)
}

// Start compares the same Attempt revision, so a racing receipt either wins
// permission first or loses to this no-effect settlement. No timer proves that
// an already-started helper has stopped.
func (owner *Work) rejectBeforeStart(ctx context.Context, view workqueue.ResumeView) error {
	if view.Attempt.Attempt.StartedAt != nil {
		return errs.New(errs.KindStateConflict, "environment provisioning may already have started")
	}
	at := time.Now().UTC()
	report := environmentexecution.Result{AttemptID: view.Attempt.Attempt.ID, HelpersAbsent: true,
		Failure: "Provisioning stopped before the directory helper was authorized", ObservedAt: at}
	proof, result, err := owner.records.PrepareResult(ctx, view, report, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion, err := owner.queue.ReadCompletionAt(ctx, view)
	if err != nil {
		return err
	}
	completion.At, completion.Result = at, result
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	committed, err := owner.queue.Commit(ctx, proof, change)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "environment provisioning start raced its no-effect settlement")
	}
	return nil
}

func (owner *Work) Stop(
	ctx context.Context,
	task workqueue.TaskSnapshot,
	item workqueue.ItemSnapshot,
	at time.Time,
) (workqueue.Change, error) {
	if task.Task.Stop == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "environment provisioning has no stop request")
	}
	if item.Item.ActiveAttemptID == "" && item.Item.AttemptNumber == 0 {
		view, err := owner.queue.ReadWork(ctx, task.Task.ID, item.Item.ID, 0)
		if err != nil {
			return workqueue.Change{}, err
		}
		if view.Task.Revision != task.Revision || view.Item.Revision != item.Revision {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"environment provisioning changed before cancellation",
			)
		}
		return owner.settleUnstarted(ctx, view, task.Task.Stop.Outcome, at)
	}
	return workqueue.PrepareRevocation(task, item, task.Task.Stop.Outcome, at)
}

func (owner *Work) settleUnstarted(
	ctx context.Context,
	view workqueue.View,
	outcome workexecution.Outcome,
	at time.Time,
) (workqueue.Change, error) {
	input, _, err := owner.records.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	ref, err := input.Reference()
	if err != nil {
		return workqueue.Change{}, err
	}
	state, err := owner.records.PrepareResourceResult(ctx, view, false)
	if err != nil {
		return workqueue.Change{}, err
	}
	completion := workexecution.Completion{Outcome: outcome, Evidence: ref, EffectsSettled: true}
	var settled workqueue.Change
	if view.Item.Item.AttemptNumber == 0 {
		settled, err = workqueue.PrepareUnstartedSettlement(view.Task, view.Item, completion, at)
	} else {
		_, previous, readErr := owner.queue.ReadAttempt(ctx, view.Task.Task.ID, view.Item.Item.ID, view.Item.Item.AttemptNumber, view.ReadRevision)
		if readErr != nil {
			keyvalue.ClearMutationValues(state.Mutations)
			return workqueue.Change{}, readErr
		}
		if previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
			keyvalue.ClearMutationValues(state.Mutations)
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "queued environment Retry has no accepted preceding result")
		}
		completion.Evidence = *previous.Attempt.Result
		settled, err = workqueue.PrepareQueuedRetrySettlement(view.Task, view.Item, completion, at)
		state.Conditions = append(state.Conditions, keyvalue.Condition{Key: workqueue.AttemptKey(previous.Attempt.ID), ModRevision: previous.Revision})
	}
	if err != nil {
		keyvalue.ClearMutationValues(state.Mutations)
		return workqueue.Change{}, err
	}
	state.Conditions = append(state.Conditions, settled.Conditions...)
	state.Mutations = append(state.Mutations, settled.Mutations...)
	return state, nil
}

func (owner *Work) ReconcileInterrupted(
	ctx context.Context,
	view workqueue.ResumeView,
	authorization agentregistration.WorkAuthorization,
) error {
	guards, err := authorization.Conditions()
	if err != nil {
		return err
	}
	current, previous := authorization.Identity(), view.Attempt.Attempt
	if current.Executor != workexecution.Agent || current.HostID != previous.Executor.HostID ||
		current == previous.Executor {
		return errs.New(
			errs.KindStateConflict,
			"environment interruption requires the replacement Agent on the captured host",
		)
	}
	completion, err := owner.queue.ReadInterruptedEnvironmentAt(ctx, view)
	if err != nil {
		return err
	}
	at := time.Now().UTC()
	report := environmentexecution.Result{
		AttemptID:     previous.ID,
		HelpersAbsent: previous.Direction == workexecution.Forward && previous.StartedAt == nil,
		Failure:       "Agent execution was interrupted before the directory result was recorded",
		ObservedAt:    at,
	}
	if !report.HelpersAbsent {
		report.CleanupFailure = "The original helper and directory need inspection on the captured host"
	}
	proof, result, err := owner.records.PrepareResult(ctx, view, report, at)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(proof.Mutations)
	completion.Result, completion.At = result, at
	change, err := workqueue.PrepareCompletion(completion)
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	committed, err := owner.queue.Commit(ctx, workqueue.Change{Conditions: guards}, proof, change)
	keyvalue.ClearValues(committed.FailureReads)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "environment interruption ownership changed before settlement")
	}
	return nil
}
