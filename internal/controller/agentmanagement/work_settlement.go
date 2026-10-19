package agentmanagement

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	attempt := delivery.Attempt.Attempt
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || view.Attempt.Attempt.FinishedAt != nil {
		return err
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedAgentManagementAt(ctx, view)
	}
	if err != nil {
		return err
	}
	input, err := owner.inputs.Load(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	saved, found, err := owner.inputs.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Agent lifecycle has no saved readiness or cleanup proof")
	}
	proof, result, err := agentoperations.PrepareResult(input, saved, attempt)
	if err != nil {
		return err
	}
	if input.Record.Action == workexecution.ActionUpdate && result.EffectsSettled &&
		saved.Record.Result.Observed != agentoperations.NotStarted {
		reopen, err := owner.registrations.PrepareResumeReplacement(
			ctx,
			input.Record.AgentID,
			input.Record.StartingGeneration,
			saved.Record.Result.Generation,
			view.Work.ReadRevision,
		)
		if err != nil {
			return err
		}
		proof.Conditions = append(proof.Conditions, reopen.Conditions...)
		proof.Mutations = append(proof.Mutations, reopen.Mutations...)
	}
	if result.EffectsSettled && attempt.Direction == workexecution.Forward && view.Work.Task.Task.Stop != nil {
		result.Outcome = view.Work.Task.Task.Stop.Outcome
	}
	request.Result, request.At = result, at
	completion, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	committed, err := queue.Commit(ctx, proof, completion)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Agent lifecycle changed before settlement")
	}
	return nil
}

func (owner *Work) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if len(finished.Items) != 1 {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Agent lifecycle completion requires one captured work item",
		)
	}
	item := finished.Items[0].Item
	input, err := owner.inputs.Load(ctx, item, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.Record.Action != finished.Task.Task.Action || input.Record.AgentID != finished.Task.Task.TargetID {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Agent lifecycle completion changed its requested action",
		)
	}
	if item.AttemptNumber == 0 {
		reference, err := input.Record.Reference()
		if err != nil {
			return workqueue.Change{}, err
		}
		if item.Result == nil || *item.Result != reference || item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"unstarted Agent lifecycle has no captured rejection",
			)
		}
		return inputGuard(input), nil
	}
	_, previous, err := owner.queue.ReadAttempt(ctx, item.TaskID, item.ID, item.AttemptNumber, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	saved, found, err := owner.inputs.ReadResult(ctx, previous.Attempt, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Agent lifecycle has no accepted terminal proof")
	}
	proof, result, err := agentoperations.PrepareResult(input, saved, previous.Attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	accepted := item.Result
	if previous.Attempt.Direction == workexecution.Recovery {
		accepted = item.CleanupResult
	}
	if !result.EffectsSettled || accepted == nil || *accepted != result.Evidence ||
		*previous.Attempt.Result != result.Evidence {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Agent lifecycle retains unresolved effects")
	}
	return proof, nil
}

func (owner *Work) RetireWork(
	ctx context.Context,
	view workqueue.View,
	at time.Time,
	final bool,
) (bool, workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.CollectionStartedAt == nil || task.FinishedAt == nil || task.RetainUntil == nil ||
		at.Before(*task.RetainUntil) ||
		item.Phase != workexecution.Settled ||
		item.ActiveAttemptID != "" ||
		item.Result == nil ||
		item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
		return false, workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Agent history collection requires expired settled work",
		)
	}
	input, err := owner.inputs.Load(ctx, item, view.ReadRevision)
	if err != nil {
		return false, workqueue.Change{}, err
	}
	change := inputGuard(input)
	if final {
		change.Mutations = append(
			change.Mutations,
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: agentoperations.InputKey(item.ID)},
		)
	}
	// Installed credentials, runtime files and the current Agent record are
	// resource-owned and never deleted by diagnostic-history collection.
	return true, change, nil
}

func (owner *Work) PrepareAttemptRetirement(
	view workqueue.View,
	snapshot workqueue.AttemptSnapshot,
) (workqueue.Change, error) {
	attempt := snapshot.Attempt
	if view.Item.Item.Operation != workexecution.AgentLifecycle &&
		view.Item.Item.Operation != workexecution.AgentSoftware ||
		view.Item.Item.Input == nil ||
		attempt.Input != *view.Item.Item.Input ||
		attempt.Executor.Executor != workexecution.Controller {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"Agent attempt history differs from its captured lifecycle",
		)
	}
	prepared, err := workqueue.PrepareAttemptCollection(view.Task, view.Item, snapshot)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{
		Conditions: prepared.Conditions,
		Mutations:  []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: agentoperations.ResultKey(attempt.ID)}},
	}, nil
}
