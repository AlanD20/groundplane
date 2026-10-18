package controllerupgrade

import (
	"context"
	"log/slog"
	"time"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/controllersoftware"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (owner *Work) Settle(ctx context.Context, queue *workqueue.Repository, delivery workqueue.Delivery, at time.Time) error {
	attempt := delivery.Attempt.Attempt
	view, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if view.Attempt.Attempt.FinishedAt != nil {
		owner.publishObservations(ctx, view.Attempt.Attempt)
		return nil
	}
	var request workqueue.Completion
	if view.CurrentExecutor {
		request, err = queue.ReadCompletionAt(ctx, view)
	} else {
		request, err = queue.ReadInterruptedControllerSoftwareAt(ctx, view)
	}
	if err != nil {
		return err
	}
	capture, err := owner.inputs.Load(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	saved, found, err := owner.inputs.ReadResult(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "Controller handoff has no saved qualification or restoration result")
	}
	proof, result, err := controllersoftware.PrepareResult(capture, saved, attempt)
	if err != nil {
		return err
	}
	if result.EffectsSettled && attempt.Direction == workexecution.Forward &&
		(saved.Record.Result.Journal == nil || saved.Record.Result.Journal.Phase == upgrade.PhaseCancelled) && view.Work.Task.Task.Stop != nil {
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
		return errs.New(errs.KindStateConflict, "Controller activation changed before settlement")
	}
	finished, err := queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err == nil {
		owner.publishObservations(ctx, finished.Attempt.Attempt)
	} else {
		slog.WarnContext(ctx, "unable to read accepted Controller result for diagnostics", "attempt_id", attempt.ID, "error", err)
	}
	return nil
}

func (owner *Work) PrepareFinish(ctx context.Context, finished workqueue.FinishedWork, _ time.Time) (workqueue.Change, error) {
	if len(finished.Items) != 1 || finished.Task.Task.Action != workexecution.ActionUpdate || finished.Task.Task.TargetID != controllersoftware.TargetID() {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Controller activation completion requires one captured work item")
	}
	item := finished.Items[0].Item
	capture, err := owner.inputs.Load(ctx, item, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.AttemptNumber == 0 {
		reference, err := capture.Record.Reference()
		if err != nil {
			return workqueue.Change{}, err
		}
		if item.Result == nil || *item.Result != reference || item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(errs.KindStateConflict, "unstarted Controller update has no matching rejection")
		}
		return captureGuard(capture), nil
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
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Controller update has no accepted terminal result")
	}
	proof, result, err := controllersoftware.PrepareResult(capture, saved, previous.Attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	accepted := item.Result
	if previous.Attempt.Direction == workexecution.Recovery {
		accepted = item.CleanupResult
	}
	if !result.EffectsSettled || *previous.Attempt.Result != result.Evidence || accepted == nil || *accepted != result.Evidence {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "Controller update has unresolved handoff effects")
	}
	return proof, nil
}
