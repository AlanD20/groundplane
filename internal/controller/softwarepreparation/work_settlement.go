package softwarepreparation

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	preparationrecord "github.com/AlanD20/groundplane/internal/infra/etcd/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Settle(
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
		request, err = queue.ReadInterruptedSoftwarePreparationAt(ctx, view)
	}
	if err != nil {
		return err
	}
	snapshot, err := service.records.LoadWork(ctx, view.Work.Item.Item, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	saved, found, err := service.records.ReadOutcome(ctx, attempt, view.Work.ReadRevision)
	if err != nil {
		return err
	}
	if !found {
		return errs.New(errs.KindStateConflict, "software preparation has no saved execution outcome")
	}
	proof, result, err := preparationrecord.PrepareResult(snapshot, saved, attempt)
	if err != nil {
		return err
	}
	if result.EffectsSettled && attempt.Direction == workexecution.Forward {
		if stop := view.Work.Task.Task.Stop; stop != nil {
			result.Outcome = stop.Outcome
		} else if view.Work.Item.Item.ForwardRevoked {
			result.Outcome = workexecution.Superseded
		}
	}
	request.Result, request.At = result, at
	change, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	committed, err := queue.Commit(ctx, proof, change)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "software preparation changed before execution settlement")
	}
	return nil
}

func (service *Service) PrepareFinish(
	ctx context.Context,
	finished workqueue.FinishedWork,
	_ time.Time,
) (workqueue.Change, error) {
	if len(finished.Items) != 1 || finished.Task.Task.Action != workexecution.ActionPrepare {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software preparation completion requires one captured operation",
		)
	}
	item := finished.Items[0].Item
	snapshot, err := service.records.LoadWork(ctx, item, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if item.AttemptNumber == 0 {
		reference, err := snapshot.Capture.Reference()
		if err != nil {
			return workqueue.Change{}, err
		}
		if item.Result == nil || *item.Result != reference || item.Outcome == workexecution.Succeeded {
			return workqueue.Change{}, errs.New(
				errs.KindStateConflict,
				"unstarted preparation has no matching rejection",
			)
		}
		return snapshot.Guard(), nil
	}
	_, previous, err := service.queue.ReadAttempt(ctx, item.TaskID, item.ID, item.AttemptNumber, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	saved, found, err := service.records.ReadOutcome(ctx, previous.Attempt, finished.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !found || previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software preparation completion has no accepted outcome",
		)
	}
	proof, result, err := preparationrecord.PrepareResult(snapshot, saved, previous.Attempt)
	if err != nil {
		return workqueue.Change{}, err
	}
	accepted := item.Result
	if previous.Attempt.Direction == workexecution.Recovery {
		accepted = item.CleanupResult
	}
	if !result.EffectsSettled || *previous.Attempt.Result != result.Evidence || accepted == nil ||
		*accepted != result.Evidence {
		return workqueue.Change{}, errs.New(
			errs.KindStateConflict,
			"software preparation still has unresolved build resources",
		)
	}
	return proof, nil
}
