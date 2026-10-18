package imagedelivery

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/imageoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (service *Service) Reconcile(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := service.loadOperation(ctx, delivery)
	if err != nil {
		return err
	}
	record, attempt := loaded.Record, delivery.Attempt.Attempt
	if attempt.StartedAt == nil || delivery.Item.Item.Phase != workexecution.Running {
		return errs.New(errs.KindStateConflict, "image reconciliation requires a started attempt")
	}
	if record.Phase == imageoperations.Finished {
		return nil
	}
	journal, err := imageoperations.NewJournal(service.operations, record.Input, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	switch record.Phase {
	case imageoperations.Ready:
		return journal.RejectForward(ctx, workexecution.Failed, time.Now().UTC())
	case imageoperations.Fetching:
		plan := *record.Input.Fetch
		tag, err := plan.TagReference()
		if err != nil {
			return err
		}
		if tag == "" {
			images, err := service.registry.List(ctx)
			if err != nil {
				return err
			}
			for _, image := range images {
				if image.ID != plan.ConfigDigest {
					continue
				}
				for _, digest := range image.Digests {
					if canonicalImageReference(digest) == plan.Reference() {
						return journal.CompleteDigestFetch(ctx, image.ID, time.Now().UTC())
					}
				}
			}
		}
		// A reconnect does not authorize finishing a tag update that never began.
		return journal.RejectForward(ctx, workexecution.Failed, time.Now().UTC())
	case imageoperations.Tagging, imageoperations.Removing, imageoperations.Uncertain:
		// Inventory alone cannot prove a timed-out Docker request has stopped.
		// Keep its fence rather than let a late write race a newer selection.
		if err := journal.RecordUnknown(ctx, time.Now().UTC()); err != nil {
			return err
		}
		return errs.New(errs.KindStateConflict, "Docker operation completion is unknown; image protection remains held")
	default:
		return errs.New(errs.KindInternal, "image operation has an unsupported reconciliation state")
	}
}

func (service *Service) Settle(ctx context.Context, queue *workqueue.Repository, delivery workqueue.Delivery, at time.Time) error {
	if queue == nil {
		return errs.New(errs.KindInternal, "image settlement requires execution storage")
	}
	attempt := delivery.Attempt.Attempt
	request, recorded, err := queue.ReadCompletion(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || recorded {
		return err
	}
	loaded, err := service.operations.Load(ctx, attempt.TaskID)
	if err != nil {
		return err
	}
	proof, result, err := imageoperations.Result(loaded, request.Task, request.Attempt.Attempt)
	if err != nil {
		return err
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
		return errs.New(errs.KindStateConflict, "image settlement changed concurrently")
	}
	return nil
}

func (service *Service) RejectUnstarted(ctx context.Context, delivery workqueue.Delivery) error {
	loaded, err := service.loadOperation(ctx, delivery)
	if err != nil {
		return err
	}
	attempt := delivery.Attempt.Attempt
	if attempt.StartedAt != nil || delivery.Item.Item.Phase != workexecution.Assigned {
		return errs.New(errs.KindStateConflict, "image operation has started; reconcile its effects")
	}
	if loaded.Record.Phase == imageoperations.Finished {
		return nil
	}
	journal, err := imageoperations.NewJournal(service.operations, loaded.Record.Input, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	outcome := workexecution.Cancelled
	if !time.Now().UTC().Before(attempt.Deadline) {
		outcome = workexecution.Failed
	}
	return journal.RejectAssigned(ctx, outcome, time.Now().UTC())
}
