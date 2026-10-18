package edgechecks

import (
	"context"
	"time"

	checkrecord "github.com/AlanD20/groundplane/internal/infra/etcd/edgechecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) Settle(ctx context.Context, queue *workqueue.Repository, delivery workqueue.Delivery, at time.Time) error {
	if queue == nil {
		return errs.New(errs.KindInternal, "edge check settlement requires execution storage")
	}
	attempt := delivery.Attempt.Attempt
	request, recorded, err := queue.ReadCompletion(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || recorded {
		return err
	}
	loaded, err := executor.repository.Load(ctx, attempt.TaskID)
	if err != nil {
		return err
	}
	evidence, result, err := checkrecord.PrepareResult(loaded, request.Task, request.Attempt.Attempt, at)
	if err != nil {
		return err
	}
	request.Result, request.At = result, at
	completion, err := workqueue.PrepareCompletion(request)
	if err != nil {
		return err
	}
	committed, err := queue.Commit(ctx, evidence, completion)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "edge check settlement changed concurrently; reload its recorded attempt")
	}
	return nil
}
