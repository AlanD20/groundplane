package connectors

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/connectorchecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Settle accepts only the operation's saved cleanup/result proof. Executor errors
// are diagnostic input to the journal, never standalone permission to release
// claims. Lost acknowledgements reuse the already-finished attempt.
func (executor *CheckExecutor) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	if queue == nil {
		return errs.New(errs.KindInternal, "connector check settlement requires execution storage")
	}
	attempt := delivery.Attempt.Attempt
	request, recorded, err := queue.ReadCompletion(
		ctx,
		attempt.TaskID,
		attempt.WorkItemID,
		attempt.ID,
		attempt.Executor,
	)
	if err != nil || recorded {
		return err
	}
	loaded, err := executor.repository.Load(ctx, attempt.TaskID)
	if err != nil {
		return err
	}
	defer loaded.Record.Clear()
	evidence, result, err := connectorchecks.PrepareResult(loaded, request.Task, request.Attempt.Attempt, at)
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
		return errs.New(
			errs.KindStateConflict,
			"connector check settlement changed concurrently; reload the saved attempt",
		)
	}
	return nil
}
