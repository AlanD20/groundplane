package dnsresolver

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/dnschecks"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *ConnectionCheckExecutor) Settle(
	ctx context.Context,
	queue *workqueue.Repository,
	delivery workqueue.Delivery,
	at time.Time,
) error {
	if queue == nil {
		return errs.New(errs.KindInternal, "DNS check settlement requires execution storage")
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
	loaded, err := executor.records.Load(ctx, attempt.TaskID)
	if err != nil {
		return err
	}
	proof, result, err := dnschecks.Result(loaded, request.Task, request.Attempt.Attempt)
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
		return errs.New(errs.KindStateConflict, "DNS check completion changed concurrently")
	}
	return nil
}
