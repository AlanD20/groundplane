package runner

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (executor *Executor) Settle(ctx context.Context, queue *workqueue.Repository,
	delivery workqueue.Delivery, at time.Time,
) error {
	if queue == nil {
		return errs.New(errs.KindInternal, "Runner settlement requires execution storage")
	}
	attempt := delivery.Attempt.Attempt
	request, recorded, err := queue.ReadCompletion(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil || recorded {
		return err
	}
	input, err := executor.inputs.Load(ctx, attempt.WorkItemID, 0)
	if err != nil {
		return err
	}
	result, err := executor.inputs.LoadResult(ctx, attempt, input.ReadRevision)
	if err != nil {
		return err
	}
	completion, err := result.Record.Completion(input.Record)
	if err != nil {
		return err
	}
	request.Result, request.At = completion, at
	var settled workqueue.Change
	if attempt.Direction == workexecution.Forward && !completion.EffectsSettled {
		settled, err = workqueue.PrepareCompletionWithRecovery(request, at.Add(5*time.Minute))
	} else {
		settled, err = workqueue.PrepareCompletion(request)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(settled.Mutations)
	var resource workqueue.Change
	if input.Record.Operation == runnerallocation.RuntimeOperationRemove {
		resource, err = executor.repository.PrepareRunnerRemovalResult(ctx, input, result)
	} else {
		resource, err = executor.repository.PrepareRunnerCreationResult(ctx, input, result)
	}
	if err != nil {
		return err
	}
	defer keyvalue.ClearMutationValues(resource.Mutations)
	proof := workqueue.Change{Conditions: []keyvalue.Condition{{Key: runnerwork.ResultKey(attempt.ID), ModRevision: result.Revision}}}
	committed, err := queue.Commit(ctx, settled, resource, proof)
	if err != nil {
		return err
	}
	if !committed.Succeeded {
		return errs.New(errs.KindStateConflict, "Runner completion changed concurrently")
	}
	executor.broker.Drop(TokenKey{TaskID: attempt.TaskID, WorkItemID: attempt.WorkItemID, Attempt: attempt.Number})
	return nil
}
