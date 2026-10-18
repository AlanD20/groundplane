package workdispatch

import (
	"context"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/controllerownership"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReconcileStartup makes one bounded pass over previous process assignments.
// It does not replay forward commands, transfer their identities, renew their
// deadlines, or turn diagnostic errors into settlement evidence. The caller
// retains ownership through Run and shutdown. Failed effects keep their claims.
func (runtime *Runtime) ReconcileStartup(ctx context.Context, ownership *controllerownership.Lease,
	identity workexecution.Identity,
) ([]Issue, error) {
	if ctx == nil || identity.Validate() != nil || identity.Executor != workexecution.Controller {
		return nil, errs.New(errs.KindInternal, "startup reconciliation requires a Controller identity")
	}
	if err := ownership.Check(); err != nil {
		return nil, err
	}
	deliveries, err := runtime.queue.InterruptedDeliveries(ctx, identity)
	if err != nil {
		return nil, err
	}
	// A missing owner is an incomplete deployment, not an operation failure that
	// may be ignored while enabling the runtime.
	for _, delivery := range deliveries {
		if runtime.controller.owners[delivery.Item.Item.Operation] == nil {
			return nil, errs.New(errs.KindStateConflict, "interrupted work has no registered operation owner")
		}
	}
	issues := make([]Issue, len(deliveries))
	jobs := make(chan int)
	var workers sync.WaitGroup
	for index := 0; index < min(4, len(deliveries)); index++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for index := range jobs {
				delivery := deliveries[index]
				if err := runtime.controller.reconcileInterrupted(ctx, delivery); err != nil {
					issues[index] = Issue{TaskID: delivery.Item.Item.TaskID, WorkID: delivery.Item.Item.ID, Err: err}
				}
			}
		}()
	}
	for index := range deliveries {
		select {
		case jobs <- index:
		case <-ctx.Done():
			close(jobs)
			workers.Wait()
			return nil, ctx.Err()
		}
	}
	close(jobs)
	workers.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	retained := issues[:0]
	for _, issue := range issues {
		if issue.Err != nil {
			retained = append(retained, issue)
		}
	}
	if err := ownership.Check(); err != nil {
		return retained, err
	}
	current, err := runtime.queue.ReadExecutor(ctx, workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID})
	if err != nil {
		return retained, err
	}
	if current.Revision == 0 || current.Record.Identity != identity || current.Record.Ready {
		return retained, errs.New(errs.KindStateConflict, "startup executor registration changed")
	}
	change, err := workqueue.PrepareAvailability(current, identity, true, current.Record.Capacity, nil)
	if err != nil {
		return retained, err
	}
	defer keyvalue.ClearMutationValues(change.Mutations)
	result, err := runtime.queue.Commit(ctx, change)
	if err != nil {
		return retained, err
	}
	if !result.Succeeded {
		return retained, errs.New(errs.KindStateConflict, "startup executor readiness changed concurrently")
	}
	return retained, nil
}

func (executor *ControllerExecutor) reconcileInterrupted(ctx context.Context, delivery workqueue.Delivery) error {
	attempt := delivery.Attempt.Attempt
	bounded, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	view, err := executor.queue.ReadResume(bounded, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if view.Attempt.Attempt.FinishedAt != nil {
		return nil
	}
	if view.CurrentExecutor || !view.ActiveAttempt {
		return errs.New(errs.KindStateConflict, "interrupted attempt no longer has its captured ownership")
	}
	delivery.Item, delivery.Attempt = view.Work.Item, view.Attempt
	owner := executor.owners[delivery.Item.Item.Operation]
	if owner == nil {
		return errs.New(errs.KindInternal, "interrupted operation owner is missing")
	}
	// Rejection before the durable start receipt cannot execute the command. A
	// started attempt must inspect its operation journal and external effects.
	observing, finishObservation := context.WithTimeout(bounded, 15*time.Second)
	var operationErr error
	if delivery.Attempt.Attempt.StartedAt == nil {
		operationErr = owner.RejectUnstarted(observing, delivery)
	} else {
		operationErr = owner.Reconcile(observing, delivery)
	}
	finishObservation()
	settleErr := owner.Settle(bounded, executor.queue, delivery, time.Now().UTC())
	if operationErr != nil && settleErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, operationErr, settleErr)
	}
	if settleErr != nil {
		return settleErr
	}
	return operationErr
}
