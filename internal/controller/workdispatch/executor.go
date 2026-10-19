package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ControllerOperation separates a fresh start from reconciliation. A delivery
// with an existing start receipt must never silently rerun its forward command.
// Reconcile inspects operation-owned evidence and accounts for unknown effects.
type ControllerOperation interface {
	AuthorizeStart(context.Context, workqueue.Delivery) (workqueue.Change, error)
	RejectUnstarted(context.Context, workqueue.Delivery) error
	Execute(context.Context, workqueue.Delivery) error
	Reconcile(context.Context, workqueue.Delivery) error
	Settle(context.Context, *workqueue.Repository, workqueue.Delivery, time.Time) error
}

type ControllerRegistration struct {
	Operation workexecution.Operation
	Owner     ControllerOperation
}

type ControllerExecutor struct {
	queue  *workqueue.Repository
	owners map[workexecution.Operation]ControllerOperation
}

func NewControllerExecutor(
	queue *workqueue.Repository,
	registrations []ControllerRegistration,
) (*ControllerExecutor, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "controller execution requires storage and operation owners")
	}
	owners := make(map[workexecution.Operation]ControllerOperation, len(registrations))
	for _, registration := range registrations {
		if !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil ||
			owners[registration.Operation] != nil {
			return nil, errs.New(errs.KindInternal, "controller operation registration is invalid or duplicated")
		}
		owners[registration.Operation] = registration.Owner
	}
	return &ControllerExecutor{queue: queue, owners: owners}, nil
}

// execute is called by the single local delivery loop, serially per attempt.
// Durable start CAS handles competing delivery; the loop prevents reconciling a
// command still running in this process. A failed/ambiguous start never executes.
func (executor *ControllerExecutor) execute(ctx context.Context, delivery workqueue.Delivery) error {
	attempt := delivery.Attempt.Attempt
	if attempt.Executor.Executor != workexecution.Controller {
		return errs.New(errs.KindInternal, "controller cannot execute Agent work")
	}
	owner := executor.owners[delivery.Item.Item.Operation]
	if owner == nil {
		return errs.New(errs.KindInternal, "controller work has no operation owner")
	}
	view, err := executor.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
	if err != nil {
		return err
	}
	if view.Attempt.Attempt.FinishedAt != nil {
		return nil
	}
	if !view.ActiveAttempt || !view.CurrentExecutor {
		return errs.New(errs.KindStateConflict, "controller attempt no longer belongs to this process")
	}
	delivery.Item, delivery.Attempt = view.Work.Item, view.Attempt
	fresh := view.Attempt.Attempt.StartedAt == nil
	if fresh && (view.Work.Task.Task.Stop != nil && attempt.Direction == workexecution.Forward ||
		view.Work.Item.Item.ForwardRevoked && attempt.Direction == workexecution.Forward || !time.Now().UTC().Before(attempt.Deadline)) {
		if err := owner.RejectUnstarted(ctx, delivery); err != nil {
			return err
		}
		return owner.Settle(ctx, executor.queue, delivery, time.Now().UTC())
	}
	if fresh {
		operation, err := owner.AuthorizeStart(ctx, delivery)
		if err != nil {
			return err
		}
		if len(operation.Conditions) == 0 {
			return errs.New(errs.KindInternal, "operation start has no captured-input comparison")
		}
		registration, err := executor.queue.ReadExecutor(ctx, delivery.Item.Item.Placement)
		if err != nil {
			return err
		}
		start, err := workqueue.PrepareStart(
			view.Work.Task,
			view.Work.Item,
			view.Attempt,
			registration,
			time.Now().UTC(),
		)
		if err != nil {
			return err
		}
		committed, err := executor.queue.Commit(ctx, start, operation)
		if err != nil {
			return err
		}
		if !committed.Succeeded {
			return errs.New(errs.KindStateConflict, "execution start changed concurrently")
		}
		started, err := executor.queue.ReadResume(ctx, attempt.TaskID, attempt.WorkItemID, attempt.ID, attempt.Executor)
		if err != nil {
			return err
		}
		if !started.ActiveAttempt || !started.CurrentExecutor || started.Attempt.Attempt.StartedAt == nil {
			return errs.New(errs.KindStateConflict, "execution start is no longer active")
		}
		delivery.Item, delivery.Attempt = started.Work.Item, started.Attempt
	}
	var executionErr error
	if fresh {
		running, cancel := context.WithDeadline(ctx, delivery.Attempt.Attempt.Deadline)
		executionErr = owner.Execute(running, delivery)
		cancel()
	} else {
		// The execution deadline forbids new forward work; it must not prevent
		// inspection of effects or recording interruption after that deadline.
		// Reconcile may inspect/clean up, never repeat the forward command.
		reconciling, cancel := context.WithTimeout(ctx, 15*time.Second)
		executionErr = owner.Reconcile(reconciling, delivery)
		cancel()
	}
	// A command error cannot release claims. Only operation-owned durable proof
	// can settle the attempt, even when its deadline or shutdown cancelled work.
	settlement, finish := context.WithTimeout(context.WithoutCancel(ctx), 15*time.Second)
	defer finish()
	settleErr := owner.Settle(settlement, executor.queue, delivery, time.Now().UTC())
	if executionErr != nil && settleErr != nil {
		return errs.WrapJoined(errs.KindStateConflict, executionErr, settleErr)
	}
	if settleErr != nil {
		return settleErr
	}
	return executionErr
}
