package workdispatch

import (
	"context"
	"errors"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetryOwner classifies the preceding captured result and admits the operation's
// permitted forward or cleanup retry. Failure alone is not retry authority.
type RetryOwner interface {
	PrepareWorkRetry(context.Context, workqueue.TaskSnapshot, workqueue.ItemSnapshot,
		workqueue.AttemptSnapshot, int64, time.Time) (workqueue.Change, error)
}

type RetryRegistration struct {
	Operation workexecution.Operation
	Owner     RetryOwner
}

type Retries struct {
	queue  *workqueue.Repository
	owners map[workexecution.Operation]RetryOwner
}

func NewRetries(queue *workqueue.Repository, registrations []RetryRegistration) (*Retries, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "work retry requires storage and operation owners")
	}
	owners := make(map[workexecution.Operation]RetryOwner, len(registrations))
	for _, registration := range registrations {
		if !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil || owners[registration.Operation] != nil {
			return nil, errs.New(errs.KindInternal, "work retry operation registration is invalid or duplicated")
		}
		owners[registration.Operation] = registration.Owner
	}
	return &Retries{queue: queue, owners: owners}, nil
}

// Prepare preserves the public Task and exact work identity. The operator action
// coordinator must authorize scope and join its request replay record before
// committing this change. It must not automatically retry a failed CAS: that
// would reinterpret the operator request against a newer attempt.
func (retries *Retries) Prepare(ctx context.Context, taskID, workID string,
	expectedAttempt uint64, queuedFailureAt *time.Time, at time.Time,
) (workqueue.Change, error) {
	if expectedAttempt == 0 && (queuedFailureAt == nil || queuedFailureAt.IsZero()) || expectedAttempt != 0 && queuedFailureAt != nil {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "Retry of unexecuted work requires its exact failure time")
	}
	return retries.prepareAt(ctx, taskID, workID, expectedAttempt, queuedFailureAt, 0, at)
}

// Availability evaluates the same read-only preparation as admission at the
// inspection revision. It grants no authority; submission rechecks current state.
func (retries *Retries) Availability(ctx context.Context, taskID, workID string, expectedAttempt uint64, revision int64, at time.Time) (bool, string, error) {
	if revision <= 0 {
		return false, "", errs.New(errs.KindValidationFailed, "retry availability requires a snapshot revision")
	}
	change, err := retries.prepareAt(ctx, taskID, workID, expectedAttempt, nil, revision, at)
	defer keyvalue.ClearMutationValues(change.Mutations)
	if err == nil {
		return true, "", nil
	}
	if kind, ok := errs.KindOf(err); ok && kind == errs.KindTaskNotRetryable {
		var domain *errs.Error
		if errors.As(err, &domain) {
			return false, domain.ToProblem().Detail, nil
		}
	}
	return false, "", err
}

func (retries *Retries) prepareAt(ctx context.Context, taskID, workID string, expectedAttempt uint64, queuedFailureAt *time.Time, revision int64, at time.Time) (workqueue.Change, error) {
	if at.IsZero() {
		return workqueue.Change{}, errs.New(errs.KindValidationFailed, "work retry requires an identified attempt and timestamp")
	}
	var view workqueue.View
	var previous workqueue.AttemptSnapshot
	var err error
	if expectedAttempt == 0 {
		view, err = retries.queue.ReadWork(ctx, taskID, workID, revision)
	} else {
		view, previous, err = retries.queue.ReadAttempt(ctx, taskID, workID, expectedAttempt, revision)
	}
	if err != nil {
		return workqueue.Change{}, err
	}
	item := view.Item.Item
	if queuedFailureAt != nil && !queuedFailureAt.Equal(item.UpdatedAt) {
		return workqueue.Change{}, errs.New(errs.KindStateConflict, "queued work changed since this failure was selected; refresh Task details")
	}
	cleanup := item.Phase == workexecution.Attention && (item.Cleanup == workexecution.CleanupRequired || item.Cleanup == workexecution.CleanupStopped)
	forward := item.Phase == workexecution.Settled && item.Outcome == workexecution.Failed && !item.ForwardRevoked &&
		(item.Cleanup == workexecution.CleanupNone || item.Cleanup == workexecution.CleanupComplete)
	if view.Task.Task.CollectionStartedAt != nil || item.AttemptNumber != expectedAttempt || item.ActiveAttemptID != "" ||
		expectedAttempt != 0 && (previous.Attempt.FinishedAt == nil || previous.Attempt.Result == nil) || !cleanup && !forward {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "this attempt has no settled failure or stopped cleanup to retry")
	}
	owner := retries.owners[item.Operation]
	if owner == nil {
		return workqueue.Change{}, errs.New(errs.KindTaskNotRetryable, "this operation does not permit Retry")
	}
	return owner.PrepareWorkRetry(ctx, view.Task, view.Item, previous, view.ReadRevision, at.UTC())
}
