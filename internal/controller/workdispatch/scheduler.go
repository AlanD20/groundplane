// Package workdispatch schedules typed resource work. It does not interpret
// operation payloads, execute host commands or infer recovery from log output.
package workdispatch

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Admission belongs to the operation owner. It compares the captured input and
// current resource authority before assignment. It may settle obsolete/expired
// unstarted work instead. Neither this call nor a successful read grants effects.
type Admission interface {
	Admit(context.Context, workqueue.Assignment, time.Time) (Decision, error)
	Stop(context.Context, workqueue.TaskSnapshot, workqueue.ItemSnapshot, time.Time) (workqueue.Change, error)
}

type Disposition uint8

const (
	Dispatch Disposition = iota + 1
	Wait
	Settle
	NeedsAttention
	// Advance commits operation-owned Controller preparation while leaving work
	// queued. It may prepare Controller or Agent work but grants no host effects.
	Advance
)

type Decision struct {
	Disposition Disposition
	Change      workqueue.Change
	Deadline    time.Time
	Wait        *workexecution.Wait
}

type Registration struct {
	Operation    workexecution.Operation
	Executor     workexecution.Executor
	Owner        Admission
	DecisionOnly bool
}

type admissionKey struct {
	operation workexecution.Operation
	executor  workexecution.Executor
}

type Scheduler struct {
	queue     *workqueue.Repository
	owners    map[admissionKey]Admission
	decisions []workexecution.Operation
}

func NewScheduler(queue *workqueue.Repository, registrations []Registration) (*Scheduler, error) {
	if queue == nil || len(registrations) == 0 {
		return nil, errs.New(errs.KindInternal, "work scheduler requires storage and operation owners")
	}
	owners := make(map[admissionKey]Admission, len(registrations))
	var decisions []workexecution.Operation
	for _, registration := range registrations {
		if !workexecution.ValidOperation(registration.Operation) || registration.Owner == nil ||
			registration.Executor != workexecution.Controller && registration.Executor != workexecution.Agent ||
			registration.DecisionOnly && registration.Executor != workexecution.Controller {
			return nil, errs.New(errs.KindInternal, "work scheduler registration is invalid")
		}
		key := admissionKey{registration.Operation, registration.Executor}
		if _, exists := owners[key]; exists {
			return nil, errs.New(errs.KindInternal, "work operation has more than one owner")
		}
		owners[key] = registration.Owner
		if registration.DecisionOnly {
			decisions = append(decisions, registration.Operation)
		}
	}
	return &Scheduler{queue: queue, owners: owners, decisions: decisions}, nil
}

type Deferred struct {
	TaskID string
	WorkID string
	Reason workexecution.Wait
}

func (scheduler *Scheduler) owner(item workexecution.Item) Admission {
	return scheduler.owners[admissionKey{item.Operation, item.Placement.Executor}]
}

// Issue is private diagnostic context. Transport adapters must use the standard
// public error mapper rather than expose wrapped provider errors to operators.
type Issue struct {
	TaskID string
	WorkID string
	Err    error
}

type Sweep struct {
	Assignments []workexecution.Attempt
	Deferred    []Deferred
	Issues      []Issue
	Settled     []string
	Attention   []string
	Advanced    []string
	Next        string
}

// Schedule scans at most one bounded page. Next must be carried to the following
// sweep; an empty cursor wraps to the oldest item. Blocked or failing work cannot
// keep every later item behind it. Concurrent schedulers are serialized by the
// assignment transaction, not by an in-process global lock.
func (scheduler *Scheduler) Schedule(
	ctx context.Context,
	identity workexecution.Identity,
	after string,
	at time.Time,
) (Sweep, error) {
	if ctx == nil || at.IsZero() {
		return Sweep{}, errs.New(errs.KindInternal, "work scheduling requires context and time")
	}
	if err := ctx.Err(); err != nil {
		return Sweep{}, err
	}
	if err := identity.Validate(); err != nil {
		return Sweep{}, err
	}
	page, err := scheduler.queue.ScanReady(
		ctx,
		workexecution.Placement{Executor: identity.Executor, HostID: identity.HostID},
		after,
	)
	if err != nil {
		return Sweep{}, err
	}
	var result Sweep
	if page.More {
		result.Next = page.LastID
	}
	for _, entry := range page.Entries {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		candidate, err := scheduler.queue.ReadCandidate(ctx, entry, identity, at, scheduler.decisions)
		if err != nil {
			result.issue(entry, err)
			continue
		}
		if candidate.Wait != nil {
			result.deferWork(entry, *candidate.Wait)
			continue
		}
		if candidate.Stopped {
			owner := scheduler.owner(candidate.Assignment.Item.Item)
			if owner == nil {
				result.issue(entry, errs.New(errs.KindInternal, "stopped work has no registered operation owner"))
				continue
			}
			change, err := owner.Stop(ctx, candidate.Assignment.Task, candidate.Assignment.Item, at)
			if err != nil {
				result.issue(entry, err)
				continue
			}
			committed, err := scheduler.queue.Commit(ctx, change)
			if err != nil {
				result.issue(entry, err)
			} else if committed.Succeeded {
				result.Settled = append(result.Settled, entry.WorkItemID)
			}
			continue
		}
		if candidate.RetryDue {
			change, err := workqueue.PrepareRetryReady(candidate.Assignment.Task, candidate.Assignment.Item,
				candidate.Assignment.QueueRevision, at)
			if err != nil {
				result.issue(entry, err)
				continue
			}
			if _, err := scheduler.queue.Commit(ctx, change); err != nil {
				result.issue(entry, err)
			}
			// Assignment is a separate admission on the following sweep. Waking
			// a retry does not acquire a slot or bypass operation authorization.
			continue
		}
		owner := scheduler.owner(candidate.Assignment.Item.Item)
		if owner == nil {
			result.issue(entry, errs.New(errs.KindInternal, "queued operation has no registered owner"))
			continue
		}
		if !candidate.Assignment.DecisionOnly {
			// Admission may inspect the exact prospective recovery attempt. No
			// execution is authorized until its assignment commits below.
			candidate.Assignment.AttemptID = ids.New(ids.KindAttempt)
		}
		decision, err := owner.Admit(ctx, candidate.Assignment, at)
		defer keyvalue.ClearMutationValues(decision.Change.Mutations)
		if err != nil {
			result.issue(entry, err)
			continue
		}
		if err := decision.validate(at, candidate.Assignment.Item.Item.Deadline); err != nil {
			result.issue(entry, err)
			continue
		}
		if decision.Disposition == Wait {
			result.deferWork(entry, *decision.Wait)
			continue
		}
		if decision.Disposition == Settle || decision.Disposition == NeedsAttention || decision.Disposition == Advance {
			fence := workqueue.Change{}
			if candidate.Assignment.DecisionOnly && decision.Disposition != Advance {
				if !candidate.Assignment.Item.Item.Deadline.After(at) {
					fence, err = workqueue.PrepareExpiryFence(candidate.Assignment, at)
				} else {
					fence, err = workqueue.PrepareDecisionFence(candidate.Assignment)
				}
				if err != nil {
					result.issue(entry, err)
					continue
				}
			}
			committed, err := scheduler.queue.Commit(ctx, decision.Change, fence)
			if err != nil {
				result.issue(entry, err)
			} else if committed.Succeeded {
				if decision.Disposition == Advance {
					result.Advanced = append(result.Advanced, entry.WorkItemID)
				} else if decision.Disposition == NeedsAttention {
					result.Attention = append(result.Attention, entry.WorkItemID)
				} else {
					result.Settled = append(result.Settled, entry.WorkItemID)
				}
			}
			continue
		}
		request := candidate.Assignment
		if request.DecisionOnly {
			result.issue(entry, errs.New(errs.KindInternal, "Controller decision cannot dispatch an execution attempt"))
			continue
		}
		request.At, request.Deadline = at, decision.Deadline
		change, attempt, err := workqueue.PrepareAssignment(request)
		if err != nil {
			result.issue(entry, err)
			continue
		}
		committed, err := scheduler.queue.Commit(ctx, change, decision.Change)
		if err != nil {
			// An ambiguous transaction is not retried in this sweep. Durable
			// assignments, not this return value, drive executor delivery.
			result.issue(entry, err)
		} else if committed.Succeeded {
			result.Assignments = append(result.Assignments, attempt)
		}
	}
	return result, nil
}

func (decision Decision) validate(at, deadline time.Time) error {
	invalid := func() error { return errs.New(errs.KindInternal, "operation admission returned an invalid decision") }
	switch decision.Disposition {
	case Dispatch:
		if decision.Wait != nil || !decision.Deadline.After(at) || decision.Deadline.After(deadline) ||
			len(decision.Change.Conditions) == 0 {
			return invalid()
		}
	case Wait:
		if decision.Wait == nil || !decision.Deadline.IsZero() || len(decision.Change.Conditions) != 0 ||
			len(decision.Change.Mutations) != 0 {
			return invalid()
		}
		if err := decision.Wait.Validate(); err != nil {
			return invalid()
		}
	case Settle, NeedsAttention, Advance:
		if decision.Wait != nil || !decision.Deadline.IsZero() || len(decision.Change.Conditions) == 0 ||
			len(decision.Change.Mutations) == 0 {
			return invalid()
		}
	default:
		return invalid()
	}
	return nil
}

func (sweep *Sweep) issue(entry workqueue.QueueEntry, err error) {
	sweep.Issues = append(sweep.Issues, Issue{TaskID: entry.TaskID, WorkID: entry.WorkItemID, Err: err})
}

func (sweep *Sweep) deferWork(entry workqueue.QueueEntry, reason workexecution.Wait) {
	sweep.Deferred = append(sweep.Deferred, Deferred{TaskID: entry.TaskID, WorkID: entry.WorkItemID, Reason: reason})
}
