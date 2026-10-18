package workexecution

import (
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (attempt Attempt) Validate() error {
	if ids.Validate(ids.KindAttempt, attempt.ID) != nil ||
		ids.Validate(ids.KindWorkItem, attempt.WorkItemID) != nil ||
		ids.Validate(ids.KindTask, attempt.TaskID) != nil || attempt.Number == 0 {
		return invalid("execution attempt identity is invalid")
	}
	if err := attempt.Executor.Validate(); err != nil {
		return err
	}
	if err := attempt.Input.Validate(); err != nil {
		return err
	}
	if attempt.Direction != Forward && attempt.Direction != Recovery ||
		!validTime(attempt.AssignedAt) || !validTime(attempt.Deadline) ||
		!attempt.Deadline.After(attempt.AssignedAt) {
		return invalid("execution attempt direction or deadline is invalid")
	}
	if attempt.StartedAt != nil && (!validTime(*attempt.StartedAt) || attempt.StartedAt.Before(attempt.AssignedAt)) ||
		attempt.FinishedAt != nil && (!validTime(*attempt.FinishedAt) || attempt.FinishedAt.Before(attempt.AssignedAt)) ||
		attempt.StartedAt != nil && attempt.FinishedAt != nil && attempt.FinishedAt.Before(*attempt.StartedAt) {
		return invalid("execution attempt timestamps are inconsistent")
	}
	if attempt.FinishedAt == nil {
		if attempt.Outcome != "" || attempt.Result != nil {
			return invalid("unfinished execution cannot have a terminal result")
		}
	} else {
		if !validOutcome(attempt.Outcome) || attempt.Result == nil {
			return invalid("finished execution requires its captured result")
		}
		if err := attempt.Result.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// Assign is a pure transition. Its caller must atomically compare the observed
// item, dependency results, current executor generation and ALL conflict claims
// before publishing the returned item and attempt. It cannot authorize effects
// until that transaction succeeds.
func Assign(item Item, identity Identity, attemptID string, direction Direction,
	at, deadline time.Time,
) (Item, Attempt, error) {
	if err := item.Validate(); err != nil {
		return Item{}, Attempt{}, err
	}
	if err := identity.Validate(); err != nil {
		return Item{}, Attempt{}, err
	}
	if item.Phase != Ready || item.Input == nil || identity.Executor != item.Placement.Executor ||
		identity.HostID != item.Placement.HostID || item.AttemptNumber == math.MaxUint64 ||
		at.Before(item.UpdatedAt) || !deadline.After(at) || deadline.After(item.Deadline) {
		return Item{}, Attempt{}, conflict("work is not ready for this executor")
	}
	switch direction {
	case Forward:
		if item.ForwardRevoked || item.Cleanup != CleanupNone || deadline.After(item.Deadline) {
			return Item{}, Attempt{}, conflict("forward execution permission is unavailable")
		}
	case Recovery:
		if item.Cleanup != CleanupRequired && item.Cleanup != CleanupStopped {
			return Item{}, Attempt{}, conflict("captured cleanup is not ready for assignment")
		}
	default:
		return Item{}, Attempt{}, invalid("execution direction is invalid")
	}
	attempt := Attempt{
		ID: attemptID, WorkItemID: item.ID, TaskID: item.TaskID,
		Number: item.AttemptNumber + 1, Executor: identity, Direction: direction,
		Input: *item.Input, AssignedAt: at, Deadline: deadline,
	}
	if err := attempt.Validate(); err != nil {
		return Item{}, Attempt{}, err
	}
	next := Clone(item)
	next.Phase, next.ActiveAttemptID, next.AttemptNumber = Assigned, attempt.ID, attempt.Number
	next.UpdatedAt = at
	if direction == Recovery {
		next.Cleanup = CleanupRunning
	}
	return next, attempt, nil
}

// Start is idempotent for delivery of the same start acknowledgement. It never
// renews the deadline or changes executor ownership.
func Start(item Item, attempt Attempt, at time.Time) (Item, Attempt, error) {
	if err := matchActive(item, attempt); err != nil {
		return Item{}, Attempt{}, err
	}
	if attempt.StartedAt != nil {
		return Clone(item), cloneAttempt(attempt), nil
	}
	if item.Phase != Assigned || at.Before(item.UpdatedAt) || !at.Before(attempt.Deadline) ||
		attempt.Direction == Forward && item.ForwardRevoked {
		return Item{}, Attempt{}, conflict("execution cannot start under this assignment")
	}
	next, execution := Clone(item), cloneAttempt(attempt)
	next.Phase, next.UpdatedAt = Running, at
	execution.StartedAt = &at
	return next, execution, nil
}

// Completion references operation-owned evidence. EffectsSettled is supplied
// only after the operation owner verifies that evidence, not from a worker's
// status string, a timeout or a diagnostic event.
type Completion struct {
	Outcome        Outcome
	Evidence       Input
	EffectsSettled bool
}

func Finish(item Item, attempt Attempt, result Completion, at time.Time) (Item, Attempt, error) {
	if err := matchActive(item, attempt); err != nil {
		return Item{}, Attempt{}, err
	}
	if !validOutcome(result.Outcome) || at.Before(item.UpdatedAt) ||
		result.Outcome == Succeeded && (!result.EffectsSettled || attempt.StartedAt == nil) {
		return Item{}, Attempt{}, invalid("execution completion does not prove its outcome")
	}
	if err := result.Evidence.Validate(); err != nil {
		return Item{}, Attempt{}, err
	}
	next, execution := Clone(item), cloneAttempt(attempt)
	execution.FinishedAt, execution.Outcome, execution.Result = &at, result.Outcome, &result.Evidence
	next.ActiveAttemptID, next.UpdatedAt = "", at
	if attempt.Direction == Forward {
		next.Outcome = result.Outcome
		next.Result = &result.Evidence
		if result.Outcome == Failed && next.FirstFailure == nil {
			failure := result.Evidence
			next.FirstFailure = &failure
			next.FirstFailureAttempt = attempt.Number
		}
		if result.EffectsSettled {
			next.Phase, next.Cleanup = Settled, CleanupNone
		} else {
			next.Phase, next.Cleanup = Attention, CleanupRequired
		}
	} else if result.EffectsSettled {
		next.Phase, next.Cleanup = Settled, CleanupComplete
		next.CleanupResult = &result.Evidence
	} else {
		next.Phase, next.Cleanup = Attention, CleanupStopped
		next.CleanupResult = &result.Evidence
	}
	if err := next.Validate(); err != nil {
		return Item{}, Attempt{}, err
	}
	if err := execution.Validate(); err != nil {
		return Item{}, Attempt{}, err
	}
	return next, execution, nil
}

// RevokeForward stops future forward permission. An active executor must still
// account for its effects; revocation alone cannot clear its assignment/claims.
func RevokeForward(item Item, outcome Outcome, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if outcome != Cancelled && outcome != Superseded || at.Before(item.UpdatedAt) {
		return Item{}, invalid("forward revocation is invalid")
	}
	next := Clone(item)
	if item.Phase == Settled {
		return next, nil
	}
	next.ForwardRevoked, next.UpdatedAt = true, at
	if next.ActiveAttemptID == "" && next.Cleanup == CleanupNone {
		next.Phase, next.Outcome, next.Wait = Settled, outcome, nil
	}
	return next, nil
}

func matchActive(item Item, attempt Attempt) error {
	if err := item.Validate(); err != nil {
		return err
	}
	if err := attempt.Validate(); err != nil {
		return err
	}
	if item.ID != attempt.WorkItemID || item.TaskID != attempt.TaskID ||
		item.ActiveAttemptID != attempt.ID || item.AttemptNumber != attempt.Number ||
		item.Input == nil || *item.Input != attempt.Input || attempt.FinishedAt != nil ||
		item.Placement.Executor != attempt.Executor.Executor || item.Placement.HostID != attempt.Executor.HostID {
		return conflict("execution attempt no longer owns this work")
	}
	return nil
}

func conflict(message string) error {
	return errs.New(errs.KindStateConflict, message)
}
