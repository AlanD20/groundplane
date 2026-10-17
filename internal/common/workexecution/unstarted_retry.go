package workexecution

import "time"

// RetryUnstarted reopens a failed queued action without inventing an execution
// attempt. The operation owner must verify the saved failure and captured input.
func RetryUnstarted(item Item, deadline, at time.Time) (Item, error) {
	if item.Validate() != nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Input == nil || item.Result == nil ||
		item.Phase != Settled || item.Outcome != Failed || item.Cleanup != CleanupNone || item.ForwardRevoked ||
		!validTime(at) || !validTime(deadline) || at.Before(item.UpdatedAt) || !deadline.After(at) {
		return Item{}, conflict("retry requires a failed captured action that has not executed")
	}
	next := Clone(item)
	next.Phase, next.Outcome, next.Result, next.Wait = Ready, "", nil, nil
	next.Deadline, next.UpdatedAt = deadline, at
	return next, next.Validate()
}

// RenewUnstartedDeadline follows the explicitly renewed Task budget for work
// still queued behind dependencies. It does not change input or grant effects.
func RenewUnstartedDeadline(item Item, deadline, at time.Time) (Item, error) {
	if item.Validate() != nil || item.AttemptNumber != 0 || item.ActiveAttemptID != "" || item.Input == nil || item.Result != nil ||
		item.Phase != Ready && item.Phase != Waiting || item.Cleanup != CleanupNone || item.ForwardRevoked ||
		!validTime(at) || !validTime(deadline) || at.Before(item.UpdatedAt) || !deadline.After(at) || !deadline.After(item.Deadline) {
		return Item{}, conflict("deadline renewal requires unexecuted queued work")
	}
	next := Clone(item)
	next.Deadline, next.UpdatedAt = deadline, at
	return next, next.Validate()
}
