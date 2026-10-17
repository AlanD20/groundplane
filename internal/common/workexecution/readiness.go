package workexecution

import "time"

// PrepareInput seals a previously unresolved item's exact operation input. The
// caller binds dependency outputs and current intent in the same transaction.
// Replacing an already prepared input is never readiness or retry.
func PrepareInput(item Item, input Input, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if err := input.Validate(); err != nil {
		return Item{}, err
	}
	if item.Phase != Waiting || item.ForwardRevoked || item.AttemptNumber != 0 ||
		at.Before(item.UpdatedAt) || !at.Before(item.Deadline) {
		return Item{}, conflict("work no longer accepts prepared input")
	}
	if item.Input != nil && *item.Input != input {
		return Item{}, conflict("captured work input cannot be replaced")
	}
	next := Clone(item)
	next.Input, next.Phase, next.Wait, next.UpdatedAt = &input, Ready, nil, at
	return next, nil
}

// RetryDecision is produced by the operation owner after classifying the saved
// result and checking its captured retry policy. The storage transaction must
// consume that authorization with the expected attempt/result and replay marker.
// The scheduler cannot construct a decision from a timeout or connection loss.
type RetryDecision struct {
	Direction Direction
	Result    Input
	NotBefore time.Time
	Deadline  time.Time
}

func Retry(item Item, previous Attempt, decision RetryDecision, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if err := previous.Validate(); err != nil {
		return Item{}, err
	}
	if err := decision.Result.Validate(); err != nil {
		return Item{}, err
	}
	if previous.WorkItemID != item.ID || previous.TaskID != item.TaskID || previous.Number != item.AttemptNumber ||
		previous.FinishedAt == nil || previous.Result == nil || *previous.Result != decision.Result ||
		item.ActiveAttemptID != "" || item.Input == nil || *item.Input != previous.Input ||
		at.Before(item.UpdatedAt) || !validTime(at) || !validTime(decision.NotBefore) ||
		!validTime(decision.Deadline) || decision.NotBefore.Before(at) || !decision.Deadline.After(decision.NotBefore) {
		return Item{}, conflict("retry does not match the completed captured execution")
	}
	next := Clone(item)
	switch decision.Direction {
	case Forward:
		if item.Phase != Settled || item.Outcome != Failed || item.ForwardRevoked ||
			item.Cleanup != CleanupNone && item.Cleanup != CleanupComplete {
			return Item{}, conflict("forward retry requires failed work with settled effects")
		}
		next.Cleanup, next.Outcome = CleanupNone, ""
		next.Result, next.CleanupResult = nil, nil
	case Recovery:
		if item.Phase != Attention || item.Cleanup != CleanupRequired && item.Cleanup != CleanupStopped {
			return Item{}, conflict("recovery retry requires unresolved captured cleanup")
		}
	default:
		return Item{}, invalid("retry direction is invalid")
	}
	next.Deadline, next.UpdatedAt = decision.Deadline, at
	if decision.NotBefore.After(at) {
		next.Phase = Waiting
		next.Wait = &Wait{Reason: WaitRetry, Until: &decision.NotBefore}
	} else {
		next.Phase, next.Wait = Ready, nil
	}
	return next, nil
}

// ReleaseRetryWait changes scheduling readiness, not retry permission. Only a
// previously persisted operation-owned decision can create this waiting state.
func ReleaseRetryWait(item Item, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if item.Phase != Waiting || item.Wait == nil || item.Wait.Reason != WaitRetry || item.Wait.Until == nil ||
		at.Before(*item.Wait.Until) || at.Before(item.UpdatedAt) || !at.Before(item.Deadline) ||
		item.ForwardRevoked && item.Cleanup == CleanupNone {
		return Item{}, conflict("captured retry is not ready")
	}
	next := Clone(item)
	next.Phase, next.Wait, next.UpdatedAt = Ready, nil, at
	return next, nil
}

// ExpireCleanupRetry withdraws scheduling permission, not cleanup ownership.
// No attempt is invented for work that never acquired a slot. The preceding
// result remains the basis for another explicit operation-authorized Retry.
func ExpireCleanupRetry(item Item, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if !validTime(at) || at.Before(item.UpdatedAt) || at.Before(item.Deadline) ||
		item.ActiveAttemptID != "" || item.AttemptNumber == 0 ||
		item.Phase != Ready && item.Phase != Waiting ||
		item.Cleanup != CleanupRequired && item.Cleanup != CleanupStopped {
		return Item{}, conflict("cleanup retry is not awaiting assignment beyond its deadline")
	}
	next := Clone(item)
	next.Phase, next.Cleanup, next.Wait, next.UpdatedAt = Attention, CleanupStopped, nil, at
	return next, next.Validate()
}
