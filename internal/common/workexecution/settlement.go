package workexecution

import "time"

// SettleUnstarted closes work that never obtained an execution assignment. The
// operation owner supplies proof of a rejected prerequisite, expired deadline,
// superseded intent or already-satisfied action. It cannot close unknown effects
// or manufacture success for an interrupted attempt.
func SettleUnstarted(item Item, result Completion, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if err := result.Evidence.Validate(); err != nil {
		return Item{}, err
	}
	if item.AttemptNumber != 0 || item.ActiveAttemptID != "" ||
		item.Phase != Ready && item.Phase != Waiting || item.Cleanup != CleanupNone ||
		!validOutcome(result.Outcome) || !result.EffectsSettled || at.Before(item.UpdatedAt) {
		return Item{}, conflict("work requires execution reconciliation before settlement")
	}
	next := Clone(item)
	next.Phase, next.Outcome, next.Wait, next.UpdatedAt = Settled, result.Outcome, nil, at
	next.Result = &result.Evidence
	if result.Outcome == Failed && next.FirstFailure == nil {
		failure := result.Evidence
		next.FirstFailure = &failure
	}
	if result.Outcome == Cancelled || result.Outcome == Superseded {
		next.ForwardRevoked = true
	}
	return next, next.Validate()
}

// SettleQueuedRetry withdraws an authorized forward retry before assignment.
// Retry already required settled effects; neither that proof nor the historical
// attempt is replaced by a synthetic execution of the cancelled retry.
func SettleQueuedRetry(item Item, result Completion, at time.Time) (Item, error) {
	if err := item.Validate(); err != nil {
		return Item{}, err
	}
	if result.Evidence.Validate() != nil || !validTime(at) || at.Before(item.UpdatedAt) ||
		item.AttemptNumber == 0 || item.ActiveAttemptID != "" || item.FirstFailure == nil ||
		item.Cleanup != CleanupNone || item.Outcome != "" || item.Result != nil ||
		item.Phase != Ready && (item.Phase != Waiting || item.Wait == nil || item.Wait.Reason != WaitRetry) ||
		!result.EffectsSettled || result.Outcome != Failed && result.Outcome != Cancelled && result.Outcome != Superseded {
		return Item{}, conflict("retry settlement requires queued work with previously settled effects")
	}
	next := Clone(item)
	next.Phase, next.Outcome, next.Wait, next.UpdatedAt = Settled, result.Outcome, nil, at
	next.Result = &result.Evidence
	if result.Outcome == Cancelled || result.Outcome == Superseded {
		next.ForwardRevoked = true
	}
	return next, next.Validate()
}
