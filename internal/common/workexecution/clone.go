package workexecution

// Clone isolates mutable slices and optional values at publication/transition
// boundaries. It does not copy the operation's potentially large input bytes.
func Clone(item Item) Item {
	copy := item
	copy.Dependencies = append([]string(nil), item.Dependencies...)
	copy.Claims = append([]Claim(nil), item.Claims...)
	if item.Result != nil {
		result := *item.Result
		copy.Result = &result
	}
	if item.CleanupResult != nil {
		result := *item.CleanupResult
		copy.CleanupResult = &result
	}
	if item.FirstFailure != nil {
		failure := *item.FirstFailure
		copy.FirstFailure = &failure
	}
	if item.Input != nil {
		input := *item.Input
		copy.Input = &input
	}
	if item.Wait != nil {
		wait := *item.Wait
		if wait.Until != nil {
			until := *wait.Until
			wait.Until = &until
		}
		copy.Wait = &wait
	}
	return copy
}

func cloneAttempt(attempt Attempt) Attempt {
	copy := attempt
	if attempt.StartedAt != nil {
		at := *attempt.StartedAt
		copy.StartedAt = &at
	}
	if attempt.FinishedAt != nil {
		at := *attempt.FinishedAt
		copy.FinishedAt = &at
	}
	if attempt.Result != nil {
		result := *attempt.Result
		copy.Result = &result
	}
	return copy
}
