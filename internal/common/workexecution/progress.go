package workexecution

import "time"

// CountPublication applies only to newly stored items before the publication
// barrier opens. The storage layer compares their absent keys atomically.
func CountPublication(task Task, items []Item, at time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if task.PublicationComplete || at.Before(task.UpdatedAt) ||
		task.Progress.Total()+uint64(len(items)) > uint64(task.ExpectedItems) {
		return Task{}, conflict("task work publication is closed or exceeds its captured plan")
	}
	next := CloneTask(task)
	seen := make(map[string]struct{}, len(items))
	for _, item := range items {
		if err := item.Validate(); err != nil {
			return Task{}, err
		}
		if item.TaskID != task.ID || item.AttemptNumber != 0 || item.Outcome != "" ||
			item.Cleanup != CleanupNone || item.Phase != Waiting && item.Phase != Ready ||
			item.CreatedAt.Before(task.CreatedAt) || item.Deadline.After(task.Deadline) {
			return Task{}, invalid("published work does not match its captured task")
		}
		if _, exists := seen[item.ID]; exists {
			return Task{}, invalid("work publication contains a duplicate item")
		}
		seen[item.ID] = struct{}{}
		counter := progressCounter(&next.Progress, item)
		*counter++
	}
	next.UpdatedAt = at
	return next, nil
}

// CountTransition keeps Task progress in the same atomic write as the work
// transition. Failure is retained even while cleanup runs or more work settles.
func CountTransition(task Task, before, after Item, at time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if err := before.Validate(); err != nil {
		return Task{}, err
	}
	if err := after.Validate(); err != nil {
		return Task{}, err
	}
	if !task.PublicationComplete || task.FinishedAt != nil || before.TaskID != task.ID ||
		after.TaskID != task.ID || before.ID != after.ID || at.Before(task.UpdatedAt) {
		return Task{}, conflict("task no longer accepts this execution transition")
	}
	next := CloneTask(task)
	previous := progressCounter(&next.Progress, before)
	if previous == nil || *previous == 0 {
		return Task{}, invalid("task progress does not contain the previous work state")
	}
	*previous--
	current := progressCounter(&next.Progress, after)
	if current == nil {
		return Task{}, invalid("work state cannot be counted")
	}
	*current++
	if next.FirstFailure == nil && after.FirstFailure != nil {
		failure := *after.FirstFailure
		next.FirstFailure = &failure
	}
	if after.Outcome == Failed {
		next.Outcome = Failed
	} else if after.Outcome == Cancelled && next.Outcome != Failed {
		next.Outcome = Cancelled
	} else if after.Outcome == Superseded && next.Outcome == "" {
		next.Outcome = Superseded
	}
	next.UpdatedAt = at
	return next, nil
}

// CompleteTask is separate from an item result: operation-owned aggregate
// settlement and absence of unfinished work must join the storage transaction.
func CompleteTask(task Task, at, retainUntil time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if !task.PublicationComplete || task.Progress.Unsettled() != 0 || task.FinishedAt != nil ||
		at.Before(task.UpdatedAt) || !retainUntil.After(at) {
		return Task{}, conflict("task work has not fully settled")
	}
	next := CloneTask(task)
	outcome, err := CompletionOutcome(task)
	if err != nil {
		return Task{}, err
	}
	next.Outcome = outcome
	next.UpdatedAt, next.FinishedAt, next.RetainUntil = at, &at, &retainUntil
	if err := next.Validate(); err != nil {
		return Task{}, err
	}
	return next, nil
}

// CompletionOutcome supplies the shared outcome policy without inventing a
// finish time or retention window while an aggregate owner prepares its writes.
func CompletionOutcome(task Task) (Outcome, error) {
	if err := task.Validate(); err != nil {
		return "", err
	}
	if !task.PublicationComplete || task.Progress.Unsettled() != 0 || task.FinishedAt != nil {
		return "", conflict("task work has not fully settled")
	}
	switch {
	case task.Progress.Failed > 0:
		return Failed, nil
	case task.Stop != nil:
		return task.Stop.Outcome, nil
	case task.Progress.Cancelled > 0:
		return Cancelled, nil
	case task.Progress.Superseded > 0:
		return Superseded, nil
	default:
		return Succeeded, nil
	}
}

// CountRetry reopens the same public Task instead of manufacturing another Task
// identity. Captured timing is renewed only by the operation's retry decision.
// The first failure stays available even if the new forward attempt succeeds.
func CountRetry(task Task, before, after Item, at time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if err := before.Validate(); err != nil {
		return Task{}, err
	}
	if err := after.Validate(); err != nil {
		return Task{}, err
	}
	if !task.PublicationComplete || task.CollectionStartedAt != nil || before.TaskID != task.ID ||
		after.TaskID != task.ID ||
		before.ID != after.ID ||
		before.AttemptNumber != after.AttemptNumber ||
		before.Phase != Settled && before.Phase != Attention ||
		after.Phase != Ready && after.Phase != Waiting ||
		at.Before(task.UpdatedAt) {
		return Task{}, conflict("task cannot resume this captured work")
	}
	if task.Stop != nil && after.Cleanup == CleanupNone {
		return Task{}, conflict("stopped task cannot retry forward execution")
	}
	next := CloneTask(task)
	previous := progressCounter(&next.Progress, before)
	current := progressCounter(&next.Progress, after)
	if previous == nil || current == nil || *previous == 0 {
		return Task{}, invalid("task retry progress is inconsistent")
	}
	*previous--
	*current++
	next.FinishedAt, next.RetainUntil, next.UpdatedAt = nil, nil, at
	if after.Deadline.After(next.Deadline) {
		next.Deadline = after.Deadline
	}
	// Cleanup retries retain the failed action. A settled forward retry may clear
	// the aggregate outcome only when no other failed/cancelled work remains.
	if after.Outcome == "" && next.Progress.Failed == 0 && next.Progress.Attention == 0 &&
		next.Progress.Cancelled == 0 && next.Progress.Superseded == 0 {
		next.Outcome = ""
	}
	return next, nil
}

func progressCounter(progress *Progress, item Item) *uint32 {
	switch item.Phase {
	case Waiting:
		return &progress.Waiting
	case Ready:
		return &progress.Ready
	case Assigned:
		return &progress.Assigned
	case Running:
		return &progress.Running
	case Attention:
		return &progress.Attention
	case Settled:
		switch item.Outcome {
		case Succeeded:
			return &progress.Succeeded
		case Failed:
			return &progress.Failed
		case Cancelled:
			return &progress.Cancelled
		case Superseded:
			return &progress.Superseded
		}
	}
	return nil
}
