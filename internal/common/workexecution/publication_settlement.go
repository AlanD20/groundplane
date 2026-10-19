package workexecution

import "time"

// SettlePublication records a rejected captured item before dispatch opens.
// Existing items are counted out of their staged state; missing items are added
// directly as terminal members. It never invents an attempt or permits success.
// The storage owner must compare the immutable graph and settle any resource
// ownership it acquired during staging in this same decision.
func SettlePublication(task Task, item Item, published bool, result Completion, at time.Time) (Task, Item, error) {
	if task.Validate() != nil || task.PublicationComplete || task.FinishedAt != nil ||
		task.CollectionStartedAt != nil || !validTime(at) || at.Before(task.UpdatedAt) ||
		item.TaskID != task.ID || item.CreatedAt.Before(task.CreatedAt) || item.Deadline.After(task.Deadline) ||
		result.Outcome != Failed && result.Outcome != Cancelled && result.Outcome != Superseded ||
		result.Outcome != Failed && (task.Stop == nil || task.Stop.Outcome != result.Outcome) {
		return Task{}, Item{}, conflict(
			"publication settlement requires unexecuted captured work and a rejection or stop",
		)
	}
	nextItem, err := SettleUnstarted(item, result, at)
	if err != nil {
		return Task{}, Item{}, err
	}
	next := CloneTask(task)
	if published {
		previous := progressCounter(&next.Progress, item)
		if previous == nil || *previous == 0 {
			return Task{}, Item{}, invalid("publication progress does not contain this staged item")
		}
		*previous--
	} else if next.Progress.Total() >= uint64(next.ExpectedItems) {
		return Task{}, Item{}, invalid("publication settlement exceeds captured membership")
	}
	current := progressCounter(&next.Progress, nextItem)
	*current++
	if next.FirstFailure == nil && nextItem.FirstFailure != nil {
		failure := *nextItem.FirstFailure
		next.FirstFailure = &failure
	}
	if result.Outcome == Failed || next.Outcome == "" {
		next.Outcome = result.Outcome
	}
	next.UpdatedAt = at
	if err := next.Validate(); err != nil {
		return Task{}, Item{}, err
	}
	return next, nextItem, nil
}
