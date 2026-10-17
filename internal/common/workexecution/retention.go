package workexecution

import "time"

// BeginCollection closes Retry before deleting any operation input or attempt.
// Once entered, this state cannot be reversed. Collection may resume in bounded
// batches after a crash without exposing half-retired history as executable work.
func BeginCollection(task Task, at time.Time) (Task, error) {
	if err := task.Validate(); err != nil {
		return Task{}, err
	}
	if task.CollectionStartedAt != nil {
		return CloneTask(task), nil
	}
	if !validTime(at) || task.FinishedAt == nil || task.RetainUntil == nil ||
		task.Progress.Unsettled() != 0 || at.Before(*task.RetainUntil) || at.Before(task.UpdatedAt) {
		return Task{}, conflict("task history is not eligible for collection")
	}
	next := CloneTask(task)
	next.CollectionStartedAt, next.UpdatedAt = &at, at
	return next, next.Validate()
}
