package workqueue

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// PrepareCapturedMembership fixes the size of a deferred removal graph before
// publishing any item. The operation owner must join its sealed-plan comparison.
func PrepareCapturedMembership(snapshot TaskSnapshot, count uint32, at time.Time) (Change, error) {
	task := snapshot.Task
	if snapshot.Revision <= 0 || task.Action != workexecution.ActionRemove || task.ExpectedItems != 0 ||
		task.PublicationComplete || task.Progress.Total() != 0 || task.FinishedAt != nil || count == 0 ||
		count > workexecution.MaximumTaskItems || at.Before(task.UpdatedAt) {
		return Change{}, invalid("removal membership requires an unpublished captured plan")
	}
	next := workexecution.CloneTask(task)
	next.ExpectedItems, next.UpdatedAt = count, at
	return taskChange(snapshot, next)
}
