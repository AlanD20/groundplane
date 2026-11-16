package acceptedaction

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// QueueTimeout bounds dependency waiting and preparation for simple actions.
// Release actions retain their explicit configured whole-Task deadline instead.
const QueueTimeout = 24 * time.Hour

// ExecutionDeadline retains each operation's existing timeout from actual
// capture time, without extending the immutable accepted Task deadline.
func ExecutionDeadline(task workexecution.Task, capturedAt time.Time, timeout time.Duration) time.Time {
	deadline := capturedAt.Add(timeout)
	if task.Deadline.Before(deadline) {
		return task.Deadline
	}
	return deadline
}
