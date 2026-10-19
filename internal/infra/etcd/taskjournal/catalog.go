package taskjournal

// TaskType identifies prepared resource intents. Execution and attempt state
// belong exclusively to workexecution and workqueue, not to this catalog.
type TaskType string

const (
	TaskBackupPrune TaskType = "backup_prune"
	TaskCreate      TaskType = "create"
	TaskUpdate      TaskType = "update"
	TaskRemove      TaskType = "remove"
)

type TaskExecutor string

const TaskExecutorAgent TaskExecutor = "agent"

// TaskStatus is retained by prepared resource intents until their operation
// settles. It is not a queue, execution state machine or retry authority.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusAborted   TaskStatus = "aborted"
	TaskStatusTimedOut  TaskStatus = "timed_out"
)

func IsTerminalTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}
