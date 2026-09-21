package taskjournal

func ValidTaskTransition(current TaskStatus, next TaskStatus) bool {
	switch current {
	case TaskStatusPending:
		return next == TaskStatusRunning || next == TaskStatusAborted
	case TaskStatusRunning:
		return IsTerminalTaskStatus(next)
	default:
		return false
	}
}

func IsTerminalTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}

func ValidTaskType(taskType TaskType) bool {
	switch taskType {
	case TaskDeploy, TaskRollback, TaskBackup, TaskBackupPrune, TaskRestore, TaskAttach, TaskDetach,
		TaskRun, TaskScript, TaskProvision, TaskCreate, TaskUpdate, TaskRemove,
		TaskStart, TaskStop, TaskDestroy, TaskRotate:
		return true
	default:
		return false
	}
}

func ValidTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusPending, TaskStatusRunning, TaskStatusCompleted,
		TaskStatusFailed, TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}
