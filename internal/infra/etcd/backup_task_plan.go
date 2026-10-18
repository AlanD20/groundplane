package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

const backupTaskTimeoutSeconds = 6 * 60 * 60

func backupTaskTimeout(taskType taskjournal.TaskType) int64 {
	if taskType == taskjournal.TaskBackupPrune {
		return 30 * 60
	}
	return backupTaskTimeoutSeconds
}

// TaskForwardDeadline keeps Backup budgets relative to publication, not claim
// or reconnect. Ordinary Tasks retain their execution-start boundary.
func TaskForwardDeadline(task TaskRecord, assignedAt time.Time) time.Time {
	start := assignedAt
	if task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskBackupPrune ||
		task.Type == taskjournal.TaskRestore || taskjournal.IsReleaseNetworkChild(task.Params) ||
		task.Params[taskjournal.TaskReleaseNetworkChildParam] != "" {
		start = task.CreatedAt
	}
	return start.Add(time.Duration(task.TimeoutSeconds) * time.Second)
}
