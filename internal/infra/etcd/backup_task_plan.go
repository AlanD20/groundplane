package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

const backupTaskTimeoutSeconds = 6 * 60 * 60

func backupTaskTimeout(taskType taskjournal.TaskType) int64 {
	if taskType == taskjournal.TaskBackupPrune {
		return 30 * 60
	}
	return backupTaskTimeoutSeconds
}
