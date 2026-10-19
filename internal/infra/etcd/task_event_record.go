package etcd

import (
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// PreparedTaskEvent contains the values a repository must publish in one CAS
// transaction. Duplicate is true only for an identical prior Agent event.
type PreparedTaskEvent struct {
	Task      TaskRecord
	Event     taskjournal.TaskEventRecord
	Dedup     taskjournal.TaskEventDedupRecord
	Sequence  uint64
	Duplicate bool
}

func taskEventBelongsToTask(task TaskRecord, identity taskjournal.TaskEventIdentity) bool {
	if identity.ControllerOperation() {
		return task.Executor == taskjournal.TaskExecutorController
	}
	return task.Executor == taskjournal.TaskExecutorAgent && taskContainsStep(task, identity.StepID)
}
