package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func firstTaskEventSequence(task TaskRecord) uint64 {
	return task.NextEventSequence - uint64(task.EventCount)
}

func nextTaskControllerTimestamp(previous time.Time, supplied time.Time) (time.Time, error) {
	if err := recordcodec.ValidateTimestamp("task controller timestamp", supplied); err != nil {
		return time.Time{}, err
	}
	if supplied.After(previous) {
		return supplied, nil
	}
	return previous.Add(time.Nanosecond), nil
}

func validateTaskEventCheckpoints(task TaskRecord) error {
	limit := len(task.Steps)
	if task.Executor == taskjournal.TaskExecutorController {
		limit = 1
	}
	if len(task.EventCheckpoints) > limit {
		return recordcodec.CorruptRecord()
	}
	previous := ""
	for index, checkpoint := range task.EventCheckpoints {
		if taskjournal.ValidateTaskEventIdentity(checkpoint.Identity) != nil || checkpoint.Identity.TaskID != task.ID ||
			!taskEventBelongsToTask(task, checkpoint.Identity) || index > 0 && checkpoint.Identity.StepID <= previous ||
			checkpoint.Sequence == 0 || checkpoint.Sequence >= firstTaskEventSequence(task) ||
			!recordcodec.ValidSHA256(checkpoint.PayloadSHA256) || !taskjournal.ValidTaskEventState(checkpoint.State) ||
			checkpoint.Completed && !checkpoint.Running || checkpoint.Running && !checkpoint.EffectPossible ||
			checkpoint.State == taskjournal.TaskEventStateRunning && !checkpoint.Running ||
			checkpoint.State == taskjournal.TaskEventStateCompleted && !checkpoint.Completed ||
			checkpoint.State != taskjournal.TaskEventStatePending && !checkpoint.EffectPossible {
			return recordcodec.CorruptRecord()
		}
		previous = checkpoint.Identity.StepID
		if checkpoint.ControllerOperation.Validate() != nil ||
			checkpoint.Identity.ControllerOperation() != (checkpoint.ControllerOperation != nil) ||
			checkpoint.ControllerOperation != nil && checkpoint.HealthWait != nil {
			return recordcodec.CorruptRecord()
		}
		if checkpoint.Failure.Validate() != nil || checkpoint.Failure != nil &&
			checkpoint.State != taskjournal.TaskEventStateFailed && checkpoint.State != taskjournal.TaskEventStateTimedOut && checkpoint.State != taskjournal.TaskEventStateAborted {
			return recordcodec.CorruptRecord()
		}
		if checkpoint.HealthWait.Validate() != nil ||
			checkpoint.HealthWait == nil &&
				(checkpoint.HealthWaitOrdinal != 0 || checkpoint.HealthWaitReceivedAt != nil) ||
			checkpoint.HealthWait != nil &&
				(checkpoint.HealthWaitOrdinal < 2 || checkpoint.HealthWaitOrdinal > checkpoint.Identity.Ordinal ||
					checkpoint.HealthWaitReceivedAt == nil || recordcodec.ValidateTimestamp("health wait received_at", *checkpoint.HealthWaitReceivedAt) != nil || checkpoint.HealthWaitReceivedAt.After(task.UpdatedAt)) {
			return recordcodec.CorruptRecord()
		}
	}
	return nil
}
