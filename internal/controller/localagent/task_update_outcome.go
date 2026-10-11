package localagent

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (updates *TaskUpdates) Execute(ctx context.Context, task etcd.TaskRecord, deadline time.Time,
	progress *controllertask.Progress,
) (taskjournal.ControllerOutcome, error) {
	if progress == nil {
		return taskjournal.ControllerOutcome{}, errs.New(
			errs.KindInternal,
			"Agent update progress recorder is required",
		)
	}
	status, err := updates.execute(ctx, task, deadline, progress)
	if err != nil {
		return taskjournal.ControllerOutcome{}, err
	}
	updates.mu.Lock()
	operation := updates.active
	if operation == nil || operation.taskID != task.ID || operation.settled != status {
		updates.mu.Unlock()
		return taskjournal.ControllerOutcome{}, errs.New(errs.KindStateConflict, "agent update outcome changed")
	}
	outcome := taskjournal.ControllerOutcome{Status: status}
	if status != taskjournal.TaskStatusCompleted {
		outcome.Failure = taskprogress.CloneFailure(operation.failure)
	}
	request := operation.request
	updates.mu.Unlock()
	if status != taskjournal.TaskStatusCompleted {
		retained, err := updates.agents.ReplacementFailure(ctx, request)
		if err != nil {
			return taskjournal.ControllerOutcome{}, err
		}
		if retained != nil {
			outcome.Failure = retained
		}
	}
	if outcome.Failure == nil && status == taskjournal.TaskStatusAborted {
		outcome.Failure = taskprogress.CaptureControllerFailure(context.Canceled)
	}
	if outcome.Failure == nil && status == taskjournal.TaskStatusTimedOut {
		outcome.Failure = taskprogress.CaptureControllerFailure(context.DeadlineExceeded)
	}
	return outcome, nil
}
