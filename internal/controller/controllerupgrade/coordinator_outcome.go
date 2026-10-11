package controllerupgrade

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Execute returns the original observed cause only after the existing update
// state machine proves qualification or predecessor restoration. Reading a
// diagnostic never authorizes an otherwise incomplete recovery to finish.
func (coordinator *Coordinator) Execute(ctx context.Context, task etcd.TaskRecord, deadline time.Time,
	progress *controllertask.Progress,
) (taskjournal.ControllerOutcome, error) {
	if progress == nil {
		return taskjournal.ControllerOutcome{}, errs.New(
			errs.KindInternal,
			"Controller update progress recorder is required",
		)
	}
	status, err := coordinator.execute(ctx, task, deadline, progress)
	if err != nil {
		return taskjournal.ControllerOutcome{}, err
	}
	outcome := taskjournal.ControllerOutcome{Status: status}
	if status == taskjournal.TaskStatusCompleted {
		return outcome, nil
	}
	coordinator.mu.Lock()
	operation := coordinator.active
	if operation == nil || operation.expected.TaskID != task.ID || operation.settled != status {
		coordinator.mu.Unlock()
		return taskjournal.ControllerOutcome{}, errs.New(errs.KindStateConflict, "controller update outcome changed")
	}
	outcome.Failure = taskprogress.CloneFailure(operation.failure)
	coordinator.mu.Unlock()
	journal, found, err := coordinator.current(ctx, operation)
	if err != nil {
		return taskjournal.ControllerOutcome{}, err
	}
	if found && journal.Failure != nil {
		outcome.Failure = taskprogress.CloneFailure(journal.Failure)
	}
	if outcome.Failure == nil && status == taskjournal.TaskStatusAborted {
		outcome.Failure = taskprogress.CaptureControllerFailure(context.Canceled)
	}
	if outcome.Failure == nil && status == taskjournal.TaskStatusTimedOut {
		outcome.Failure = taskprogress.CaptureControllerFailure(context.DeadlineExceeded)
	}
	return outcome, nil
}
