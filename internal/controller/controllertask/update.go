package controllertask

import (
	"context"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// UpdateExecutor owns only Controller/Agent update recovery. Unlike ordinary
// effect replay, expiry must still recover the pinned predecessor before
// acknowledging. Errors mean unresolved recovery: retain the same claim.
// Restore establishes admission holds without requiring a running channel.
type UpdateExecutor interface {
	Restore(context.Context, etcd.TaskAssignment) error
	Execute(context.Context, etcd.TaskRecord, time.Time) (taskjournal.TaskStatus, error)
	Abort(context.Context, string) error
}

// Restore is a startup barrier and must finish before the Agent channel opens.
// It does not claim new work or execute effects that require authenticated Ready.
func (runner *Runner) Restore(ctx context.Context) error {
	claims, err := runner.store.ListControllerTaskClaims(ctx)
	if err != nil {
		return err
	}
	if len(claims) > 1 {
		return errs.New(errs.KindInternal, "controller startup received multiple native claims")
	}
	if len(claims) == 0 || !isPlatformUpdate(claims[0].Task.Record) {
		return nil
	}
	if runner.updates == nil {
		return errs.New(errs.KindInternal, "controller update recovery is not configured")
	}
	return runner.updates.Restore(ctx, claims[0])
}

func (runner *Runner) executeUpdate(ctx context.Context, claim etcd.TaskAssignment) (result error) {
	if runner.updates == nil {
		return errs.New(errs.KindInternal, "controller update recovery is not configured")
	}
	active := &activeExecution{taskID: claim.Task.Record.ID, update: true, done: make(chan struct{})}
	runner.mu.Lock()
	if runner.active != nil {
		runner.mu.Unlock()
		return errs.New(errs.KindInternal, "controller Task runner already has an active execution")
	}
	runner.active = active
	runner.mu.Unlock()
	defer func() { runner.finishExecution(active, result) }()
	if err := runner.updates.Restore(ctx, claim); err != nil {
		return err
	}
	status, err := runner.updates.Execute(ctx, claim.Task.Record, claim.Assignment.Record.Deadline)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	switch status {
	case taskjournal.TaskStatusCompleted,
		taskjournal.TaskStatusFailed,
		taskjournal.TaskStatusAborted,
		taskjournal.TaskStatusTimedOut:
	default:
		return errs.New(errs.KindInternal, "controller update recovery returned no settled result")
	}
	_, err = runner.store.AcknowledgeControllerTask(ctx, claim.Task.Record.ID, status, runner.now().UTC())
	return err
}

func isPlatformUpdate(task etcd.TaskRecord) bool {
	resource := task.Params[taskjournal.TaskResourceKindParam]
	return task.Type == taskjournal.TaskUpdate &&
		(resource == taskjournal.TaskResourceAgent || resource == taskjournal.TaskResourceController)
}
