package controllertask

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"log/slog"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Store interface {
	ListControllerTaskClaims(context.Context) ([]etcd.TaskAssignment, error)
	ClaimNextControllerTask(context.Context, time.Time) (etcd.TaskAssignment, bool, error)
	AcknowledgeControllerTask(
		context.Context,
		string,
		taskjournal.TaskStatus,
		time.Time,
	) (etcdstore.Versioned[etcd.TaskRecord], error)
}

type Handler interface {
	Execute(context.Context, etcd.TaskRecord) error
}

// Runner serially resumes or claims native Controller Tasks. Handler effects
// must be idempotent because a process crash after the effect but before the
// terminal transaction deliberately leaves the durable claim for replay.
type Runner struct {
	store    Store
	handler  Handler
	updates  UpdateExecutor
	interval time.Duration
	logger   *slog.Logger
	now      func() time.Time
	wake     chan struct{}

	mu     sync.Mutex
	active *activeExecution
}

type activeExecution struct {
	taskID        string
	cancel        context.CancelFunc
	done          chan struct{}
	operatorAbort bool
	result        error
	update        bool
}

func New(
	store Store, handler Handler, updates UpdateExecutor, interval time.Duration, logger *slog.Logger,
) (*Runner, error) {
	if store == nil || handler == nil || interval <= 0 || logger == nil {
		return nil, errs.New(errs.KindInternal, "Controller Task runner dependencies are invalid")
	}
	return &Runner{
		store: store, handler: handler, updates: updates, interval: interval, logger: logger,
		now: time.Now, wake: make(chan struct{}, 1),
	}, nil
}

// Wake requests an immediate durable queue scan. Signals coalesce so Task
// publication never blocks on runner availability; the interval remains the
// recovery path when a signal is lost across process failure.
func (runner *Runner) Wake() {
	if runner == nil || runner.wake == nil {
		return
	}
	select {
	case runner.wake <- struct{}{}:
	default:
	}
}

// Run executes immediately on startup and then waits only when no work was
// available or storage temporarily failed. Cancellation never terminalizes a
// running claim; the next Controller process resumes it.
func (runner *Runner) Run(ctx context.Context) {
	if ctx == nil {
		return
	}
	ticker := time.NewTicker(runner.interval)
	defer ticker.Stop()
	for {
		progressed, err := runner.runOne(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			runner.logger.Error(
				"Controller Task runner pass failed",
				slog.String("code", errorCode(err)),
				slog.Any("error", err),
			)
		}
		if progressed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-runner.wake:
		case <-ticker.C:
		}
	}
}

func (runner *Runner) runOne(ctx context.Context) (bool, error) {
	claims, err := runner.store.ListControllerTaskClaims(ctx)
	if err != nil {
		return false, err
	}
	if len(claims) > 1 {
		return false, errs.New(errs.KindInternal, "Controller Task runner received multiple claims")
	}
	var claim etcd.TaskAssignment
	if len(claims) == 1 {
		claim = claims[0]
	} else {
		var found bool
		claim, found, err = runner.store.ClaimNextControllerTask(ctx, runner.now().UTC())
		if err != nil || !found {
			return false, err
		}
	}
	if err := runner.execute(ctx, claim); err != nil {
		return true, err
	}
	return true, nil
}

func (runner *Runner) execute(ctx context.Context, claim etcd.TaskAssignment) (result error) {
	if claim.Assignment.Record.Executor != taskjournal.TaskExecutorController ||
		claim.Task.Record.Executor != taskjournal.TaskExecutorController ||
		claim.Assignment.Record.TaskID != claim.Task.Record.ID {
		return errs.New(errs.KindInternal, "Controller Task runner received an invalid claim")
	}
	deadline := claim.Assignment.Record.Deadline
	if isPlatformUpdate(claim.Task.Record) {
		return runner.executeUpdate(ctx, claim)
	}
	now := runner.now().UTC()
	if !now.Before(deadline) {
		_, err := runner.store.AcknowledgeControllerTask(
			ctx, claim.Task.Record.ID, taskjournal.TaskStatusTimedOut, now,
		)
		return err
	}
	executionContext, cancel := context.WithTimeout(ctx, deadline.Sub(now))
	active := &activeExecution{taskID: claim.Task.Record.ID, cancel: cancel, done: make(chan struct{})}
	runner.mu.Lock()
	if runner.active != nil {
		runner.mu.Unlock()
		cancel()
		return errs.New(errs.KindInternal, "Controller Task runner already has an active execution")
	}
	runner.active = active
	runner.mu.Unlock()
	defer func() {
		runner.finishExecution(active, result)
	}()
	executionErr := runner.handler.Execute(executionContext, claim.Task.Record)
	cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	terminalAt := runner.now().UTC()
	status := taskjournal.TaskStatusCompleted
	if executionErr != nil {
		status = taskjournal.TaskStatusFailed
		runner.logger.Error(
			"Controller Task execution failed",
			slog.String("task_id", claim.Task.Record.ID),
			slog.String("code", errorCode(executionErr)),
			slog.Any("error", executionErr),
		)
		runner.mu.Lock()
		operatorAbort := active.operatorAbort
		runner.mu.Unlock()
		if operatorAbort && errors.Is(executionErr, context.Canceled) {
			status = taskjournal.TaskStatusAborted
		} else if errors.Is(executionContext.Err(), context.DeadlineExceeded) || !terminalAt.Before(deadline) {
			status = taskjournal.TaskStatusTimedOut
		}
	}
	_, err := runner.store.AcknowledgeControllerTask(ctx, claim.Task.Record.ID, status, terminalAt)
	return err
}

// AbortTask cancels only the exact active native Controller Task and returns
// after its terminal acknowledgement has committed.
func (runner *Runner) AbortTask(ctx context.Context, taskID string) error {
	if ctx == nil || ids.Validate(ids.KindTask, taskID) != nil {
		return errs.New(errs.KindValidationFailed, "Controller Task abort target is invalid")
	}
	runner.mu.Lock()
	active := runner.active
	if active == nil || active.taskID != taskID {
		runner.mu.Unlock()
		return errs.New(errs.KindStateConflict, "Controller Task is not executing in this process")
	}
	if active.update {
		runner.mu.Unlock()
		if err := runner.updates.Abort(ctx, taskID); err != nil {
			return err
		}
		return runner.waitExecution(ctx, active)
	}
	active.operatorAbort = true
	active.cancel()
	runner.mu.Unlock()
	return runner.waitExecution(ctx, active)
}

func (runner *Runner) waitExecution(ctx context.Context, active *activeExecution) error {
	select {
	case <-active.done:
		runner.mu.Lock()
		result := active.result
		runner.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (runner *Runner) finishExecution(active *activeExecution, result error) {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	active.result = result
	if runner.active == active {
		runner.active = nil
	}
	close(active.done)
}

func errorCode(err error) string {
	var domainError *errs.Error
	if errors.As(err, &domainError) {
		return string(domainError.Code)
	}
	if errors.Is(err, context.Canceled) {
		return "context.canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "context.deadline_exceeded"
	}
	return string(errs.CodeInternal)
}
