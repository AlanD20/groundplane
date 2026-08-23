package controllertask

import (
	"context"
	"errors"
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
		etcd.TaskStatus,
		time.Time,
	) (etcd.Versioned[etcd.TaskRecord], error)
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
	interval time.Duration
	logger   *slog.Logger
	now      func() time.Time

	mu     sync.Mutex
	active *activeExecution
}

type activeExecution struct {
	taskID        string
	cancel        context.CancelFunc
	done          chan struct{}
	operatorAbort bool
	result        error
}

func New(store Store, handler Handler, interval time.Duration, logger *slog.Logger) (*Runner, error) {
	if store == nil || handler == nil || interval <= 0 || logger == nil {
		return nil, errs.New(errs.KindInternal, "Controller Task runner dependencies are invalid")
	}
	return &Runner{
		store: store, handler: handler, interval: interval, logger: logger,
		now: time.Now,
	}, nil
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
			runner.logger.Error("Controller Task runner pass failed", slog.String("code", errorCode(err)))
		}
		if progressed && err == nil {
			continue
		}
		select {
		case <-ctx.Done():
			return
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
	if claim.Assignment.Record.Executor != etcd.TaskExecutorController ||
		claim.Task.Record.Executor != etcd.TaskExecutorController ||
		claim.Assignment.Record.TaskID != claim.Task.Record.ID {
		return errs.New(errs.KindInternal, "Controller Task runner received an invalid claim")
	}
	deadline := claim.Assignment.Record.Deadline
	now := runner.now().UTC()
	if !now.Before(deadline) {
		_, err := runner.store.AcknowledgeControllerTask(
			ctx, claim.Task.Record.ID, etcd.TaskStatusTimedOut, now,
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
		runner.mu.Lock()
		active.result = result
		if runner.active == active {
			runner.active = nil
		}
		close(active.done)
		runner.mu.Unlock()
	}()
	executionErr := runner.handler.Execute(executionContext, claim.Task.Record)
	cancel()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	terminalAt := runner.now().UTC()
	status := etcd.TaskStatusCompleted
	if executionErr != nil {
		status = etcd.TaskStatusFailed
		runner.mu.Lock()
		operatorAbort := active.operatorAbort
		runner.mu.Unlock()
		if operatorAbort && errors.Is(executionErr, context.Canceled) {
			status = etcd.TaskStatusAborted
		} else if errors.Is(executionContext.Err(), context.DeadlineExceeded) || !terminalAt.Before(deadline) {
			status = etcd.TaskStatusTimedOut
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
	active.operatorAbort = true
	active.cancel()
	done := active.done
	runner.mu.Unlock()
	select {
	case <-done:
		runner.mu.Lock()
		result := active.result
		runner.mu.Unlock()
		return result
	case <-ctx.Done():
		return ctx.Err()
	}
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
