package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/adapters"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type PlanHash [sha256.Size]byte

type TaskStep struct {
	StepID string
	Step   adapters.Step
}

type Assignment struct {
	TaskID   string
	PlanHash PlanHash
	Steps    []TaskStep
	Timeout  time.Duration
}

type TaskTerminal uint8

const (
	TaskTerminalCompleted TaskTerminal = iota + 1
	TaskTerminalFailed
	TaskTerminalTimedOut
	TaskTerminalAborted
)

type TaskResult struct {
	TaskID   string
	PlanHash PlanHash
	Terminal TaskTerminal
	ExitCode int32
	Result   []byte
}

type TaskProgressState uint8

const (
	TaskProgressRunning TaskProgressState = iota + 1
	TaskProgressCompleted
	TaskProgressFailed
	TaskProgressTimedOut
	TaskProgressAborted
)

type TaskProgress struct {
	TaskID   string
	PlanHash PlanHash
	StepID   string
	Attempt  uint32
	Ordinal  uint64
	State    TaskProgressState
	Chunk    []byte
}

// WorkerOutput is a closed ordered union. Exactly one member is non-nil, and
// each Task's terminal step progress is emitted before its final result.
type WorkerOutput struct {
	Progress *TaskProgress
	Result   *TaskResult
}

type taskReservation struct {
	assignment Assignment
	ctx        context.Context
	cancel     context.CancelFunc
}

// WorkerPool reserves at most size queued or active assignments.
type WorkerPool struct {
	size        int
	runner      runner.Runner
	logger      *slog.Logger
	work        chan *taskReservation
	outputs     chan WorkerOutput
	executeStep func(context.Context, adapters.Step) error

	mu           sync.Mutex
	reservations map[string]*taskReservation
	running      bool
	stopped      bool
}

func NewWorkerPool(size int, taskRunner runner.Runner, logger *slog.Logger) *WorkerPool {
	pool := &WorkerPool{
		size:         size,
		runner:       taskRunner,
		logger:       logger,
		work:         make(chan *taskReservation, size),
		outputs:      make(chan WorkerOutput, size),
		reservations: make(map[string]*taskReservation, size),
	}
	pool.executeStep = pool.runStep
	return pool
}

func (p *WorkerPool) Capacity() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.size - len(p.reservations)
}

func (p *WorkerPool) Outputs() <-chan WorkerOutput {
	return p.outputs
}

// Run owns and joins every worker before returning.
func (p *WorkerPool) Run(ctx context.Context) {
	p.mu.Lock()
	if p.running || p.stopped {
		p.mu.Unlock()
		return
	}
	p.running = true
	p.mu.Unlock()

	var workers sync.WaitGroup
	for range p.size {
		workers.Add(1)
		go func() {
			defer workers.Done()
			p.runWorker(ctx)
		}()
	}
	<-ctx.Done()
	p.stop()
	workers.Wait()
	p.releaseQueued(ctx)
}

func (p *WorkerPool) runWorker(runCtx context.Context) {
	for {
		select {
		case <-runCtx.Done():
			return
		case reservation := <-p.work:
			p.execute(runCtx, reservation)
		}
	}
}

func (p *WorkerPool) execute(runCtx context.Context, reservation *taskReservation) {
	err := reservation.ctx.Err()
	for _, step := range reservation.assignment.Steps {
		if err != nil {
			break
		}
		p.emitProgress(runCtx, TaskProgress{
			TaskID: reservation.assignment.TaskID, PlanHash: reservation.assignment.PlanHash,
			StepID: step.StepID, Attempt: 1, Ordinal: 1, State: TaskProgressRunning,
		})
		err = p.executeStep(reservation.ctx, step.Step)
		p.emitProgress(runCtx, TaskProgress{
			TaskID: reservation.assignment.TaskID, PlanHash: reservation.assignment.PlanHash,
			StepID: step.StepID, Attempt: 1, Ordinal: 2,
			State: progressStateFor(reservation.ctx, err),
		})
	}
	terminal := terminalFor(reservation.ctx, err)
	if err != nil && terminal == TaskTerminalFailed && p.logger != nil {
		p.logger.Error("agent: step failed", "task_id", reservation.assignment.TaskID, "error", err)
	}
	p.complete(runCtx, reservation, TaskResult{
		TaskID: reservation.assignment.TaskID, PlanHash: reservation.assignment.PlanHash, Terminal: terminal,
	})
}

func terminalFor(taskCtx context.Context, err error) TaskTerminal {
	if errors.Is(taskCtx.Err(), context.DeadlineExceeded) || errors.Is(err, context.DeadlineExceeded) {
		return TaskTerminalTimedOut
	}
	if taskCtx.Err() != nil || errors.Is(err, context.Canceled) {
		return TaskTerminalAborted
	}
	if err != nil {
		return TaskTerminalFailed
	}
	return TaskTerminalCompleted
}

func progressStateFor(taskCtx context.Context, err error) TaskProgressState {
	switch terminalFor(taskCtx, err) {
	case TaskTerminalCompleted:
		return TaskProgressCompleted
	case TaskTerminalFailed:
		return TaskProgressFailed
	case TaskTerminalTimedOut:
		return TaskProgressTimedOut
	case TaskTerminalAborted:
		return TaskProgressAborted
	default:
		return 0
	}
}

func (p *WorkerPool) emitProgress(runCtx context.Context, progress TaskProgress) {
	owned := progress
	owned.Chunk = append([]byte(nil), progress.Chunk...)
	select {
	case p.outputs <- WorkerOutput{Progress: &owned}:
	case <-runCtx.Done():
	}
}

func (p *WorkerPool) complete(runCtx context.Context, reservation *taskReservation, result TaskResult) {
	owned := result
	owned.Result = append([]byte(nil), result.Result...)
	select {
	case p.outputs <- WorkerOutput{Result: &owned}:
	case <-runCtx.Done():
	}
	p.mu.Lock()
	if p.reservations[result.TaskID] == reservation {
		delete(p.reservations, result.TaskID)
	}
	p.mu.Unlock()
	reservation.cancel()
}

func (p *WorkerPool) stop() {
	p.mu.Lock()
	p.stopped = true
	for _, reservation := range p.reservations {
		reservation.cancel()
	}
	p.mu.Unlock()
}

func (p *WorkerPool) releaseQueued(runCtx context.Context) {
	for {
		select {
		case reservation := <-p.work:
			p.complete(runCtx, reservation, TaskResult{
				TaskID: reservation.assignment.TaskID, PlanHash: reservation.assignment.PlanHash,
				Terminal: TaskTerminalAborted,
			})
		default:
			return
		}
	}
}

// Abort cancels queued or active work because ownership starts at Submit.
func (p *WorkerPool) Abort(ctx context.Context, taskID string) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent: abort context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return errs.New(errs.KindInternal, "agent: Controller sent an invalid task id")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if reservation := p.reservations[taskID]; reservation != nil {
		reservation.cancel()
	}
	return nil
}

// Submit reserves capacity without blocking the sole receive/control loop.
func (p *WorkerPool) Submit(ctx context.Context, assignment Assignment) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent: submit context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owned, err := validateAndCopyAssignment(assignment)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return errs.New(errs.KindStateConflict, "agent: worker pool is stopped")
	}
	if existing := p.reservations[owned.TaskID]; existing != nil {
		if existing.assignment.PlanHash != owned.PlanHash {
			return errs.New(errs.KindInternal, "agent: task id was reused with a different plan hash")
		}
		return nil
	}
	if len(p.reservations) >= p.size {
		return errs.New(errs.KindStateConflict, "agent: worker pool has no capacity")
	}
	taskCtx, cancel := context.WithTimeout(ctx, owned.Timeout)
	reservation := &taskReservation{assignment: owned, ctx: taskCtx, cancel: cancel}
	p.reservations[owned.TaskID] = reservation
	select {
	case p.work <- reservation:
		return nil
	default:
		delete(p.reservations, owned.TaskID)
		cancel()
		return errs.New(errs.KindInternal, "agent: worker queue reservation is inconsistent")
	}
}

func validateAndCopyAssignment(assignment Assignment) (Assignment, error) {
	if err := ids.Validate(ids.KindTask, assignment.TaskID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid task id")
	}
	if assignment.Timeout <= 0 {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid task timeout")
	}
	owned := Assignment{TaskID: assignment.TaskID, PlanHash: assignment.PlanHash, Timeout: assignment.Timeout}
	owned.Steps = make([]TaskStep, len(assignment.Steps))
	seen := make(map[string]struct{}, len(assignment.Steps))
	for index, step := range assignment.Steps {
		if err := ids.Validate(ids.KindStep, step.StepID); err != nil {
			return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid step id")
		}
		if _, duplicate := seen[step.StepID]; duplicate {
			return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent duplicate step ids")
		}
		seen[step.StepID] = struct{}{}
		params := make(map[string]string, len(step.Step.Params))
		for key, value := range step.Step.Params {
			params[key] = value
		}
		owned.Steps[index] = TaskStep{StepID: step.StepID, Step: adapters.Step{Op: step.Step.Op, Params: params}}
	}
	return owned, nil
}

// runStep fails closed until the corresponding typed procedure is accepted.
func (p *WorkerPool) runStep(_ context.Context, step adapters.Step) error {
	switch step.Op {
	case adapters.StepComposeUp, adapters.StepComposeDown, adapters.StepWaitHealthy,
		adapters.StepSwitchAlias, adapters.StepSwitchRoute, adapters.StepWriteFile,
		adapters.StepReload, adapters.StepJoinNetwork, adapters.StepProvisionNetwork,
		adapters.StepRunScript, adapters.StepExec, adapters.StepSQL, adapters.StepDump,
		adapters.StepRestore, adapters.StepEncrypt, adapters.StepUpload, adapters.StepVerify,
		adapters.StepPrune, adapters.StepAck:
		return errs.New(errs.KindNotImplemented, "agent: task procedure is not implemented")
	default:
		return errs.New(errs.KindInternal, "agent: Controller sent an unknown step operation")
	}
}
