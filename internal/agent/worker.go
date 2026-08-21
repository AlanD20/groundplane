package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type PlanHash [sha256.Size]byte

type Assignment struct {
	TaskID      string
	OperationID string
	RetryOf     string
	Plan        *agentpb.ExecutionPlan
	Timeout     time.Duration
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
	executeStep func(context.Context, *agentpb.ExecutionStep) error
	compose     *ComposeRuntime

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

func NewWorkerPoolWithCompose(
	size int,
	taskRunner runner.Runner,
	logger *slog.Logger,
	compose *ComposeRuntime,
) *WorkerPool {
	pool := NewWorkerPool(size, taskRunner, logger)
	pool.compose = compose
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
	planHash := hashForPlan(reservation.assignment.Plan)
	exitCode := int32(0)
	for _, step := range reservation.assignment.Plan.Steps {
		if err != nil {
			break
		}
		p.emitProgress(runCtx, TaskProgress{
			TaskID: reservation.assignment.TaskID, PlanHash: planHash,
			StepID: step.StepId, Attempt: 1, Ordinal: 1, State: TaskProgressRunning,
		})
		stepCtx, cancel := context.WithTimeout(
			reservation.ctx,
			time.Duration(step.TimeoutSeconds)*time.Second,
		)
		if p.compose == nil {
			err = p.executeStep(stepCtx, step)
		} else {
			var stepExitCode int32
			stepExitCode, err = p.compose.executeStep(stepCtx, reservation.assignment, step)
			if stepExitCode != 0 {
				exitCode = stepExitCode
			}
		}
		cancel()
		p.emitProgress(runCtx, TaskProgress{
			TaskID: reservation.assignment.TaskID, PlanHash: planHash,
			StepID: step.StepId, Attempt: 1, Ordinal: 2,
			State: progressStateFor(reservation.ctx, err),
		})
	}
	terminal := terminalFor(reservation.ctx, err)
	if err != nil && terminal == TaskTerminalFailed && p.logger != nil {
		p.logger.Error("agent: step failed", "task_id", reservation.assignment.TaskID, "error", err)
	}
	p.complete(runCtx, reservation, TaskResult{
		TaskID: reservation.assignment.TaskID, PlanHash: planHash, Terminal: terminal,
		ExitCode: exitCode,
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
				TaskID: reservation.assignment.TaskID, PlanHash: hashForPlan(reservation.assignment.Plan),
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
		if hashForPlan(existing.assignment.Plan) != hashForPlan(owned.Plan) {
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
	if err := ids.Validate(ids.KindOperation, assignment.OperationID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid operation id")
	}
	if assignment.RetryOf != "" {
		if err := ids.Validate(ids.KindTask, assignment.RetryOf); err != nil || assignment.RetryOf == assignment.TaskID {
			return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid retry identity")
		}
	}
	plan, err := executionplan.Validate(assignment.Plan)
	if err != nil {
		return Assignment{}, errs.Wrap(errs.KindInternal, err)
	}
	for _, step := range plan.Steps {
		if time.Duration(step.TimeoutSeconds)*time.Second > assignment.Timeout {
			return Assignment{}, errs.New(errs.KindInternal, "agent: step timeout exceeds its task timeout")
		}
	}
	return Assignment{
		TaskID: assignment.TaskID, OperationID: assignment.OperationID,
		RetryOf: assignment.RetryOf, Plan: plan, Timeout: assignment.Timeout,
	}, nil
}

// runStep fails closed until the corresponding typed procedure is accepted.
func (p *WorkerPool) runStep(_ context.Context, step *agentpb.ExecutionStep) error {
	switch step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply, *agentpb.ExecutionStep_ComposeStop,
		*agentpb.ExecutionStep_ComposeRemove, *agentpb.ExecutionStep_WaitHealthy:
		return errs.New(errs.KindNotImplemented, "agent: task procedure is not implemented")
	default:
		return errs.New(errs.KindInternal, "agent: Controller sent an unknown step payload")
	}
}

func hashForPlan(plan *agentpb.ExecutionPlan) PlanHash {
	var hash PlanHash
	if plan != nil {
		copy(hash[:], plan.PlanHash)
	}
	return hash
}
