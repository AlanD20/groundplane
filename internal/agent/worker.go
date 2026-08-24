package agent

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type PlanHash [sha256.Size]byte

type Assignment struct {
	AssignmentID string
	TaskID       string
	OperationID  string
	RetryOf      string
	Plan         *agentpb.ExecutionPlan
	Timeout      time.Duration
}

type TaskTerminal uint8

const (
	TaskTerminalCompleted TaskTerminal = iota + 1
	TaskTerminalFailed
	TaskTerminalTimedOut
	TaskTerminalAborted
)

type TaskResult struct {
	AssignmentID         string
	TaskID               string
	PlanHash             PlanHash
	Terminal             TaskTerminal
	ExitCode             int32
	Compose              *agentpb.ComposeTaskResult
	EnvironmentDirectory *agentpb.EnvironmentDirectoryTaskResult
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
	AssignmentID string
	TaskID       string
	PlanHash     PlanHash
	StepID       string
	Attempt      uint32
	Ordinal      uint64
	State        TaskProgressState
	Chunk        []byte
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
	size                   int
	volumeRoot             string
	runner                 runner.Runner
	logger                 *slog.Logger
	work                   chan *taskReservation
	outputs                chan WorkerOutput
	executeStep            func(context.Context, *agentpb.ExecutionStep) error
	compose                *ComposeRuntime
	environmentDirectories *EnvironmentDirectoryRuntime
	materializer           *MaterializationRuntime
	adapter                *AdapterRuntime
	materializations       *materializationInbox
	backupSecrets          *backupSecretSlotInbox

	mu           sync.Mutex
	reservations map[string]*taskReservation
	running      bool
	stopped      bool
}

func NewWorkerPool(size int, volumeRoot string, taskRunner runner.Runner, logger *slog.Logger) *WorkerPool {
	pool := &WorkerPool{
		size:             size,
		volumeRoot:       volumeRoot,
		runner:           taskRunner,
		logger:           logger,
		work:             make(chan *taskReservation, size),
		outputs:          make(chan WorkerOutput, size),
		reservations:     make(map[string]*taskReservation, size),
		materializations: newMaterializationInbox(),
		backupSecrets:    newBackupSecretSlotInbox(),
		adapter:          NewAdapterRuntime(taskRunner),
	}
	pool.executeStep = pool.runStep
	return pool
}

func NewWorkerPoolWithRuntimes(
	size int,
	volumeRoot string,
	taskRunner runner.Runner,
	logger *slog.Logger,
	compose *ComposeRuntime,
	environmentDirectories *EnvironmentDirectoryRuntime,
	materializer *MaterializationRuntime,
) *WorkerPool {
	pool := NewWorkerPool(size, volumeRoot, taskRunner, logger)
	pool.compose = compose
	pool.environmentDirectories = environmentDirectories
	pool.materializer = materializer
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
	failedStepID := ""
	diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE
	reconciliationRequired := false
	mutationAttempted := false
	projects := make(map[string]*agentpb.ObservedProject)
	environmentDirectoryTask := reservation.assignment.Plan.Operation ==
		agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	for _, step := range reservation.assignment.Plan.Steps {
		if err != nil {
			break
		}
		p.emitProgress(runCtx, TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID,
			TaskID:       reservation.assignment.TaskID, PlanHash: planHash,
			StepID: step.StepId, Attempt: 1, Ordinal: 1, State: TaskProgressRunning,
		})
		stepCtx, cancel := context.WithTimeout(
			reservation.ctx,
			time.Duration(step.TimeoutSeconds)*time.Second,
		)
		if step.GetMaterializeFile() != nil {
			var payload materializationPayload
			payload, err = p.materializations.Take(
				stepCtx,
				reservation.assignment.TaskID,
				step.GetStepId(),
			)
			if err == nil {
				if p.materializer == nil {
					err = closeMaterializationSource(payload.Source, "agent: materialization runtime is not configured")
				} else {
					err = p.materializer.executeStep(stepCtx, reservation.assignment, step, payload)
				}
			}
		} else if step.GetAdapterProcedure() != nil {
			var stepResult adapterStepResult
			stepResult, err = p.adapter.executeStep(stepCtx, step)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
		} else if (step.GetEnvironmentDirectoryCreate() != nil || step.GetEnvironmentDirectoryRemove() != nil ||
			step.GetManagedVolumeDirectoriesEnsure() != nil) && p.environmentDirectories != nil {
			var stepResult environmentDirectoryStepResult
			stepResult, err = p.environmentDirectories.executeStep(stepCtx, reservation.assignment, step)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
			if stepResult.FailedStepID != "" {
				failedStepID = stepResult.FailedStepID
			}
		} else if p.compose == nil {
			err = p.executeStep(stepCtx, step)
		} else {
			var stepResult composeStepResult
			stepResult, err = p.compose.executeStep(stepCtx, reservation.assignment, step)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
			if stepResult.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED {
				diagnostic = stepResult.Diagnostic
			}
			if stepResult.Observed != nil {
				projects[stepResult.Observed.GetProjectName()] = proto.Clone(stepResult.Observed).(*agentpb.ObservedProject)
			}
			reconciliationRequired = reconciliationRequired || stepResult.ReconciliationRequired
			mutationAttempted = mutationAttempted || stepResult.MutationAttempted
		}
		cancel()
		if err != nil {
			failedStepID = step.GetStepId()
		}
		p.emitProgress(runCtx, TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID,
			TaskID:       reservation.assignment.TaskID, PlanHash: planHash,
			StepID: step.StepId, Attempt: 1, Ordinal: 2,
			State: progressStateFor(reservation.ctx, err),
		})
	}
	terminal := terminalFor(reservation.ctx, err)
	if terminal != TaskTerminalCompleted && mutationAttempted {
		reconciliationRequired = true
	}
	if err != nil && terminal == TaskTerminalFailed && p.logger != nil {
		p.logger.Error("agent: step failed", "task_id", reservation.assignment.TaskID, "error", err)
	}
	result := TaskResult{
		AssignmentID: reservation.assignment.AssignmentID,
		TaskID:       reservation.assignment.TaskID, PlanHash: planHash, Terminal: terminal,
		ExitCode: exitCode,
	}
	if environmentDirectoryTask {
		result.EnvironmentDirectory = &agentpb.EnvironmentDirectoryTaskResult{FailedStepId: failedStepID}
	} else {
		result.Compose = composeTaskResult(projects, failedStepID, diagnostic, reconciliationRequired)
	}
	p.complete(runCtx, reservation, result)
}

func composeTaskResult(
	projects map[string]*agentpb.ObservedProject,
	failedStepID string,
	diagnostic agentpb.ComposeHelperDiagnostic,
	reconciliationRequired bool,
) *agentpb.ComposeTaskResult {
	names := make([]string, 0, len(projects))
	for name := range projects {
		names = append(names, name)
	}
	sort.Strings(names)
	result := &agentpb.ComposeTaskResult{
		FailedStepId: failedStepID, Diagnostic: diagnostic,
		ReconciliationRequired: reconciliationRequired,
	}
	for _, name := range names {
		result.Projects = append(result.Projects, projects[name])
	}
	return result
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
	p.backupSecrets.Release(result.TaskID)

	owned := result
	if result.Compose != nil {
		owned.Compose = proto.Clone(result.Compose).(*agentpb.ComposeTaskResult)
	}
	if result.EnvironmentDirectory != nil {
		owned.EnvironmentDirectory = proto.Clone(result.EnvironmentDirectory).(*agentpb.EnvironmentDirectoryTaskResult)
	}
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
	p.materializations.Release(result.TaskID)
}

func (p *WorkerPool) stop() {
	p.mu.Lock()
	p.stopped = true
	for _, reservation := range p.reservations {
		reservation.cancel()
	}
	p.mu.Unlock()
	p.backupSecrets.ReleaseAll()
}

func (p *WorkerPool) releaseQueued(runCtx context.Context) {
	for {
		select {
		case reservation := <-p.work:
			result := TaskResult{
				AssignmentID: reservation.assignment.AssignmentID,
				TaskID:       reservation.assignment.TaskID, PlanHash: hashForPlan(reservation.assignment.Plan),
				Terminal: TaskTerminalAborted,
			}
			if reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE {
				result.EnvironmentDirectory = &agentpb.EnvironmentDirectoryTaskResult{}
			} else {
				result.Compose = &agentpb.ComposeTaskResult{
					Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
				}
			}
			p.complete(runCtx, reservation, result)
		default:
			return
		}
	}
}

// Abort cancels queued or active work because ownership starts at Submit.
func (p *WorkerPool) Abort(ctx context.Context, taskID string, assignmentID string) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent: abort context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := ids.Validate(ids.KindTask, taskID); err != nil {
		return errs.New(errs.KindInternal, "agent: Controller sent an invalid task id")
	}
	if err := ids.Validate(ids.KindAssignment, assignmentID); err != nil {
		return errs.New(errs.KindInternal, "agent: Controller sent an invalid assignment id")
	}
	p.mu.Lock()
	reservation := p.reservations[taskID]
	if reservation == nil || reservation.assignment.AssignmentID != assignmentID {
		p.mu.Unlock()
		return errs.New(errs.KindStateConflict, "agent: Task abort assignment is not reserved")
	}
	reservation.cancel()
	p.mu.Unlock()
	p.materializations.Release(taskID)
	p.backupSecrets.Release(taskID)
	return nil
}

func (p *WorkerPool) AcceptMaterializationTransfer(
	ctx context.Context,
	transfer *agentpb.MaterializationTransfer,
) error {
	return p.materializations.Accept(ctx, transfer)
}

func (p *WorkerPool) AcceptBackupSecretSlotTransfer(
	ctx context.Context,
	transfer *agentpb.BackupSecretSlotTransfer,
) error {
	return p.backupSecrets.Accept(ctx, transfer)
}

// ConsumeBackupSecretSlot gives one owned transient slot to a callback exactly
// once and clears it immediately when the callback returns.
func (p *WorkerPool) ConsumeBackupSecretSlot(
	ctx context.Context,
	taskID string,
	assignmentID string,
	stepID string,
	purpose agentpb.BackupSecretSlotPurpose,
	consume func([]byte) error,
) error {
	return p.backupSecrets.Consume(ctx, taskID, assignmentID, stepID, purpose, consume)
}

// Submit reserves capacity without blocking the sole receive/control loop.
func (p *WorkerPool) Submit(ctx context.Context, assignment Assignment) error {
	if ctx == nil {
		return errs.New(errs.KindInternal, "agent: submit context is required")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	owned, err := validateAndCopyAssignment(assignment, p.volumeRoot)
	if err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return errs.New(errs.KindStateConflict, "agent: worker pool is stopped")
	}
	if existing := p.reservations[owned.TaskID]; existing != nil {
		if existing.assignment.AssignmentID != owned.AssignmentID ||
			hashForPlan(existing.assignment.Plan) != hashForPlan(owned.Plan) {
			return errs.New(errs.KindStateConflict, "agent: task id was reused with a different assignment")
		}
		return nil
	}
	if len(p.reservations) >= p.size {
		return errs.New(errs.KindStateConflict, "agent: worker pool has no capacity")
	}
	if err := p.materializations.Register(owned); err != nil {
		return err
	}
	if err := p.backupSecrets.Register(owned); err != nil {
		p.materializations.Release(owned.TaskID)
		return err
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
		p.materializations.Release(owned.TaskID)
		p.backupSecrets.Release(owned.TaskID)
		return errs.New(errs.KindInternal, "agent: worker queue reservation is inconsistent")
	}
}

func validateAndCopyAssignment(assignment Assignment, volumeRoot string) (Assignment, error) {
	if err := ids.Validate(ids.KindAssignment, assignment.AssignmentID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid assignment id")
	}
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
		if err := ids.Validate(
			ids.KindTask,
			assignment.RetryOf,
		); err != nil ||
			assignment.RetryOf == assignment.TaskID {
			return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid retry identity")
		}
	}
	plan, err := executionplan.Validate(assignment.Plan)
	if err != nil {
		return Assignment{}, errs.Wrap(errs.KindInternal, err)
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, volumeRoot); err != nil {
		return Assignment{}, errs.Wrap(errs.KindInternal, err)
	}
	for _, step := range plan.Steps {
		if time.Duration(step.TimeoutSeconds)*time.Second > assignment.Timeout {
			return Assignment{}, errs.New(errs.KindInternal, "agent: step timeout exceeds its task timeout")
		}
	}
	return Assignment{
		AssignmentID: assignment.AssignmentID,
		TaskID:       assignment.TaskID, OperationID: assignment.OperationID,
		RetryOf: assignment.RetryOf, Plan: plan, Timeout: assignment.Timeout,
	}, nil
}

// runStep fails closed until the corresponding typed procedure is accepted.
func (p *WorkerPool) runStep(_ context.Context, step *agentpb.ExecutionStep) error {
	switch step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply, *agentpb.ExecutionStep_ComposeStop,
		*agentpb.ExecutionStep_ComposeRemove, *agentpb.ExecutionStep_WaitHealthy,
		*agentpb.ExecutionStep_ManagedNetworkRemove,
		*agentpb.ExecutionStep_CaddyConfigApply,
		*agentpb.ExecutionStep_EnvironmentDirectoryCreate,
		*agentpb.ExecutionStep_EnvironmentDirectoryRemove,
		*agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure,
		*agentpb.ExecutionStep_MaterializeFile, *agentpb.ExecutionStep_AdapterProcedure,
		*agentpb.ExecutionStep_BackupSourceCapture:
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
