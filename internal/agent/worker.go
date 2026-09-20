package agent

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/agent/backingadapter"
	backupsecrettransfer "github.com/AlanD20/groundplane/internal/agent/backupsecrettransfer"
	checkpointmailbox "github.com/AlanD20/groundplane/internal/agent/checkpointmailbox"
	componentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	directoryruntime "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	filematerialization "github.com/AlanD20/groundplane/internal/agent/materialization"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type taskReservation struct {
	assignment    taskassignment.Assignment
	ctx           context.Context
	cancel        context.CancelFunc
	eventsDurable bool
}

type ScriptRuntime interface {
	ExecuteScript(
		context.Context,
		taskassignment.Assignment,
		*agentpb.ExecutionStep,
		func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	) (int32, error)
	CompleteScriptWithoutStart(
		context.Context,
		taskassignment.Assignment,
		*agentpb.ExecutionStep,
		agentpb.ScriptOutcomeReason,
		func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	) error
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
	compose                *composeruntime.Runtime
	environmentDirectories *directoryruntime.Runtime
	materializer           *filematerialization.Runtime
	adapter                *backingadapter.Runtime
	componentActions       ComponentActionRuntime
	hostResolution         HostResolutionRuntime
	scriptRuntime          ScriptRuntime
	materializations       *filematerialization.Inbox
	managedConfigs         *componentaction.Inbox
	backupSecrets          *backupsecrettransfer.Inbox
	backupCheckpoints      *checkpointmailbox.BackupInbox
	scriptCheckpoints      *checkpointmailbox.ScriptInbox
	backingHookCheckpoints *backingHookCheckpointInbox
	volumeCheckpoints      *volumeCheckpointInbox
	taskEventAcks          *taskEventAckInbox

	mu           sync.Mutex
	reservations map[string]*taskReservation
	running      bool
	stopped      bool
}

func NewWorkerPool(size int, volumeRoot string, taskRunner runner.Runner, logger *slog.Logger) *WorkerPool {
	pool := &WorkerPool{
		size:                   size,
		volumeRoot:             volumeRoot,
		runner:                 taskRunner,
		logger:                 logger,
		work:                   make(chan *taskReservation, size),
		outputs:                make(chan WorkerOutput, size),
		reservations:           make(map[string]*taskReservation, size),
		materializations:       filematerialization.NewInbox(),
		managedConfigs:         componentaction.NewInbox(),
		backupSecrets:          backupsecrettransfer.New(),
		backupCheckpoints:      checkpointmailbox.NewBackupInbox(),
		scriptCheckpoints:      checkpointmailbox.NewScriptInbox(),
		backingHookCheckpoints: newBackingHookCheckpointInbox(),
		volumeCheckpoints:      &volumeCheckpointInbox{pending: make(map[string]*volumeCheckpointWaiter)},
		taskEventAcks:          &taskEventAckInbox{receipts: make(map[taskEventAckKey]*taskEventReceipt)},
		adapter:                backingadapter.New(taskRunner),
	}
	pool.executeStep = pool.runStep
	return pool
}

func NewWorkerPoolWithRuntimes(
	size int,
	volumeRoot string,
	taskRunner runner.Runner,
	logger *slog.Logger,
	compose *composeruntime.Runtime,
	environmentDirectories *directoryruntime.Runtime,
	materializer *filematerialization.Runtime,
) *WorkerPool {
	pool := NewWorkerPool(size, volumeRoot, taskRunner, logger)
	pool.compose = compose
	pool.environmentDirectories = environmentDirectories
	pool.materializer = materializer
	return pool
}

func (p *WorkerPool) SetComponentActionRuntime(runtime ComponentActionRuntime) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.componentActions = runtime
}

func (p *WorkerPool) SetHostResolutionRuntime(runtime HostResolutionRuntime) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.hostResolution = runtime
}

func (p *WorkerPool) SetScriptRuntime(runtime ScriptRuntime) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.scriptRuntime = runtime
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

func isReleaseExecution(plan *agentpb.ExecutionPlan) bool {
	if plan == nil || plan.GetCandidateReleaseProcedure() == nil {
		return false
	}
	switch plan.GetOperation() {
	case agentpb.PlanOperation_PLAN_OPERATION_DEPLOY,
		agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK,
		agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY:
		return true
	default:
		return false
	}
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

func (p *WorkerPool) complete(runCtx context.Context, reservation *taskReservation, result TaskResult) {
	p.backupSecrets.Release(result.TaskID)
	p.materializations.Retire(result.TaskID)

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
	clearExecutionPlanSecrets(reservation.assignment.Plan)
	taskassignment.ClearScriptArtifacts(reservation.assignment.ScriptArtifacts)
	p.mu.Lock()
	if p.reservations[result.TaskID] == reservation {
		delete(p.reservations, result.TaskID)
	}
	p.mu.Unlock()
	reservation.cancel()
	p.managedConfigs.Release(result.TaskID)
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
				TaskID:       reservation.assignment.TaskID, PlanHash: taskassignment.PlanDigest(reservation.assignment.Plan),
				Terminal: TaskTerminalAborted, ExecutionEpoch: reservation.assignment.ExecutionEpoch,
				ReleaseRecoveryRecordSHA256: append([]byte(nil), reservation.assignment.ReleaseRecoveryRecordSHA256...),
			}
			if usesEnvironmentDirectory(reservation.assignment.Plan) {
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

func isEnvironmentDirectoryTask(plan *agentpb.ExecutionPlan) bool {
	if plan == nil {
		return false
	}
	for _, step := range plan.GetSteps() {
		if step.GetEnvironmentDirectoryCreate() != nil || step.GetEnvironmentDirectoryRemove() != nil {
			return true
		}
	}
	return false
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
	p.materializations.Retire(taskID)
	p.managedConfigs.Release(taskID)
	p.backupSecrets.Release(taskID)
	return nil
}

func (p *WorkerPool) AcceptMaterializationTransfer(
	ctx context.Context,
	transfer *agentpb.MaterializationTransfer,
) error {
	return p.materializations.Accept(ctx, transfer)
}

func (p *WorkerPool) AcceptManagedConfigTransfer(
	ctx context.Context,
	transfer *agentpb.ManagedConfigTransfer,
) error {
	return p.managedConfigs.Accept(ctx, transfer)
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
func (p *WorkerPool) Submit(ctx context.Context, assignment taskassignment.Assignment) error {
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
	transferred := false
	defer func() {
		if !transferred {
			clearExecutionPlanSecrets(owned.Plan)
			taskassignment.ClearScriptArtifacts(owned.ScriptArtifacts)
		}
	}()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped {
		return errs.New(errs.KindStateConflict, "agent: worker pool is stopped")
	}
	if existing := p.reservations[owned.TaskID]; existing != nil {
		if existing.assignment.AssignmentID != owned.AssignmentID ||
			existing.assignment.ExecutionEpoch != owned.ExecutionEpoch ||
			taskassignment.PlanDigest(existing.assignment.Plan) != taskassignment.PlanDigest(owned.Plan) {
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
	if err := p.managedConfigs.Register(owned); err != nil {
		p.materializations.Release(owned.TaskID)
		return err
	}
	if err := p.backupSecrets.Register(owned); err != nil {
		p.materializations.Release(owned.TaskID)
		p.managedConfigs.Release(owned.TaskID)
		return err
	}
	taskCtx, cancel := context.WithDeadline(ctx, owned.Deadline)
	reservation := &taskReservation{assignment: owned, ctx: taskCtx, cancel: cancel, eventsDurable: true}
	p.reservations[owned.TaskID] = reservation
	select {
	case p.work <- reservation:
		transferred = true
		return nil
	default:
		delete(p.reservations, owned.TaskID)
		cancel()
		p.materializations.Release(owned.TaskID)
		p.managedConfigs.Release(owned.TaskID)
		p.backupSecrets.Release(owned.TaskID)
		return errs.New(errs.KindInternal, "agent: worker queue reservation is inconsistent")
	}
}

// runStep fails closed until the corresponding typed procedure is accepted.
func (p *WorkerPool) runStep(_ context.Context, step *agentpb.ExecutionStep) error {
	switch step.Payload.(type) {
	case *agentpb.ExecutionStep_ComposeApply, *agentpb.ExecutionStep_ComposeStop,
		*agentpb.ExecutionStep_ComposeRemove, *agentpb.ExecutionStep_WaitHealthy,
		*agentpb.ExecutionStep_ManagedNetworkRemove,
		*agentpb.ExecutionStep_ManagedVolumeRemove,
		*agentpb.ExecutionStep_ComponentApply,
		*agentpb.ExecutionStep_HostResolutionApply,
		*agentpb.ExecutionStep_HostResolutionRestore,
		*agentpb.ExecutionStep_EnvironmentDirectoryCreate,
		*agentpb.ExecutionStep_EnvironmentDirectoryRemove,
		*agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure,
		*agentpb.ExecutionStep_ManagedVolumeDirectoryRemove,
		*agentpb.ExecutionStep_MaterializeFile, *agentpb.ExecutionStep_AdapterProcedure,
		*agentpb.ExecutionStep_BackingHookProcedure,
		*agentpb.ExecutionStep_BackupSourceCapture:
		return errs.New(errs.KindNotImplemented, "agent: task procedure is not implemented")
	default:
		return errs.New(errs.KindInternal, "agent: Controller sent an unknown step payload")
	}
}
