package agent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sort"
	"strconv"
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
	AssignmentID       string
	TaskID             string
	OperationID        string
	RetryOf            string
	Plan               *agentpb.ExecutionPlan
	ScriptArtifacts    *agentpb.ScriptAssignmentArtifacts
	ScriptCheckpoints  []*agentpb.ScriptExecutionCheckpoint
	AutomaticReconcile bool
	Deadline           time.Time
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
	Progress         *TaskProgress
	Result           *TaskResult
	BackupCheckpoint *agentpb.BackupCheckpointRequest
	ScriptCheckpoint *agentpb.ScriptCheckpointRequest
}

type taskReservation struct {
	assignment Assignment
	ctx        context.Context
	cancel     context.CancelFunc
}

type ScriptRuntime interface {
	ExecuteScript(
		context.Context,
		Assignment,
		*agentpb.ExecutionStep,
		func(context.Context, *agentpb.ScriptCheckpointRequest) error,
	) (int32, error)
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
	componentActions       ComponentActionRuntime
	hostResolution         HostResolutionRuntime
	scriptRuntime          ScriptRuntime
	materializations       *materializationInbox
	managedConfigs         *managedConfigInbox
	backupSecrets          *backupSecretSlotInbox
	backupCheckpoints      *backupCheckpointInbox
	scriptCheckpoints      *scriptCheckpointInbox

	mu           sync.Mutex
	reservations map[string]*taskReservation
	running      bool
	stopped      bool
}

func NewWorkerPool(size int, volumeRoot string, taskRunner runner.Runner, logger *slog.Logger) *WorkerPool {
	pool := &WorkerPool{
		size:              size,
		volumeRoot:        volumeRoot,
		runner:            taskRunner,
		logger:            logger,
		work:              make(chan *taskReservation, size),
		outputs:           make(chan WorkerOutput, size),
		reservations:      make(map[string]*taskReservation, size),
		materializations:  newMaterializationInbox(),
		managedConfigs:    newManagedConfigInbox(),
		backupSecrets:     newBackupSecretSlotInbox(),
		backupCheckpoints: newBackupCheckpointInbox(),
		scriptCheckpoints: newScriptCheckpointInbox(),
		adapter:           NewAdapterRuntime(taskRunner),
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

func (p *WorkerPool) execute(runCtx context.Context, reservation *taskReservation) {
	if isReleaseExecution(reservation.assignment.Plan) {
		p.executeRelease(runCtx, reservation)
		return
	}
	err := reservation.ctx.Err()
	planHash := hashForPlan(reservation.assignment.Plan)
	exitCode := int32(0)
	failedStepID := ""
	diagnostic := agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE
	reconciliationRequired := false
	mutationAttempted := false
	componentCompensated := false
	projects := make(map[string]*agentpb.ObservedProject)
	environmentDirectoryTask := usesEnvironmentDirectory(reservation.assignment.Plan)
	var environmentDirectoryResult *agentpb.EnvironmentDirectoryTaskResult
	var dnsResolverCandidateObservation *agentpb.DNSResolverObservationEvidence
	var dnsResolverRollbackObservation *agentpb.DNSResolverObservationEvidence
	var managedConfigStep *agentpb.ExecutionStep
	attemptedSteps := make([]*agentpb.ExecutionStep, 0, len(reservation.assignment.Plan.Steps))
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
		attemptedSteps = append(attemptedSteps, step)
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
			step.GetManagedVolumeDirectoriesEnsure() != nil || step.GetManagedVolumeDirectoryRemove() != nil) &&
			p.environmentDirectories != nil {
			var stepResult environmentDirectoryStepResult
			stepResult, err = p.environmentDirectories.executeStep(stepCtx, reservation.assignment, step)
			if stepResult.ExitCode != 0 {
				exitCode = stepResult.ExitCode
			}
			if stepResult.FailedStepID != "" {
				failedStepID = stepResult.FailedStepID
			}
			if step.GetManagedVolumeDirectoryRemove() != nil {
				environmentDirectoryResult = &agentpb.EnvironmentDirectoryTaskResult{
					NextCursor: append([]byte(nil), stepResult.NextCursor...), MutationCount: stepResult.MutationCount,
					Complete: stepResult.Complete, ResponseSha256: append([]byte(nil), stepResult.ResponseSHA256...),
				}
			}
		} else if step.GetBackupArtifactPrune() != nil {
			err = p.executeBackupArtifactPrune(stepCtx, reservation.assignment, step)
		} else if step.GetComponentApply() != nil {
			var payload ManagedConfigPayload
			if reservation.assignment.Plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY &&
				step.GetComponentApply().GetManagedConfigContent() {
				payload, err = p.managedConfigs.Take(stepCtx, reservation.assignment.TaskID, step.GetStepId())
			}
			if err == nil {
				if p.componentActions == nil {
					err = closeManagedConfigSource(payload.Source, "agent: Component action runtime is not configured")
				} else {
					var actionResult *ComponentActionResult
					actionResult, err = p.componentActions.ExecuteComponentAction(
						stepCtx, reservation.assignment, step, payload,
					)
					if actionResult != nil && actionResult.DNSResolverObservation != nil {
						dnsResolverCandidateObservation = actionResult.DNSResolverObservation
					}
					if step.GetComponentApply().GetManagedConfigContent() {
						if actionResult != nil {
							managedConfigStep = step
							mutationAttempted = true
						}
						if err == nil && !managedConfigPublicationProven(step, actionResult) {
							err = errs.New(
								errs.KindInternal,
								"agent: managed-config publication result is invalid",
							)
						}
					}
				}
			}
		} else if step.GetHostResolutionApply() != nil || step.GetHostResolutionRestore() != nil {
			mutationAttempted = true
			if p.hostResolution == nil {
				err = errs.New(errs.KindInternal, "agent: host resolution runtime is not configured")
			} else {
				err = p.hostResolution.ExecuteHostResolution(stepCtx, reservation.assignment, step)
			}
		} else if step.GetRunScript() != nil {
			if p.scriptRuntime == nil {
				err = errs.New(errs.KindInternal, "agent: Script runtime is not configured")
			} else {
				exitCode, err = p.scriptRuntime.ExecuteScript(
					stepCtx, reservation.assignment, step, p.CheckpointScript,
				)
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
	if reservation.assignment.Plan.GetOperation() == agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		if err == nil && managedConfigStep != nil {
			finalizeCtx, finalizeCancel := context.WithTimeout(context.Background(), 30*time.Second)
			var finalization ManagedConfigTransactionState
			finalization, err = p.componentActions.FinalizeManagedConfig(
				finalizeCtx, reservation.assignment, managedConfigStep, true,
			)
			finalizeCancel()
			if err == nil && !managedConfigCommitProven(managedConfigStep, finalization) {
				err = errs.New(errs.KindInternal, "agent: managed-config commit result is invalid")
			}
			if err != nil {
				failedStepID = managedConfigStep.GetStepId()
			}
		}
		if err != nil {
			var compensationErr error
			dnsResolverRollbackObservation, compensationErr = p.compensateComponentLifecycle(
				reservation.assignment, managedConfigStep, attemptedSteps,
			)
			if compensationErr != nil {
				err = errors.Join(err, compensationErr)
				reconciliationRequired = true
			} else if mutationAttempted {
				componentCompensated = true
			}
		}
	}
	terminal := terminalFor(reservation.ctx, err)
	if terminal != TaskTerminalCompleted && mutationAttempted && !componentCompensated {
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
		if environmentDirectoryResult != nil {
			result.EnvironmentDirectory.NextCursor = environmentDirectoryResult.NextCursor
			result.EnvironmentDirectory.MutationCount = environmentDirectoryResult.MutationCount
			result.EnvironmentDirectory.Complete = environmentDirectoryResult.Complete
			result.EnvironmentDirectory.ResponseSha256 = environmentDirectoryResult.ResponseSha256
		}
	} else {
		result.Compose = composeTaskResult(projects, failedStepID, diagnostic, reconciliationRequired)
		result.Compose.DnsResolverCandidateObservation = dnsResolverCandidateObservation
		result.Compose.DnsResolverRollbackObservation = dnsResolverRollbackObservation
	}
	p.complete(runCtx, reservation, result)
}

func (p *WorkerPool) compensateComponentLifecycle(
	assignment Assignment,
	managedConfigStep *agentpb.ExecutionStep,
	attemptedSteps []*agentpb.ExecutionStep,
) (*agentpb.DNSResolverObservationEvidence, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	attempted := make(map[string]bool, len(attemptedSteps))
	for _, step := range attemptedSteps {
		attempted[step.GetStepId()] = true
	}
	var compensationErr error
	mode := assignment.Plan.GetComponentLifecycleMode()
	if mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE {
		for index := len(assignment.Plan.Steps) - 1; index >= 0; index-- {
			step := assignment.Plan.Steps[index]
			switch {
			case step.GetHostResolutionApply() != nil && attempted[step.GetStepId()]:
				apply := step.GetHostResolutionApply()
				compensationErr = errors.Join(
					compensationErr,
					p.executeLifecycleCompensation(ctx, assignment, &agentpb.ExecutionStep{
						StepId:         step.GetStepId(),
						TimeoutSeconds: 30,
						Payload: &agentpb.ExecutionStep_HostResolutionRestore{
							HostResolutionRestore: &agentpb.HostResolutionRestore{
								ComponentId: apply.GetComponentId(),
								Generation:  apply.GetGeneration(),
							},
						},
					}),
				)
			case step.GetComposeApply() != nil && attempted[step.GetStepId()]:
				apply := step.GetComposeApply()
				compensationErr = errors.Join(
					compensationErr,
					p.executeLifecycleCompensation(ctx, assignment, &agentpb.ExecutionStep{
						StepId:         step.GetStepId(),
						TimeoutSeconds: 30,
						Payload: &agentpb.ExecutionStep_ComposeRemove{
							ComposeRemove: &agentpb.ComposeRemove{
								ArtifactId: apply.GetArtifactId(),
								ServiceIds: append([]string(nil), apply.GetServiceIds()...),
							},
						},
					}),
				)
			}
		}
	}
	if managedConfigStep != nil {
		rollbackState, rollbackErr := p.componentActions.FinalizeManagedConfig(
			ctx,
			assignment,
			managedConfigStep,
			false,
		)
		if rollbackErr == nil && !managedConfigRollbackProven(managedConfigStep, rollbackState) {
			rollbackErr = errs.New(errs.KindInternal, "agent: managed-config rollback result is invalid")
		}
		var serviceRollbackErr error
		if rollbackErr == nil && mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UPDATE {
			if apply := attemptedComponentComposeApply(assignment.Plan, attempted); apply != nil {
				rollbackArtifact := componentRollbackComposeArtifact(assignment.Plan, apply.GetArtifactId())
				if rollbackArtifact == nil {
					serviceRollbackErr = errs.New(errs.KindInternal, "agent: Component rollback artifact is missing")
				} else {
					rollbackAssignment, rollbackStep, deriveErr := componentRollbackComposeAssignment(
						assignment,
						apply,
						rollbackArtifact,
					)
					if deriveErr != nil {
						serviceRollbackErr = deriveErr
					} else {
						serviceRollbackErr = p.executeLifecycleCompensation(ctx, rollbackAssignment, rollbackStep)
					}
				}
			}
		}
		compensationErr = errors.Join(compensationErr, rollbackErr, serviceRollbackErr)
		if rollbackErr == nil && serviceRollbackErr == nil &&
			mode != agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE &&
			len(managedConfigStep.GetComponentApply().GetExpectedPreviousArtifactDigest()) != 0 {
			rollbackObservation, observationErr := p.observeManagedConfigRollback(
				ctx, assignment, managedConfigStep,
			)
			if observationErr != nil {
				compensationErr = errors.Join(compensationErr, observationErr)
			} else if compensationErr == nil {
				return rollbackObservation, nil
			}
		}
	}
	if mode == agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_DISABLE {
		mutationAttempted := false
		for _, step := range assignment.Plan.Steps {
			if remove := step.GetComposeRemove(); remove != nil && attempted[step.GetStepId()] {
				mutationAttempted = true
				if err := p.executeLifecycleCompensation(ctx, assignment, &agentpb.ExecutionStep{
					StepId:         step.GetStepId(),
					TimeoutSeconds: 30,
					Payload: &agentpb.ExecutionStep_ComposeApply{
						ComposeApply: &agentpb.ComposeApply{
							ArtifactId:     remove.GetArtifactId(),
							ServiceIds:     append([]string(nil), remove.GetServiceIds()...),
							ForceRecreate:  true,
							NoDependencies: true,
						},
					},
				}); err != nil {
					return nil, errors.Join(compensationErr, err)
				}
			}
		}
		for _, step := range assignment.Plan.Steps {
			if step.GetHostResolutionRestore() != nil && attempted[step.GetStepId()] {
				mutationAttempted = true
			}
		}
		if !mutationAttempted {
			return nil, compensationErr
		}
		rollbackObservation, observationErr := p.observeComponentRollback(ctx, assignment)
		if observationErr != nil {
			return nil, errors.Join(compensationErr, observationErr)
		}
		for _, step := range assignment.Plan.Steps {
			if restore := step.GetHostResolutionRestore(); restore != nil && attempted[step.GetStepId()] {
				if err := p.executeLifecycleCompensation(ctx, assignment, &agentpb.ExecutionStep{
					StepId:         step.GetStepId(),
					TimeoutSeconds: 30,
					Payload: &agentpb.ExecutionStep_HostResolutionApply{
						HostResolutionApply: &agentpb.HostResolutionApply{
							ComponentId: restore.GetComponentId(),
							Generation:  restore.GetGeneration(),
						},
					},
				}); err != nil {
					return nil, errors.Join(compensationErr, err)
				}
			}
		}
		return rollbackObservation, compensationErr
	}
	return nil, compensationErr
}

func attemptedComponentComposeApply(
	plan *agentpb.ExecutionPlan,
	attempted map[string]bool,
) *agentpb.ComposeApply {
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil && attempted[step.GetStepId()] {
			return apply
		}
	}
	return nil
}

func componentRollbackComposeArtifact(plan *agentpb.ExecutionPlan, candidateArtifactID string) *agentpb.ComposeArtifact {
	if plan == nil || len(plan.GetArtifacts()) != 2 {
		return nil
	}
	for _, artifact := range plan.GetArtifacts() {
		if artifact.GetArtifactId() != candidateArtifactID {
			return artifact
		}
	}
	return nil
}

func componentRollbackComposeAssignment(
	assignment Assignment,
	candidate *agentpb.ComposeApply,
	rollbackArtifact *agentpb.ComposeArtifact,
) (Assignment, *agentpb.ExecutionStep, error) {
	if assignment.Plan == nil || candidate == nil || rollbackArtifact == nil ||
		len(rollbackArtifact.GetServices()) != 1 {
		return Assignment{}, nil, errs.New(errs.KindInternal, "agent: Component rollback Compose input is invalid")
	}
	planID := ""
	renderGeneration := uint64(0)
	for _, label := range rollbackArtifact.GetServices()[0].GetExpectedLabels() {
		switch label.GetKey() {
		case "com.groundplane.plan-id":
			planID = label.GetValue()
		case "com.groundplane.render-generation":
			renderGeneration, _ = strconv.ParseUint(label.GetValue(), 10, 64)
		}
	}
	rollbackStep := &agentpb.ExecutionStep{
		StepId: ids.New(ids.KindStep), TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId:     rollbackArtifact.GetArtifactId(),
			ServiceIds:     append([]string(nil), candidate.GetServiceIds()...),
			ForceRecreate:  true,
			NoDependencies: true,
		}},
	}
	derived := proto.Clone(assignment.Plan).(*agentpb.ExecutionPlan)
	derived.PlanId = planID
	derived.RenderGeneration = renderGeneration
	derived.Operation = agentpb.PlanOperation_PLAN_OPERATION_RECONCILE
	derived.ComponentLifecycleMode = agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_UNSPECIFIED
	derived.ComponentRollbackObservation = nil
	derived.Artifacts = []*agentpb.ComposeArtifact{proto.Clone(rollbackArtifact).(*agentpb.ComposeArtifact)}
	derived.Steps = []*agentpb.ExecutionStep{rollbackStep}
	derived.PlanHash = nil
	sealed, err := executionplan.Seal(derived)
	if err != nil {
		return Assignment{}, nil, errs.Wrap(errs.KindInternal, err)
	}
	assignment.Plan = sealed
	return assignment, rollbackStep, nil
}

func (p *WorkerPool) observeComponentRollback(
	ctx context.Context,
	assignment Assignment,
) (*agentpb.DNSResolverObservationEvidence, error) {
	if assignment.Plan == nil || p.componentActions == nil {
		return nil, errs.New(errs.KindInternal, "agent: Component rollback observation runtime is unavailable")
	}
	action := assignment.Plan.GetComponentRollbackObservation()
	if action == nil || action.GetManagedConfigContent() || len(action.GetArtifactDigest()) != sha256.Size {
		return nil, errs.New(errs.KindInternal, "agent: Component rollback observation action is invalid")
	}
	step := &agentpb.ExecutionStep{
		TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComponentApply{
			ComponentApply: proto.Clone(action).(*agentpb.ComponentApply),
		},
	}
	if len(assignment.Plan.GetSteps()) != 0 {
		step.StepId = assignment.Plan.GetSteps()[0].GetStepId()
	}
	result, err := p.componentActions.ExecuteComponentAction(ctx, assignment, step, ManagedConfigPayload{})
	if err != nil {
		return nil, err
	}
	if result == nil || result.DNSResolverObservation == nil || result.ManagedConfig != nil {
		return nil, errs.New(errs.KindInternal, "agent: Component rollback observation result is invalid")
	}
	return result.DNSResolverObservation, nil
}

func managedConfigPublicationProven(step *agentpb.ExecutionStep, result *ComponentActionResult) bool {
	if step == nil || result == nil || result.ManagedConfig == nil {
		return false
	}
	action := step.GetComponentApply()
	return action != nil &&
		managedConfigFileStateMatches(result.ManagedConfig.Live, action.GetArtifactDigest()) &&
		managedConfigFileStateMatches(result.ManagedConfig.Previous, action.GetExpectedPreviousArtifactDigest())
}

func managedConfigCommitProven(step *agentpb.ExecutionStep, state ManagedConfigTransactionState) bool {
	if step == nil || step.GetComponentApply() == nil {
		return false
	}
	action := step.GetComponentApply()
	return managedConfigFileStateMatches(state.Live, action.GetArtifactDigest()) &&
		managedConfigFileStateMatches(state.Previous, action.GetExpectedPreviousArtifactDigest())
}

func managedConfigRollbackProven(step *agentpb.ExecutionStep, state ManagedConfigTransactionState) bool {
	if step == nil || step.GetComponentApply() == nil {
		return false
	}
	expected := step.GetComponentApply().GetExpectedPreviousArtifactDigest()
	return managedConfigFileStateMatches(state.Live, expected) &&
		managedConfigFileStateMatches(state.Previous, expected)
}

func managedConfigFileStateMatches(state ManagedConfigFileState, expected []byte) bool {
	if len(expected) == 0 {
		return !state.Present
	}
	return len(expected) == sha256.Size && state.Present && bytes.Equal(state.SHA256[:], expected)
}

func (p *WorkerPool) observeManagedConfigRollback(
	ctx context.Context,
	assignment Assignment,
	managedConfigStep *agentpb.ExecutionStep,
) (*agentpb.DNSResolverObservationEvidence, error) {
	managed := managedConfigStep.GetComponentApply()
	if assignment.Plan == nil || managed == nil ||
		len(managed.GetExpectedPreviousArtifactDigest()) != sha256.Size {
		return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation input is invalid")
	}
	var observationStep *agentpb.ExecutionStep
	for _, step := range assignment.Plan.GetSteps() {
		action := step.GetComponentApply()
		if action == nil || action.GetManagedConfigContent() ||
			action.GetComponentId() != managed.GetComponentId() || action.GetArtifactId() != managed.GetArtifactId() ||
			action.GetGeneration() != managed.GetGeneration() ||
			!bytes.Equal(action.GetDefinitionDigest(), managed.GetDefinitionDigest()) ||
			!bytes.Equal(action.GetCatalogDigest(), managed.GetCatalogDigest()) {
			continue
		}
		if observationStep != nil {
			return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation action is ambiguous")
		}
		observationStep = proto.Clone(step).(*agentpb.ExecutionStep)
	}
	if observationStep == nil {
		return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation action is missing")
	}
	if rollbackArtifact := componentRollbackComposeArtifact(
		assignment.Plan,
		componentComposeApplyArtifactID(assignment.Plan),
	); rollbackArtifact != nil {
		clonedPlan := proto.Clone(assignment.Plan).(*agentpb.ExecutionPlan)
		clonedPlan.Artifacts = []*agentpb.ComposeArtifact{
			proto.Clone(rollbackArtifact).(*agentpb.ComposeArtifact),
		}
		assignment.Plan = clonedPlan
	}
	observationStep.GetComponentApply().ArtifactDigest = append(
		[]byte(nil),
		managed.GetExpectedPreviousArtifactDigest()...,
	)
	observationStep.GetComponentApply().ArtifactId = managed.GetExpectedPreviousArtifactId()
	observationStep.GetComponentApply().Generation = managed.GetExpectedPreviousGeneration()
	result, err := p.componentActions.ExecuteComponentAction(
		ctx,
		assignment,
		observationStep,
		ManagedConfigPayload{},
	)
	if err != nil {
		return nil, err
	}
	if result == nil || result.DNSResolverObservation == nil || result.ManagedConfig != nil {
		return nil, errs.New(errs.KindInternal, "agent: managed-config rollback observation result is invalid")
	}
	return result.DNSResolverObservation, nil
}

func componentComposeApplyArtifactID(plan *agentpb.ExecutionPlan) string {
	for _, step := range plan.GetSteps() {
		if apply := step.GetComposeApply(); apply != nil {
			return apply.GetArtifactId()
		}
	}
	return ""
}

func (p *WorkerPool) executeLifecycleCompensation(
	ctx context.Context,
	assignment Assignment,
	step *agentpb.ExecutionStep,
) error {
	if step.GetHostResolutionApply() != nil || step.GetHostResolutionRestore() != nil {
		if p.hostResolution == nil {
			return errs.New(errs.KindInternal, "agent: host resolution compensation runtime is not configured")
		}
		return p.hostResolution.ExecuteHostResolution(ctx, assignment, step)
	}
	if p.compose != nil {
		_, err := p.compose.executeStep(ctx, assignment, step)
		return err
	}
	return p.executeStep(ctx, step)
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
	if plan == nil || (plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_DEPLOY &&
		plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK) {
		return false
	}
	for _, step := range plan.Steps {
		if step == nil {
			continue
		}
		switch step.Policy {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK:
			return true
		}
	}
	return false
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
	clearExecutionPlanSecrets(reservation.assignment.Plan)
	clearScriptArtifacts(reservation.assignment.ScriptArtifacts)
	p.mu.Lock()
	if p.reservations[result.TaskID] == reservation {
		delete(p.reservations, result.TaskID)
	}
	p.mu.Unlock()
	reservation.cancel()
	p.materializations.Release(result.TaskID)
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
				TaskID:       reservation.assignment.TaskID, PlanHash: hashForPlan(reservation.assignment.Plan),
				Terminal: TaskTerminalAborted,
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
	p.materializations.Release(taskID)
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
	transferred := false
	defer func() {
		if !transferred {
			clearExecutionPlanSecrets(owned.Plan)
			clearScriptArtifacts(owned.ScriptArtifacts)
		}
	}()
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
	reservation := &taskReservation{assignment: owned, ctx: taskCtx, cancel: cancel}
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

func clearExecutionPlanSecrets(plan *agentpb.ExecutionPlan) {
	if plan == nil {
		return
	}
	for _, step := range plan.Steps {
		procedure := step.GetAdapterProcedure()
		if procedure == nil {
			continue
		}
		clear(procedure.Password)
		procedure.Password = nil
	}
}

func validateAndCopyAssignment(assignment Assignment, volumeRoot string) (Assignment, error) {
	if err := ids.Validate(ids.KindAssignment, assignment.AssignmentID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid assignment id")
	}
	if err := ids.Validate(ids.KindTask, assignment.TaskID); err != nil {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid task id")
	}
	if assignment.Deadline.IsZero() {
		return Assignment{}, errs.New(errs.KindInternal, "agent: Controller sent an invalid task deadline")
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
	if assignment.AutomaticReconcile && plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY {
		return Assignment{}, errs.New(
			errs.KindInternal,
			"agent: automatic reconciliation assignment has an invalid plan",
		)
	}
	scriptArtifacts, err := validateAndCopyScriptArtifacts(plan, assignment.ScriptArtifacts)
	if err != nil {
		return Assignment{}, err
	}
	scriptCheckpoints := make([]*agentpb.ScriptExecutionCheckpoint, len(assignment.ScriptCheckpoints))
	if len(assignment.ScriptCheckpoints) != len(plan.ScriptBodyArtifacts) {
		clearScriptArtifacts(scriptArtifacts)
		return Assignment{}, errs.New(errs.KindInternal, "agent: Script checkpoint set is incomplete")
	}
	expectedCheckpoints := make(map[string]struct{}, len(plan.ScriptBodyArtifacts))
	for _, metadata := range plan.ScriptBodyArtifacts {
		if metadata == nil || metadata.ScriptExecutionId == "" {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.New(errs.KindInternal, "agent: Script execution metadata is invalid")
		}
		expectedCheckpoints[metadata.ScriptExecutionId] = struct{}{}
	}
	seenCheckpoints := make(map[string]struct{}, len(assignment.ScriptCheckpoints))
	for index, checkpoint := range assignment.ScriptCheckpoints {
		scriptCheckpoints[index], err = executionplan.ValidateScriptExecutionCheckpoint(checkpoint)
		if err != nil {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.Wrap(errs.KindInternal, err)
		}
		if _, expected := expectedCheckpoints[scriptCheckpoints[index].ScriptExecutionId]; !expected {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.New(errs.KindInternal, "agent: Script checkpoint does not belong to the execution plan")
		}
		if _, duplicate := seenCheckpoints[scriptCheckpoints[index].ScriptExecutionId]; duplicate {
			clearScriptArtifacts(scriptArtifacts)
			return Assignment{}, errs.New(errs.KindInternal, "agent: Script checkpoint set contains a duplicate execution")
		}
		seenCheckpoints[scriptCheckpoints[index].ScriptExecutionId] = struct{}{}
	}
	return Assignment{
		AssignmentID: assignment.AssignmentID,
		TaskID:       assignment.TaskID, OperationID: assignment.OperationID,
		RetryOf: assignment.RetryOf, Plan: plan, ScriptArtifacts: scriptArtifacts,
		ScriptCheckpoints: scriptCheckpoints, Deadline: assignment.Deadline,
		AutomaticReconcile: assignment.AutomaticReconcile,
	}, nil
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

func usesEnvironmentDirectory(plan *agentpb.ExecutionPlan) bool {
	return executionplan.UsesEnvironmentDirectoryResult(plan)
}
