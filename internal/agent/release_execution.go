package agent

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"sort"
	"time"

	composeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseExecutionState struct {
	err                  error
	exitCode             int32
	failedStepID         string
	diagnostic           agentpb.ComposeHelperDiagnostic
	reconciliation       bool
	projects             map[string]*agentpb.ObservedProject
	evidence             map[string]*agentpb.ServiceProxyEvidence
	recreate             map[string]*agentpb.ServiceRecreateEvidence
	rolling              map[string]*agentpb.RollingReleaseEvidence
	absence              map[string]*agentpb.CandidateAbsenceEvidence
	componentRestoration *agentpb.ComponentRestorationEvidence
	serviceAbsence       *agentpb.ServiceAbsenceEvidence
	probeRequired        map[string]bool
	completed            map[string]struct{}
	recoveryClosed       bool
	restored             bool

	componentMutationAttempted bool
}

func (p *WorkerPool) executeRelease(runCtx context.Context, reservation *taskReservation) {
	state := &releaseExecutionState{
		diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		projects:   map[string]*agentpb.ObservedProject{}, evidence: map[string]*agentpb.ServiceProxyEvidence{}, recreate: map[string]*agentpb.ServiceRecreateEvidence{},
		probeRequired: map[string]bool{}, completed: map[string]struct{}{},
		rolling: map[string]*agentpb.RollingReleaseEvidence{},
	}
	if p.compose == nil {
		state.err = errs.New(errs.KindInternal, "agent: release Compose runtime is not configured")
	} else if reservation.assignment.ExecutionMode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		p.runReleaseRecovery(runCtx, reservation, state)
	} else {
		if reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY {
			p.runBlueprintReleaseForward(runCtx, reservation, state)
		} else {
			p.runReleasePhase(runCtx, reservation, state,
				agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK)
			if state.err == nil {
				p.runReleaseForward(runCtx, reservation, state)
			}
		}
		if state.err != nil {
			if hasConfigurationRestoration(reservation.assignment.Plan) {
				// Only the durable recovery record can select a possibly partial
				// file write. It restores files before any native compensation.
				state.reconciliation = true
			} else {
				original, originalStep := state.err, state.failedStepID
				originalExitCode, originalDiagnostic := state.exitCode, state.diagnostic
				compensationApplicable := false
				compensated := true
				for index := len(reservation.assignment.Plan.Steps) - 1; index >= 0; index-- {
					step := reservation.assignment.Plan.Steps[index]
					if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE || !releaseCompensationEnabled(step) {
						continue
					}
					if _, ok := state.completed[step.PrerequisiteStepId]; !ok {
						continue
					}
					compensationApplicable = true
					state.err = nil
					p.runReleaseStep(runCtx, reservation, step, state)
					if state.err != nil || !state.restored {
						state.reconciliation = true
						compensated = false
						break
					}
				}
				if compensationApplicable && compensated {
					state.reconciliation = false
				}
				state.err, state.failedStepID = original, originalStep
				state.exitCode, state.diagnostic = originalExitCode, originalDiagnostic
				if compensationApplicable && compensated && reservation.ctx.Err() == nil && !errors.Is(original, context.DeadlineExceeded) {
					p.runReleaseFailureHooks(runCtx, reservation, state)
					state.err, state.failedStepID = original, originalStep
					state.exitCode, state.diagnostic = originalExitCode, originalDiagnostic
				}
			}
		}
	}
	terminal := terminalFor(reservation.ctx, state.err)
	if terminal != TaskTerminalCompleted &&
		reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
		(reservation.assignment.Plan.GetCandidateReleaseProcedure() != nil ||
			reservation.assignment.Plan.GetComponentRecoveryProcedure() != nil ||
			reservation.assignment.Plan.GetServiceAbsenceRecoveryProcedure() != nil) &&
		!state.restored {
		// Even a pre-hook failure needs an exact predecessor/absence probe.
		// Reporting terminal failure first can release Script sources that the
		// durable recovery assignment still owns.
		state.reconciliation = true
	}
	if terminal != TaskTerminalCompleted && state.componentMutationAttempted {
		// Workload restoration cannot prove restoration of an Environment
		// Component action, including an earlier successful action in this Task.
		state.reconciliation = true
	}
	if terminal != TaskTerminalCompleted && !state.reconciliation && reservation.assignment.RetryOf != "" &&
		!state.recoveryClosed {
		state.reconciliation = true
	}
	result := TaskResult{
		AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID,
		PlanHash: taskassignment.PlanDigest(reservation.assignment.Plan), Terminal: terminal, ExitCode: state.exitCode,
		ExecutionEpoch:                reservation.assignment.ExecutionEpoch,
		ReleaseRecoveryRecordSHA256:   append([]byte(nil), reservation.assignment.ReleaseRecoveryRecordSHA256...),
		ComponentRecoveryRecordSHA256: append([]byte(nil), reservation.assignment.ComponentRecoveryRecordSHA256...),
		Compose:                       releaseComposeTaskResult(state),
	}
	if state.err != nil && p.logger != nil {
		p.logger.Error("agent: release step failed", "task_id", reservation.assignment.TaskID, "error", state.err)
	}
	p.complete(runCtx, reservation, result)
}

// Blueprint seals one global order, including setup before pre-hooks. Standalone
// Releases retain their per-candidate post-hook scheduling in runReleaseForward.
func (p *WorkerPool) runBlueprintReleaseForward(
	ctx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	stepID, err := p.compose.PreflightManagedComponentTeardown(ctx, reservation.assignment.Plan)
	if err != nil {
		state.err, state.failedStepID, state.reconciliation = err, stepID, true
		return
	}
	for _, step := range reservation.assignment.Plan.Steps {
		switch step.Policy {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK:
			p.runReleaseStep(ctx, reservation, step, state)
			if state.err != nil {
				return
			}
		}
	}
}

func (p *WorkerPool) runReleaseForward(
	runCtx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	for _, step := range reservation.assignment.Plan.Steps {
		if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
			continue
		}
		p.runReleaseStep(runCtx, reservation, step, state)
		if state.err != nil {
			return
		}
		p.runEligibleReleasePostHooks(runCtx, reservation, state)
		if state.err != nil {
			return
		}
	}
	for _, step := range reservation.assignment.Plan.Steps {
		if step.Policy == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK {
			if _, completed := state.completed[step.StepId]; !completed {
				state.err = errs.New(errs.KindInternal, "agent: release post-hook prerequisite was not completed")
				state.failedStepID = step.StepId
				return
			}
		}
	}
}

func (p *WorkerPool) runEligibleReleasePostHooks(
	runCtx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	for {
		progressed := false
		for _, step := range reservation.assignment.Plan.Steps {
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK {
				continue
			}
			if _, completed := state.completed[step.StepId]; completed {
				continue
			}
			if step.PrerequisiteStepId == "" {
				state.err = errs.New(errs.KindInternal, "agent: release post-hook prerequisite is missing")
				state.failedStepID = step.StepId
				return
			}
			if _, ready := state.completed[step.PrerequisiteStepId]; !ready {
				continue
			}
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				return
			}
			progressed = true
		}
		if !progressed {
			return
		}
	}
}

func (p *WorkerPool) runReleasePhase(
	runCtx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
	policy agentpb.ExecutionStepPolicy,
) {
	for _, step := range reservation.assignment.Plan.Steps {
		if step.Policy != policy {
			continue
		}
		p.runReleaseStep(runCtx, reservation, step, state)
		if state.err != nil {
			return
		}
	}
}

func (p *WorkerPool) runReleaseFailureHooks(
	runCtx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	for _, step := range reservation.assignment.Plan.Steps {
		if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK {
			continue
		}
		state.err = nil
		p.runReleaseStep(runCtx, reservation, step, state)
		if state.err != nil {
			return
		}
	}
}

func (p *WorkerPool) runReleaseStep(
	runCtx context.Context,
	reservation *taskReservation,
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) {
	state.restored = false
	planHash := taskassignment.PlanDigest(reservation.assignment.Plan)
	running := TaskProgress{AssignmentID: reservation.assignment.AssignmentID,
		TaskID: reservation.assignment.TaskID, PlanHash: planHash, StepID: step.StepId,
		ExecutionEpoch: reservation.assignment.ExecutionEpoch, Ordinal: 1, State: TaskProgressRunning}
	rolling := rollingStepMember(reservation.assignment.Plan, step.GetStepId())
	var rollingBudget *agentpb.RollingStepBudget
	ordinalBase := uint64(1)
	if rolling != nil {
		var err error
		var receipt *agentpb.TaskEventAck
		receipt, err = p.beginRollingStep(reservation, step, rolling, running)
		if err != nil {
			state.err, state.failedStepID, state.reconciliation = err, step.GetStepId(), true
			return
		}
		rollingBudget, ordinalBase = receipt.GetRollingBudget(), receipt.GetEventOrdinalBase()
	} else if reservation.eventsDurable {
		receipt, err := p.emitProgressForReceipt(reservation.ctx, running)
		if err != nil {
			state.err, state.failedStepID = err, step.GetStepId()
			return
		}
		ordinalBase = receipt.GetEventOrdinalBase()
	} else {
		p.emitProgress(runCtx, running)
	}
	deadline := time.Now().Add(time.Duration(step.TimeoutSeconds) * time.Second)
	if rollingBudget != nil && rollingBudget.GetDeadline().AsTime().Before(deadline) {
		deadline = rollingBudget.GetDeadline().AsTime()
	}
	stepCtx, cancel := context.WithDeadline(reservation.ctx, deadline)
	reportHealth, ordinal := p.healthWaitEvents(stepCtx, running, ordinalBase)
	var result composeruntime.StepResult
	var err error
	if step.GetRollingReplicaDrain() != nil {
		err = waitRollingDrain(stepCtx, rollingBudget)
	} else if step.GetRunScript() != nil {
		if p.scriptRuntime == nil {
			err = errs.New(errs.KindInternal, "agent: Script runtime is not configured")
		} else {
			execute, reason := releaseFailureHookExecution(step, state)
			if execute {
				result.ExitCode, err = p.scriptRuntime.ExecuteScript(
					stepCtx, reservation.assignment, step, p.CheckpointScript,
				)
			} else {
				if reason == agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE {
					state.reconciliation = true
				}
				err = p.scriptRuntime.CompleteScriptWithoutStart(
					stepCtx, reservation.assignment, step, reason, p.CheckpointScript,
				)
				if err != nil {
					state.reconciliation = true
				}
			}
		}
	} else if step.GetComponentApply() != nil {
		result, err = p.executeReleaseComponent(stepCtx, reservation.assignment, step)
		state.componentMutationAttempted = state.componentMutationAttempted || result.MutationAttempted
	} else if reservation.assignment.ExecutionMode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY &&
		executionplan.ConfigurationFilePair(reservation.assignment.Plan, step.GetStepId()) != nil {
		result, err = p.executeReleaseConfigurationStep(stepCtx, reservation.assignment, step)
	} else if step.GetMaterializeFile() != nil || step.GetAdapterProcedure() != nil ||
		step.GetManagedVolumeDirectoriesEnsure() != nil || step.GetEnvironmentDirectoryCreate() != nil {
		result, err = p.executeReleaseSetup(stepCtx, reservation.assignment, step)
	} else {
		result, err = p.compose.ExecuteStep(stepCtx, reservation.assignment, step, reportHealth)
	}
	stepContextErr := stepCtx.Err()
	cancel()
	err = errors.Join(err, stepContextErr)
	if result.ExitCode != 0 {
		state.exitCode = result.ExitCode
	}
	if result.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED {
		state.diagnostic = result.Diagnostic
	}
	if result.Observed != nil {
		state.projects[result.Observed.ProjectName] = proto.Clone(result.Observed).(*agentpb.ObservedProject)
	}
	if result.ProxyEvidence != nil {
		state.evidence[result.ProxyEvidence.ServiceId] = proto.Clone(result.ProxyEvidence).(*agentpb.ServiceProxyEvidence)
		delete(state.rolling, result.ProxyEvidence.ServiceId)
	}
	if result.RecreateEvidence != nil {
		state.recreate[result.RecreateEvidence.ServiceId] = proto.Clone(result.RecreateEvidence).(*agentpb.ServiceRecreateEvidence)
		delete(state.rolling, result.RecreateEvidence.ServiceId)
	}
	if result.RollingEvidence != nil {
		state.rolling[result.RollingEvidence.GetServiceId()] = proto.CloneOf(result.RollingEvidence)
		delete(state.evidence, result.RollingEvidence.GetServiceId())
		delete(state.recreate, result.RollingEvidence.GetServiceId())
	}
	if result.ComponentRestorationEvidence != nil {
		state.componentRestoration = proto.CloneOf(result.ComponentRestorationEvidence)
	}
	if result.ServiceAbsenceEvidence != nil {
		state.serviceAbsence = proto.CloneOf(result.ServiceAbsenceEvidence)
	}
	err = state.recordAbsenceResult(&result, err)
	pair := executionplan.ConfigurationFilePair(reservation.assignment.Plan, step.GetStepId())
	if err == nil && step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		state.restored = pair != nil ||
			composeruntime.ReleaseRestorationEvidenceProven(reservation.assignment, step, result) ||
			composeruntime.ComponentRestorationEvidenceProven(reservation.assignment, result) ||
			composeruntime.ServiceAbsenceEvidenceProven(reservation.assignment, result)
		if !state.restored {
			err = errs.New(errs.KindInternal, "agent: release compensation returned no exact restoration proof")
			result.ReconciliationRequired = true
		}
	}
	if err == nil && step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
		var required bool
		if pair != nil {
			required = result.RestorationRequired
		} else {
			if step.GetComponentRestorationProbe() != nil {
				required, err = composeruntime.ComponentRestorationProbeStatus(reservation.assignment, result)
			} else if step.GetServiceAbsenceProbe() != nil {
				required, err = composeruntime.ServiceAbsenceProbeStatus(reservation.assignment, result)
			} else {
				required, err = composeruntime.ReleaseProbeEvidenceStatus(reservation.assignment, step, result)
			}
		}
		if err == nil {
			state.probeRequired[step.GetStepId()] = required
		} else {
			result.ReconciliationRequired = true
		}
	}
	progress := progressStateFor(reservation.ctx, err)
	state.reconciliation = state.reconciliation || result.ReconciliationRequired
	if err != nil {
		if p.logger != nil {
			p.logger.Error(
				"agent: release execution step failed",
				"task_id", reservation.assignment.TaskID,
				"step_id", step.StepId,
				"policy", step.Policy.String(),
				"error", err,
			)
		}
		state.err, state.failedStepID = err, step.StepId
	} else {
		state.completed[step.StepId] = struct{}{}
	}
	completedProgress := TaskProgress{AssignmentID: reservation.assignment.AssignmentID,
		TaskID: reservation.assignment.TaskID, PlanHash: planHash, StepID: step.StepId,
		ExecutionEpoch: reservation.assignment.ExecutionEpoch, Ordinal: *ordinal + 1, State: progress,
		Failure: taskprogress.CaptureFailure(err, result.ExitCode, result.Diagnostic)}
	if reservation.eventsDurable && (rolling != nil ||
		reservation.assignment.ExecutionMode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY) {
		if progressErr := p.emitProgressAndWait(reservation.ctx, completedProgress); progressErr != nil &&
			state.err == nil {
			state.err, state.failedStepID, state.reconciliation = progressErr, step.GetStepId(), true
		}
	} else {
		p.emitProgress(runCtx, completedProgress)
	}
}

func releaseFailureHookExecution(
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) (bool, agentpb.ScriptOutcomeReason) {
	if step.GetPolicy() != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK {
		return true, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_UNSPECIFIED
	}
	run := step.GetRunScript()
	if run == nil || run.GetServiceId() == "" || run.GetReleaseId() == "" {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	proxy, hasProxy := state.evidence[run.GetServiceId()]
	recreate, hasRecreate := state.recreate[run.GetServiceId()]
	rolling, hasRolling := state.rolling[run.GetServiceId()]
	if !hasProxy && !hasRecreate && !hasRolling {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE
	}
	if hasProxy && hasRecreate || hasRolling && (hasProxy || hasRecreate) ||
		hasProxy && proxy == nil || hasRecreate && recreate == nil || hasRolling && rolling == nil {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	servingReleaseID := ""
	if hasProxy {
		servingReleaseID = proxy.GetReleaseId()
	} else if hasRecreate {
		servingReleaseID = recreate.GetReleaseId()
	} else {
		servingReleaseID = rolling.GetReleaseId()
	}
	if servingReleaseID == "" || servingReleaseID != run.GetReleaseId() {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	return true, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_UNSPECIFIED
}

func releaseComposeTaskResult(state *releaseExecutionState) *agentpb.ComposeTaskResult {
	result := composeTaskResult(state.projects, state.failedStepID, state.diagnostic, state.reconciliation)
	result.Failure = taskprogress.CaptureFailure(state.err, state.exitCode, state.diagnostic)
	serviceIDs := make([]string, 0, len(state.evidence))
	for serviceID := range state.evidence {
		serviceIDs = append(serviceIDs, serviceID)
	}
	sort.Strings(serviceIDs)
	for _, serviceID := range serviceIDs {
		evidence := proto.CloneOf(state.evidence[serviceID])
		if state.recoveryClosed {
			// Exact prior-state probes can make compensation a no-op. Once
			// the complete recovery closes, they prove the same restored state.
			evidence.Compensated = true
		}
		result.ProxyEvidence = append(result.ProxyEvidence, evidence)
	}
	recreateIDs := make([]string, 0, len(state.recreate))
	for serviceID := range state.recreate {
		recreateIDs = append(recreateIDs, serviceID)
	}
	sort.Strings(recreateIDs)
	for _, serviceID := range recreateIDs {
		result.RecreateEvidence = append(result.RecreateEvidence, state.recreate[serviceID])
	}
	result.CandidateAbsenceEvidence = state.aggregateAbsenceEvidence()
	rollingIDs := make([]string, 0, len(state.rolling))
	for serviceID := range state.rolling {
		rollingIDs = append(rollingIDs, serviceID)
	}
	sort.Strings(rollingIDs)
	for _, serviceID := range rollingIDs {
		result.RollingEvidence = append(result.RollingEvidence, proto.CloneOf(state.rolling[serviceID]))
	}
	result.ComponentRestorationEvidence = state.componentRestoration
	result.ServiceAbsenceEvidence = state.serviceAbsence
	return result
}

func releaseCompensationEnabled(step *agentpb.ExecutionStep) bool {
	if value := step.GetServiceProxyCompensate(); value != nil {
		return value.Enabled
	}
	if value := step.GetServiceRecreateCompensate(); value != nil {
		return value.Enabled
	}
	if step.GetCandidateRestorationCompensate() != nil {
		return true
	}
	if step.GetComponentRestorationCompensate() != nil {
		return true
	}
	if procedure := step.GetComposeRemove(); procedure != nil {
		return true
	}
	return false
}

func releaseRecoveryProbeServiceID(step *agentpb.ExecutionStep) (string, bool) {
	if value := step.GetServiceProxyProbe(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	if value := step.GetServiceRecreateProbe(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	if value := step.GetCandidateRestorationProbe(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	return "", false
}

func releaseCompensationServiceID(step *agentpb.ExecutionStep) (string, bool) {
	if value := step.GetServiceProxyCompensate(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	if value := step.GetServiceRecreateCompensate(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	if value := step.GetCandidateRestorationCompensate(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	return "", false
}
