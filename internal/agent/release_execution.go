package agent

import (
	"context"
	"errors"
	"slices"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type releaseExecutionState struct {
	err            error
	exitCode       int32
	failedStepID   string
	diagnostic     agentpb.ComposeHelperDiagnostic
	reconciliation bool
	projects       map[string]*agentpb.ObservedProject
	evidence       map[string]*agentpb.ServiceProxyEvidence
	recreate       map[string]*agentpb.ServiceRecreateEvidence
	absence        map[string]*agentpb.CandidateAbsenceEvidence
	probeRequired  map[string]bool
	completed      map[string]struct{}
	recoveryClosed bool
	restored       bool

	componentMutationAttempted bool
}

func (p *WorkerPool) executeRelease(runCtx context.Context, reservation *taskReservation) {
	state := &releaseExecutionState{
		diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		projects:   map[string]*agentpb.ObservedProject{}, evidence: map[string]*agentpb.ServiceProxyEvidence{}, recreate: map[string]*agentpb.ServiceRecreateEvidence{},
		probeRequired: map[string]bool{}, completed: map[string]struct{}{},
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
	terminal := terminalFor(reservation.ctx, state.err)
	if terminal != TaskTerminalCompleted &&
		reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
		reservation.assignment.Plan.GetCandidateReleaseProcedure() != nil &&
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
		PlanHash: hashForPlan(reservation.assignment.Plan), Terminal: terminal, ExitCode: state.exitCode,
		ExecutionEpoch:              reservation.assignment.ExecutionEpoch,
		ReleaseRecoveryRecordSHA256: append([]byte(nil), reservation.assignment.ReleaseRecoveryRecordSHA256...),
		Compose:                     releaseComposeTaskResult(state),
	}
	if state.err != nil && p.logger != nil {
		p.logger.Error("agent: release step failed", "task_id", reservation.assignment.TaskID, "error", state.err)
	}
	p.complete(runCtx, reservation, result)
}

func (p *WorkerPool) runReleaseRecovery(
	runCtx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	directive := reservation.assignment.ReleaseRecoveryDirective
	steps := make(map[string]*agentpb.ExecutionStep, len(reservation.assignment.Plan.GetSteps()))
	probes := make(map[string]*agentpb.ExecutionStep)
	for _, step := range reservation.assignment.Plan.GetSteps() {
		steps[step.GetStepId()] = step
		if step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
			if serviceID, ok := releaseRecoveryProbeServiceID(step); ok {
				probes[serviceID] = step
			}
		}
	}
	compensationRequired := make(map[string]bool)
	if directive.GetPhase() == agentpb.ReleaseRecoveryPhase_RELEASE_RECOVERY_PHASE_PROVEN {
		for _, step := range probes {
			required, err := p.probeReleaseRecoveryWithoutEvent(reservation, step, state)
			if err != nil || required {
				state.err, state.failedStepID = errors.Join(
					err,
					errs.New(errs.KindInternal, "agent: proven release restoration no longer has exact proof"),
				), step.GetStepId()
				state.reconciliation = true
				return
			}
		}
		state.recoveryClosed = true
		return
	}
	for _, stepID := range directive.GetStepIds()[directive.GetCursor():] {
		step := steps[stepID]
		if step == nil {
			state.err, state.failedStepID = errs.New(
				errs.KindInternal,
				"agent: release recovery directive step is absent",
			), stepID
			state.reconciliation = true
			return
		}
		switch step.GetPolicy() {
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE:
			serviceID, ok := releaseRecoveryProbeServiceID(step)
			if !ok {
				state.err, state.failedStepID = errs.New(
					errs.KindInternal,
					"agent: release recovery probe has no Service identity",
				), stepID
				state.reconciliation = true
				return
			}
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				state.reconciliation = true
				return
			}
			compensationRequired[serviceID] = state.probeRequired[stepID]
		case agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE:
			serviceID, ok := releaseCompensationServiceID(step)
			if !ok {
				state.err, state.failedStepID = errs.New(
					errs.KindInternal,
					"agent: release recovery compensation is invalid",
				), stepID
				state.reconciliation = true
				return
			}
			required, known := compensationRequired[serviceID]
			applicable := slices.Contains(directive.GetApplicableCompensationStepIds(), stepID)
			if !known {
				probe := probes[serviceID]
				if probe == nil {
					state.err, state.failedStepID = errs.New(
						errs.KindInternal,
						"agent: release recovery compensation has no sealed probe",
					), stepID
					state.reconciliation = true
					return
				}
				var err error
				required, err = p.probeReleaseRecoveryWithoutEvent(reservation, probe, state)
				if err != nil {
					state.err, state.failedStepID, state.reconciliation = err, probe.GetStepId(), true
					return
				}
			}
			if required && !applicable {
				state.err, state.failedStepID = errs.New(
					errs.KindInternal,
					"agent: host state cannot create an unrecorded compensation obligation",
				), stepID
				state.reconciliation = true
				return
			}
			if required {
				if !releaseCompensationEnabled(step) {
					state.err, state.failedStepID = errs.New(
						errs.KindInternal,
						"agent: required release recovery compensation is disabled",
					), stepID
					state.reconciliation = true
					return
				}
				p.runReleaseStep(runCtx, reservation, step, state)
				if state.err != nil || !state.restored {
					if state.err == nil {
						state.err = errs.New(
							errs.KindInternal,
							"agent: release compensation returned no exact restoration proof",
						)
					}
					state.reconciliation = true
					return
				}
			} else if err := p.completeReleaseRecoveryNoop(reservation, step, state); err != nil {
				state.err, state.failedStepID, state.reconciliation = err, stepID, true
				return
			}
		default:
			state.err, state.failedStepID = errs.New(
				errs.KindInternal,
				"agent: release recovery directive selected a forward step",
			), stepID
			state.reconciliation = true
			return
		}
	}
	state.recoveryClosed = true
}

func (p *WorkerPool) probeReleaseRecoveryWithoutEvent(
	reservation *taskReservation,
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) (bool, error) {
	stepCtx, cancel := context.WithTimeout(reservation.ctx, time.Duration(step.GetTimeoutSeconds())*time.Second)
	result, err := p.compose.executeStep(stepCtx, reservation.assignment, step)
	cancel()
	if err != nil {
		return false, err
	}
	if result.ProxyEvidence != nil {
		state.evidence[result.ProxyEvidence.GetServiceId()] = proto.Clone(result.ProxyEvidence).(*agentpb.ServiceProxyEvidence)
	}
	if result.RecreateEvidence != nil {
		state.recreate[result.RecreateEvidence.GetServiceId()] = proto.Clone(result.RecreateEvidence).(*agentpb.ServiceRecreateEvidence)
	}
	if result.CandidateAbsenceEvidence != nil {
		if err := state.recordAbsenceEvidence(result.CandidateAbsenceEvidence); err != nil {
			return false, err
		}
	}
	if result.ReconciliationRequired {
		return false, errs.New(errs.KindInternal, "agent: release recovery probe requires reconciliation")
	}
	return releaseProbeEvidenceStatus(reservation.assignment, step, result)
}

func (p *WorkerPool) completeReleaseRecoveryNoop(
	reservation *taskReservation,
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) error {
	planHash := hashForPlan(reservation.assignment.Plan)
	for ordinal, progressState := range []TaskProgressState{TaskProgressRunning, TaskProgressCompleted} {
		progress := TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID,
			PlanHash: planHash, StepID: step.GetStepId(), ExecutionEpoch: reservation.assignment.ExecutionEpoch,
			Ordinal: uint64(ordinal + 1), State: progressState,
		}
		if reservation.eventsDurable {
			if err := p.emitProgressAndWait(reservation.ctx, progress); err != nil {
				return err
			}
		} else {
			p.emitProgress(reservation.ctx, progress)
		}
	}
	state.completed[step.GetStepId()] = struct{}{}
	return nil
}

// Blueprint seals one global order, including setup before pre-hooks. Standalone
// Releases retain their per-candidate post-hook scheduling in runReleaseForward.
func (p *WorkerPool) runBlueprintReleaseForward(
	ctx context.Context,
	reservation *taskReservation,
	state *releaseExecutionState,
) {
	stepID, err := p.compose.preflightManagedComponentTeardown(ctx, reservation.assignment.Plan)
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
	planHash := hashForPlan(reservation.assignment.Plan)
	running := TaskProgress{AssignmentID: reservation.assignment.AssignmentID,
		TaskID: reservation.assignment.TaskID, PlanHash: planHash, StepID: step.StepId,
		ExecutionEpoch: reservation.assignment.ExecutionEpoch, Ordinal: 1, State: TaskProgressRunning}
	blueprintComponent := reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
		step.GetComponentApply() != nil
	if reservation.eventsDurable && (candidateMutationStep(step) || blueprintComponent) {
		if err := p.emitProgressAndWait(reservation.ctx, running); err != nil {
			state.err, state.failedStepID = err, step.GetStepId()
			return
		}
	} else {
		p.emitProgress(runCtx, running)
	}
	stepCtx, cancel := context.WithTimeout(reservation.ctx, time.Duration(step.TimeoutSeconds)*time.Second)
	var result composeStepResult
	var err error
	if step.GetRunScript() != nil {
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
	} else if reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY && step.GetComponentApply() != nil {
		result, err = p.executeBlueprintReleaseComponent(stepCtx, reservation.assignment, step)
		state.componentMutationAttempted = state.componentMutationAttempted || result.MutationAttempted
	} else if reservation.assignment.Plan.Operation == agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY &&
		(step.GetMaterializeFile() != nil || step.GetAdapterProcedure() != nil || step.GetManagedVolumeDirectoriesEnsure() != nil || step.GetEnvironmentDirectoryCreate() != nil) {
		result, err = p.executeBlueprintReleaseSetup(stepCtx, reservation.assignment, step)
	} else {
		result, err = p.compose.executeStep(stepCtx, reservation.assignment, step)
	}
	stepContextErr := stepCtx.Err()
	cancel()
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
	}
	if result.RecreateEvidence != nil {
		state.recreate[result.RecreateEvidence.ServiceId] = proto.Clone(result.RecreateEvidence).(*agentpb.ServiceRecreateEvidence)
	}
	err = state.recordAbsenceResult(&result, err)
	if err == nil && step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE {
		state.restored = releaseRestorationEvidenceProven(reservation.assignment, step, result)
		if !state.restored {
			err = errs.New(errs.KindInternal, "agent: release compensation returned no exact restoration proof")
			result.ReconciliationRequired = true
		}
	}
	if err == nil && step.GetPolicy() == agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
		var required bool
		required, err = releaseProbeEvidenceStatus(reservation.assignment, step, result)
		if err == nil {
			state.probeRequired[step.GetStepId()] = required
		} else {
			result.ReconciliationRequired = true
		}
	}
	progress := progressStateFor(reservation.ctx, errors.Join(err, stepContextErr))
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
		ExecutionEpoch: reservation.assignment.ExecutionEpoch, Ordinal: 2, State: progress}
	if reservation.eventsDurable &&
		reservation.assignment.ExecutionMode == agentpb.TaskExecutionMode_TASK_EXECUTION_MODE_RECOVERY_ONLY {
		if progressErr := p.emitProgressAndWait(reservation.ctx, completedProgress); progressErr != nil &&
			state.err == nil {
			state.err, state.failedStepID, state.reconciliation = progressErr, step.GetStepId(), true
		}
	} else {
		p.emitProgress(runCtx, completedProgress)
	}
}

func candidateMutationStep(step *agentpb.ExecutionStep) bool {
	if step.GetComposeRemove() != nil {
		return true
	}
	if apply := step.GetComposeApply(); apply != nil {
		return apply.GetForceRecreate() && apply.GetNoDependencies()
	}
	return step.GetComposeWorkloadApply() != nil || step.GetServiceProxySwitch() != nil ||
		step.GetServiceProxyCompensate() != nil || step.GetServiceRecreateCompensate() != nil ||
		step.GetCandidateRestorationCompensate() != nil
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
	if !hasProxy && !hasRecreate {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE
	}
	if hasProxy == hasRecreate || hasProxy && proxy == nil || hasRecreate && recreate == nil {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	servingReleaseID := ""
	if hasProxy {
		servingReleaseID = proxy.GetReleaseId()
	} else {
		servingReleaseID = recreate.GetReleaseId()
	}
	if servingReleaseID == "" || servingReleaseID != run.GetReleaseId() {
		return false, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE
	}
	return true, agentpb.ScriptOutcomeReason_SCRIPT_OUTCOME_REASON_UNSPECIFIED
}

func releaseComposeTaskResult(state *releaseExecutionState) *agentpb.ComposeTaskResult {
	result := composeTaskResult(state.projects, state.failedStepID, state.diagnostic, state.reconciliation)
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
