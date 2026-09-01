package agent

import (
	"context"
	"errors"
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
	completed      map[string]struct{}
	recoveryClosed bool
}

func (p *WorkerPool) executeRelease(runCtx context.Context, reservation *taskReservation) {
	state := &releaseExecutionState{
		diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		projects:   map[string]*agentpb.ObservedProject{}, evidence: map[string]*agentpb.ServiceProxyEvidence{}, recreate: map[string]*agentpb.ServiceRecreateEvidence{},
		completed: map[string]struct{}{},
	}
	if p.compose == nil {
		state.err = errs.New(errs.KindInternal, "agent: release Compose runtime is not configured")
	} else if reservation.assignment.RetryOf != "" {
		var probeErr error
		probeStepID := ""
		compensationRequired := make(map[string]bool)
		for _, step := range reservation.assignment.Plan.Steps {
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
				continue
			}
			serviceID, ok := releaseRecoveryProbeServiceID(step)
			if !ok {
				if probeErr == nil {
					probeErr = errs.New(errs.KindInternal, "agent: release recovery probe has no Service identity")
					probeStepID = step.StepId
				}
				continue
			}
			state.err, state.failedStepID = nil, ""
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				if probeErr == nil {
					probeErr, probeStepID = state.err, state.failedStepID
				}
				compensationRequired[serviceID] = true
				continue
			}
			required, dispositionErr := releaseProbeRequiresCompensation(step, state)
			if dispositionErr != nil {
				if probeErr == nil {
					probeErr, probeStepID = dispositionErr, step.StepId
				}
				compensationRequired[serviceID] = true
				continue
			}
			compensationRequired[serviceID] = required
		}
		state.err, state.failedStepID = nil, ""
		compensated := false
		for index := len(reservation.assignment.Plan.Steps) - 1; index >= 0; index-- {
			step := reservation.assignment.Plan.Steps[index]
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE || !releaseCompensationEnabled(step) {
				continue
			}
			serviceID, ok := releaseCompensationServiceID(step)
			if !ok {
				state.err = errs.New(errs.KindInternal, "agent: release compensation has no Service identity")
				state.failedStepID = step.StepId
				break
			}
			if !compensationRequired[serviceID] {
				continue
			}
			compensated = true
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				break
			}
		}
		if state.err == nil && probeErr != nil {
			state.err, state.failedStepID = probeErr, probeStepID
			state.recoveryClosed = compensated
			state.reconciliation = !compensated
		}
	} else {
		p.runReleasePhase(runCtx, reservation, state,
			agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK)
		if state.err == nil {
			p.runReleaseForward(runCtx, reservation, state)
		}
		if state.err != nil {
			original, originalStep := state.err, state.failedStepID
			originalExitCode, originalDiagnostic := state.exitCode, state.diagnostic
			compensated := true
			for index := len(reservation.assignment.Plan.Steps) - 1; index >= 0; index-- {
				step := reservation.assignment.Plan.Steps[index]
				if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE || !releaseCompensationEnabled(step) {
					continue
				}
				if _, ok := state.completed[step.PrerequisiteStepId]; !ok {
					continue
				}
				state.err = nil
				p.runReleaseStep(runCtx, reservation, step, state)
				if state.err != nil {
					compensated = false
					break
				}
			}
			if compensated {
				state.reconciliation = false
			}
			state.err, state.failedStepID = original, originalStep
			state.exitCode, state.diagnostic = originalExitCode, originalDiagnostic
			if compensated && reservation.ctx.Err() == nil && !errors.Is(original, context.DeadlineExceeded) {
				p.runReleaseFailureHooks(runCtx, reservation, state)
				state.err, state.failedStepID = original, originalStep
				state.exitCode, state.diagnostic = originalExitCode, originalDiagnostic
			}
		}
	}
	terminal := terminalFor(reservation.ctx, state.err)
	if terminal != TaskTerminalCompleted && !state.reconciliation && reservation.assignment.RetryOf != "" && !state.recoveryClosed {
		state.reconciliation = true
	}
	result := TaskResult{
		AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID,
		PlanHash: hashForPlan(reservation.assignment.Plan), Terminal: terminal, ExitCode: state.exitCode,
		Compose: releaseComposeTaskResult(state),
	}
	if state.err != nil && p.logger != nil {
		p.logger.Error("agent: release step failed", "task_id", reservation.assignment.TaskID, "error", state.err)
	}
	p.complete(runCtx, reservation, result)
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

func (p *WorkerPool) runReleaseStep(runCtx context.Context, reservation *taskReservation, step *agentpb.ExecutionStep, state *releaseExecutionState) {
	planHash := hashForPlan(reservation.assignment.Plan)
	p.emitProgress(runCtx, TaskProgress{AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID, PlanHash: planHash, StepID: step.StepId, Attempt: 1, Ordinal: 1, State: TaskProgressRunning})
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
	} else {
		result, err = p.compose.executeStep(stepCtx, reservation.assignment, step)
	}
	progress := progressStateFor(reservation.ctx, errors.Join(err, stepCtx.Err()))
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
	p.emitProgress(runCtx, TaskProgress{AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID, PlanHash: planHash, StepID: step.StepId, Attempt: 1, Ordinal: 2, State: progress})
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
		result.ProxyEvidence = append(result.ProxyEvidence, state.evidence[serviceID])
	}
	recreateIDs := make([]string, 0, len(state.recreate))
	for serviceID := range state.recreate {
		recreateIDs = append(recreateIDs, serviceID)
	}
	sort.Strings(recreateIDs)
	for _, serviceID := range recreateIDs {
		result.RecreateEvidence = append(result.RecreateEvidence, state.recreate[serviceID])
	}
	return result
}

func releaseCompensationEnabled(step *agentpb.ExecutionStep) bool {
	if value := step.GetServiceProxyCompensate(); value != nil {
		return value.Enabled
	}
	if value := step.GetServiceRecreateCompensate(); value != nil {
		return value.Enabled
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
	return "", false
}

func releaseCompensationServiceID(step *agentpb.ExecutionStep) (string, bool) {
	if value := step.GetServiceProxyCompensate(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	if value := step.GetServiceRecreateCompensate(); value != nil {
		return value.ServiceId, value.ServiceId != ""
	}
	return "", false
}

func releaseProbeRequiresCompensation(
	step *agentpb.ExecutionStep,
	state *releaseExecutionState,
) (bool, error) {
	if probe := step.GetServiceProxyProbe(); probe != nil {
		evidence, ok := state.evidence[probe.ServiceId]
		if !ok {
			return false, errs.New(errs.KindInternal, "agent: release recovery probe returned no proxy evidence")
		}
		if evidence.ReleaseId == probe.AlternateReleaseId && evidence.Target == probe.AlternateTarget {
			return true, nil
		}
		if evidence.ReleaseId == probe.ReleaseId && evidence.Target == probe.ExpectedTarget {
			return false, nil
		}
		return false, errs.New(errs.KindInternal, "agent: release recovery proxy evidence is ambiguous")
	}
	if probe := step.GetServiceRecreateProbe(); probe != nil {
		evidence, ok := state.recreate[probe.ServiceId]
		if !ok {
			return false, errs.New(errs.KindInternal, "agent: release recovery probe returned no recreate evidence")
		}
		if evidence.ReleaseId == probe.CandidateReleaseId && evidence.ArtifactId == probe.CandidateArtifactId {
			return true, nil
		}
		if evidence.ReleaseId == probe.PriorReleaseId && evidence.ArtifactId == probe.PriorArtifactId {
			return false, nil
		}
		return false, errs.New(errs.KindInternal, "agent: release recovery recreate evidence is ambiguous")
	}
	return false, errs.New(errs.KindInternal, "agent: release recovery probe type is invalid")
}
