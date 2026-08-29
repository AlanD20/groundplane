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
		for _, step := range reservation.assignment.Plan.Steps {
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE {
				continue
			}
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				probeErr, probeStepID = state.err, state.failedStepID
				break
			}
		}
		state.err = nil
		compensated := false
		for index := len(reservation.assignment.Plan.Steps) - 1; index >= 0; index-- {
			step := reservation.assignment.Plan.Steps[index]
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE || !releaseCompensationEnabled(step) {
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
		for _, step := range reservation.assignment.Plan.Steps {
			if step.Policy != agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD {
				continue
			}
			p.runReleaseStep(runCtx, reservation, step, state)
			if state.err != nil {
				break
			}
		}
		if state.err != nil {
			original, originalStep := state.err, state.failedStepID
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
				state.err, state.failedStepID = original, originalStep
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

func (p *WorkerPool) runReleaseStep(runCtx context.Context, reservation *taskReservation, step *agentpb.ExecutionStep, state *releaseExecutionState) {
	planHash := hashForPlan(reservation.assignment.Plan)
	p.emitProgress(runCtx, TaskProgress{AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID, PlanHash: planHash, StepID: step.StepId, Attempt: 1, Ordinal: 1, State: TaskProgressRunning})
	stepCtx, cancel := context.WithTimeout(reservation.ctx, time.Duration(step.TimeoutSeconds)*time.Second)
	result, err := p.compose.executeStep(stepCtx, reservation.assignment, step)
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
