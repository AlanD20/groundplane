package agent

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (p *WorkerPool) executeManagedNetworkRecovery(runCtx context.Context, reservation *taskReservation) {
	plan := reservation.assignment.Plan
	procedure, directive := plan.GetManagedNetworkRecoveryProcedure(), reservation.assignment.ReleaseRecoveryDirective
	state := &releaseExecutionState{diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE}
	steps := executionplan.NetworkRecoverySettlementSteps(procedure)
	if p.compose == nil {
		state.err = errs.New(errs.KindInternal, "agent: managed network recovery runtime is not configured")
	} else if procedure == nil || directive == nil || len(steps) == 0 || !slices.Equal(steps, directive.GetStepIds()) {
		state.err = errs.New(errs.KindInternal, "agent: managed network recovery authority changed")
	}
	for _, stepID := range steps {
		if state.err != nil {
			break
		}
		var selected *agentpb.ExecutionStep
		for _, candidate := range plan.GetSteps() {
			if candidate.GetStepId() == stepID {
				selected = candidate
				break
			}
		}
		if selected == nil {
			state.err = errs.New(errs.KindInternal, "agent: managed network settlement step is absent")
			break
		}
		running := TaskProgress{
			AssignmentID: reservation.assignment.AssignmentID,
			TaskID:       reservation.assignment.TaskID,
			PlanHash: taskassignment.PlanDigest(
				plan,
			),
			StepID:         stepID,
			ExecutionEpoch: reservation.assignment.ExecutionEpoch,
			Ordinal:        1,
			State:          TaskProgressRunning,
		}
		receipt, err := p.emitProgressForReceipt(reservation.ctx, running)
		if err != nil {
			state.err, state.failedStepID = err, stepID
			break
		}
		stepCtx, cancel := context.WithTimeout(reservation.ctx, time.Duration(selected.GetTimeoutSeconds())*time.Second)
		result, err := p.compose.ExecuteStep(stepCtx, reservation.assignment, selected, nil)
		state.err = errors.Join(err, stepCtx.Err())
		cancel()
		state.exitCode = result.ExitCode
		if result.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED {
			state.diagnostic = result.Diagnostic
		}
		if result.ReconciliationRequired && state.err == nil {
			state.err = errs.New(errs.KindStateConflict, "agent: managed network settlement was not proven")
		}
		if state.err != nil {
			state.failedStepID = stepID
		}
		completed := running
		completed.Ordinal, completed.State = receipt.GetEventOrdinalBase()+1, progressStateFor(
			reservation.ctx,
			state.err,
		)
		completed.Failure = taskprogress.CaptureFailure(state.err, state.exitCode, state.diagnostic)
		if err := p.emitProgressAndWait(reservation.ctx, completed); err != nil && state.err == nil {
			state.err, state.failedStepID = err, stepID
		}
	}
	state.reconciliation = state.err != nil
	terminal := terminalFor(reservation.ctx, state.err)
	result := TaskResult{AssignmentID: reservation.assignment.AssignmentID, TaskID: reservation.assignment.TaskID,
		PlanHash: taskassignment.PlanDigest(plan), Terminal: terminal, ExitCode: state.exitCode,
		ExecutionEpoch:              reservation.assignment.ExecutionEpoch,
		ReleaseRecoveryRecordSHA256: append([]byte(nil), reservation.assignment.ReleaseRecoveryRecordSHA256...),
		Compose:                     composeTaskResult(nil, state.failedStepID, state.diagnostic, state.reconciliation)}
	if state.err != nil && p.logger != nil {
		p.logger.Error(
			"agent: managed network recovery failed",
			"task_id",
			reservation.assignment.TaskID,
			"error",
			state.err,
		)
	}
	result.Compose.Failure = taskprogress.CaptureFailure(state.err, state.exitCode, state.diagnostic)
	p.complete(runCtx, reservation, result)
}
