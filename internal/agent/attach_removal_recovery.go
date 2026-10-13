package agent

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"time"

	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Recovery never re-enters the generic forward loop. Its exact cursor skips
// durable completions, and every remaining effect is started and completed
// durably before the next compiled cleanup step can execute.
func (p *WorkerPool) executeAttachRemovalRecovery(runCtx context.Context, reservation *taskReservation) {
	assignment := reservation.assignment
	state := &releaseExecutionState{diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE}
	state.err = executionplan.ValidateAttachRemovalRecoveryDirective(
		assignment.Plan, assignment.ReleaseRecoveryDirective, assignment.ReleaseRecoveryRecordSHA256,
	)
	if state.err == nil {
		for _, step := range assignment.Plan.GetSteps()[assignment.ReleaseRecoveryDirective.GetCursor():] {
			if reservation.ctx.Err() != nil {
				state.err = reservation.ctx.Err()
				break
			}
			progress := TaskProgress{
				AssignmentID: assignment.AssignmentID, TaskID: assignment.TaskID,
				PlanHash: taskassignment.PlanDigest(assignment.Plan), StepID: step.GetStepId(),
				ExecutionEpoch: assignment.ExecutionEpoch, Ordinal: 1, State: TaskProgressRunning,
			}
			var receipt *agentpb.TaskEventAck
			if receipt, state.err = p.emitProgressForReceipt(reservation.ctx, progress); state.err != nil {
				state.failedStepID = step.GetStepId()
				break
			}
			stepCtx, cancel := context.WithTimeout(reservation.ctx, time.Duration(step.GetTimeoutSeconds())*time.Second)
			if step.GetBackingHookProcedure() != nil {
				state.err = p.executeBackingHookStep(stepCtx, assignment, step)
			} else if step.GetAdapterProcedure() != nil {
				if p.adapter == nil {
					state.err = errs.New(errs.KindInternal, "agent: Detach adapter runtime is not configured")
				} else {
					result, err := p.adapter.ExecuteStep(stepCtx, step)
					state.err, state.exitCode = err, result.ExitCode
				}
			} else if step.GetComposeApply() != nil && p.compose != nil {
				result, err := p.compose.ExecuteStep(stepCtx, assignment, step, nil)
				state.err, state.exitCode = err, result.ExitCode
				if result.Diagnostic != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_UNSPECIFIED {
					state.diagnostic = result.Diagnostic
				}
				if result.ReconciliationRequired && state.err == nil {
					state.err = errs.New(errs.KindStateConflict, "agent: Detach network withdrawal is not proven")
				}
			} else {
				state.err = errs.New(errs.KindInternal, "agent: Detach network runtime is not configured")
			}
			state.err = errors.Join(state.err, stepCtx.Err())
			cancel()
			if state.exitCode != 0 && state.err == nil {
				state.err = errs.New(errs.KindStateConflict, "agent: Detach cleanup did not succeed")
			}
			progress.Ordinal, progress.State = receipt.GetEventOrdinalBase()+1, progressStateFor(
				reservation.ctx,
				state.err,
			)
			progress.Failure = taskprogress.CaptureFailure(state.err, state.exitCode, state.diagnostic)
			progressErr := p.emitProgressAndWait(reservation.ctx, progress)
			state.err = errors.Join(state.err, progressErr)
			if state.err != nil {
				state.failedStepID = step.GetStepId()
				break
			}
		}
	}
	state.reconciliation = state.err != nil
	result := TaskResult{
		AssignmentID: assignment.AssignmentID, TaskID: assignment.TaskID,
		PlanHash: taskassignment.PlanDigest(assignment.Plan), Terminal: terminalFor(reservation.ctx, state.err),
		ExecutionEpoch: assignment.ExecutionEpoch, ExitCode: state.exitCode,
		ReleaseRecoveryRecordSHA256: append([]byte(nil), assignment.ReleaseRecoveryRecordSHA256...),
		Compose:                     composeTaskResult(nil, state.failedStepID, state.diagnostic, state.reconciliation),
	}
	if state.err != nil && p.logger != nil {
		p.logger.Error("agent: Detach recovery failed", "task_id", assignment.TaskID, "error", state.err)
	}
	result.Compose.Failure = taskprogress.CaptureFailure(state.err, state.exitCode, state.diagnostic)
	p.complete(runCtx, reservation, result)
}
