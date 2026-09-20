package agent

import (
	"context"
	taskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// ensureManagedComposeResource prepares only the sealed Network or Volume. The helper
// verifies the physical result; observing workload containers here would fail
// the first-release path before those containers are allowed to exist.
func (runtime *ComposeRuntime) ensureManagedComposeResource(
	ctx context.Context,
	assignment taskassignment.Assignment,
	step *agentpb.ExecutionStep,
) (composeStepResult, error) {
	result := composeStepResult{MutationAttempted: true}
	response, err := runtime.helper.Execute(ctx, &agentpb.ComposeHelperRequest{
		Schema: composeHelperSchema, AssignmentId: assignment.AssignmentID,
		TaskId: assignment.TaskID, OperationId: assignment.OperationID,
		Plan: assignment.Plan, StepId: step.GetStepId(),
		TimeoutSeconds: remainingSeconds(ctx, step.GetTimeoutSeconds()),
	})
	if err != nil {
		result.ReconciliationRequired = true
		return result, err
	}
	if response == nil || response.GetSchema() != composeHelperSchema {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindInternal, "agent: resource preparation returned an invalid response")
	}
	result.ExitCode, result.Diagnostic = response.GetExitCode(), response.GetDiagnostic()
	if response.GetOutcome() != agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED ||
		response.GetExitCode() != 0 ||
		response.GetDiagnostic() != agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE {
		result.ReconciliationRequired = true
		return result, errs.New(errs.KindRequestFailed, "agent: resource preparation did not prove completion")
	}
	return result, nil
}
