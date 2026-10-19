package agent

import (
	"context"
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	"github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type fakeEnvironmentDirectoryHelper struct {
	response *agentpb.EnvironmentDirectoryHelperResponse
	request  *agentpb.EnvironmentDirectoryHelperRequest
}

func (helper *fakeEnvironmentDirectoryHelper) Execute(_ context.Context,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	helper.request = request
	return helper.response, nil
}

// OWN-01/05: retain the physical helper boundary for create and remove. A failed
// directory operation cannot become success or lose its exact failed-step ID.
// Native Environment execution/settlement is qualified separately, not here.
func TestEnvironmentDirectoryRuntimeReturnsClosedHelperResult(t *testing.T) {
	for _, remove := range []bool{false, true} {
		helper := &fakeEnvironmentDirectoryHelper{response: &agentpb.EnvironmentDirectoryHelperResponse{
			Schema: environmentdirectoryhelper.SchemaVersion,
		}}
		runtime, err := environmentdirectory.New(helper)
		if err != nil {
			t.Fatal(err)
		}
		assignment := environmentDirectoryAssignment(t, remove)
		step := assignment.Plan.Steps[0]
		result, err := runtime.ExecuteStep(t.Context(), assignment, step)
		if err != nil || result.ExitCode != 0 || result.FailedStepID != "" {
			t.Fatalf("ExecuteStep: %#v, %v", result, err)
		}
		if helper.request.TaskId != assignment.TaskID || helper.request.OperationId != assignment.OperationID ||
			helper.request.StepId != step.StepId || helper.request.TimeoutSeconds == 0 {
			t.Fatalf("helper request lost captured operation: %#v", helper.request)
		}
		helper.response = &agentpb.EnvironmentDirectoryHelperResponse{
			Schema: environmentdirectoryhelper.SchemaVersion, ExitCode: 1, FailedStepId: step.StepId,
		}
		result, err = runtime.ExecuteStep(t.Context(), assignment, step)
		if result.ExitCode != 1 || result.FailedStepID != step.StepId ||
			!errors.Is(err, errs.New(errs.KindRequestFailed, "")) {
			t.Fatalf("ExecuteStep failure: %#v, %v", result, err)
		}
	}
}

func environmentDirectoryAssignment(t *testing.T, remove bool) taskassignment.Assignment {
	t.Helper()
	const environment = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	const directory = "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environment
	step := &agentpb.ExecutionStep{StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
			EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{EnvironmentId: environment,
				ExpectedVolumeDir: directory}}}
	operation := agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE
	if remove {
		operation = agentpb.PlanOperation_PLAN_OPERATION_REMOVE
		step.Payload = &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
			EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{EnvironmentId: environment,
				ExpectedVolumeDir: directory}}
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RenderGeneration: 1, Operation: operation, TargetId: environment, Steps: []*agentpb.ExecutionStep{step},
	})
	if err != nil {
		t.Fatal(err)
	}
	return taskassignment.Assignment{AssignmentID: "asn_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		TaskID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV", Plan: plan}
}
