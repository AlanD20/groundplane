package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type fakeEnvironmentDirectoryHelper struct {
	response *agentpb.EnvironmentDirectoryHelperResponse
	request  *agentpb.EnvironmentDirectoryHelperRequest
}

func (helper *fakeEnvironmentDirectoryHelper) Execute(
	_ context.Context,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	helper.request = request
	return helper.response, nil
}

func TestEnvironmentDirectoryRuntimeReturnsClosedHelperResult(t *testing.T) {
	helper := &fakeEnvironmentDirectoryHelper{response: &agentpb.EnvironmentDirectoryHelperResponse{
		Schema: environmentDirectoryHelperSchema,
	}}
	runtime, err := NewEnvironmentDirectoryRuntime(helper)
	if err != nil {
		t.Fatalf("NewEnvironmentDirectoryRuntime() error = %v", err)
	}
	assignment := environmentDirectoryAssignment(t)
	result, err := runtime.executeStep(context.Background(), assignment, assignment.Plan.Steps[0])
	if err != nil || result.ExitCode != 0 || result.FailedStepID != "" {
		t.Fatalf("executeStep() = %#v, %v", result, err)
	}
	if helper.request.TaskId != assignment.TaskID || helper.request.OperationId != assignment.OperationID ||
		helper.request.StepId != workerTestStepID || helper.request.TimeoutSeconds == 0 {
		t.Fatalf("helper request = %#v", helper.request)
	}

	helper.response = &agentpb.EnvironmentDirectoryHelperResponse{
		Schema: environmentDirectoryHelperSchema, ExitCode: 1, FailedStepId: workerTestStepID,
	}
	result, err = runtime.executeStep(context.Background(), assignment, assignment.Plan.Steps[0])
	if result.ExitCode != 1 || result.FailedStepID != workerTestStepID ||
		!errors.Is(err, errs.New(errs.KindRequestFailed, "")) {
		t.Fatalf("executeStep(failed) = %#v, %v", result, err)
	}
}

func TestWorkerPoolReturnsEnvironmentDirectoryResultOnly(t *testing.T) {
	helper := &fakeEnvironmentDirectoryHelper{response: &agentpb.EnvironmentDirectoryHelperResponse{
		Schema: environmentDirectoryHelperSchema,
	}}
	directories, err := NewEnvironmentDirectoryRuntime(helper)
	if err != nil {
		t.Fatalf("NewEnvironmentDirectoryRuntime() error = %v", err)
	}
	pool := NewWorkerPoolWithRuntimes(
		1,
		"/var/lib/groundplane/vol",
		nil,
		testLogger(),
		nil,
		directories,
		nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	assignment := environmentDirectoryAssignment(t)
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalCompleted || result.ExitCode != 0 ||
		result.EnvironmentDirectory == nil || result.Compose != nil {
		t.Fatalf("result = %#v", result)
	}
	cancel()
	<-done
}

// Rationale: Environment removal has a generic remove operation shared by
// other resources, so its directory result must be selected from the typed
// step rather than from the operation alone.
func TestWorkerPoolReturnsEnvironmentDirectoryResultForRemoval(t *testing.T) {
	helper := &fakeEnvironmentDirectoryHelper{response: &agentpb.EnvironmentDirectoryHelperResponse{
		Schema: environmentDirectoryHelperSchema,
	}}
	directories, err := NewEnvironmentDirectoryRuntime(helper)
	if err != nil {
		t.Fatalf("NewEnvironmentDirectoryRuntime() error = %v", err)
	}
	pool := NewWorkerPoolWithRuntimes(
		1, "/var/lib/groundplane/vol", nil, testLogger(), nil, directories, nil,
	)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		pool.Run(ctx)
		close(done)
	}()
	assignment := environmentDirectoryRemovalAssignment(t)
	if err := pool.Submit(ctx, assignment); err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	result := nextWorkerResult(t, pool)
	if result.Terminal != TaskTerminalCompleted || result.ExitCode != 0 ||
		result.EnvironmentDirectory == nil || result.Compose != nil {
		t.Fatalf("result = %#v", result)
	}
	cancel()
	<-done
}

func TestSendTaskAckPreservesEnvironmentDirectoryResultVariant(t *testing.T) {
	stream := newFakeStream()
	client := &Client{}
	if err := client.sendTaskAck(stream, TaskResult{
		TaskID: workerTestTaskID, Terminal: TaskTerminalCompleted,
		EnvironmentDirectory: &agentpb.EnvironmentDirectoryTaskResult{},
	}); err != nil {
		t.Fatalf("sendTaskAck() error = %v", err)
	}
	acknowledgement := stream.sentMessages()[0].GetTaskAck()
	if acknowledgement.GetEnvironmentDirectoryResult() == nil || acknowledgement.GetComposeResult() != nil {
		t.Fatalf("TaskAck = %#v", acknowledgement)
	}
}

func environmentDirectoryAssignment(t *testing.T) Assignment {
	t.Helper()
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion,
		PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_ENVIRONMENT_CREATE,
		TargetId:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Steps: []*agentpb.ExecutionStep{{
			StepId: workerTestStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{
					EnvironmentId:     "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					ExpectedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       workerTestTaskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: plan, Deadline: time.Now().Add(time.Minute),
	}
}

func environmentDirectoryRemovalAssignment(t *testing.T) Assignment {
	t.Helper()
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion,
		PlanId: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		TargetId:  "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Steps: []*agentpb.ExecutionStep{{
			StepId: workerTestStepID, TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
				EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{
					EnvironmentId:     "env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
					ExpectedVolumeDir: "/var/lib/groundplane/vol/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/env_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				},
			},
		}},
	})
	if err != nil {
		t.Fatalf("Seal() error = %v", err)
	}
	return Assignment{
		AssignmentID: workerTestAssignmentID,
		TaskID:       workerTestTaskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: plan, Deadline: time.Now().Add(time.Minute),
	}
}
