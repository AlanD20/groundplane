package agent

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type fakeComposeHelper struct {
	response *agentpb.ComposeHelperResponse
	err      error
	request  *agentpb.ComposeHelperRequest
}

func (helper *fakeComposeHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	helper.request = request
	return helper.response, helper.err
}

type fakeComposeObserver struct {
	projects    []*agentpb.ObservedProject
	err         error
	image       *agentpb.ProcedureServiceImageResult
	verifyErr   error
	calls       int
	imageCalls  int
	verifyCalls int
	liveContext bool
}

func (observer *fakeComposeObserver) Observe(
	ctx context.Context,
	_ *agentpb.ExecutionPlan,
	_ string,
) (*agentpb.ObservedProject, error) {
	observer.calls++
	observer.liveContext = ctx.Err() == nil
	if observer.err != nil {
		return nil, observer.err
	}
	index := observer.calls - 1
	if index >= len(observer.projects) {
		index = len(observer.projects) - 1
	}
	return observer.projects[index], nil
}

func (observer *fakeComposeObserver) ObserveServiceImage(
	_ context.Context,
	_ *agentpb.ExecutionPlan,
	_, _, _, _ string,
) (*agentpb.ProcedureServiceImageResult, error) {
	observer.imageCalls++
	if observer.err != nil {
		return nil, observer.err
	}
	return observer.image, nil
}

func (observer *fakeComposeObserver) VerifyServiceImage(
	_ context.Context,
	_ *agentpb.ExecutionPlan,
	_ string,
	_ *agentpb.ProcedureServiceImageResult,
) error {
	observer.verifyCalls++
	return observer.verifyErr
}

func TestComposeRuntimeMutatesThenObserves(t *testing.T) {
	helper := completedComposeHelper()
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-platform"}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	assignment, step := composeRuntimeAssignment()

	result, err := runtime.executeStep(context.Background(), assignment, step)
	if err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if result.ExitCode != 0 || result.Observed == nil || observer.calls != 1 || result.ExecutionStepResult != nil {
		t.Fatalf("executeStep() result = %#v, observations = %d", result, observer.calls)
	}
	if helper.request.GetTaskId() != assignment.TaskID || helper.request.GetPlan() != assignment.Plan ||
		helper.request.GetStepId() != step.GetStepId() {
		t.Fatalf("helper request lost immutable assignment identity: %#v", helper.request)
	}
}

func TestComposeRuntimeObservesAfterCancelledMutation(t *testing.T) {
	helper := &fakeComposeHelper{err: context.Canceled}
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-platform"}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	assignment, step := composeRuntimeAssignment()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := runtime.mutate(ctx, assignment, step, "artifact_platform", nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("mutate() error = %v, want cancellation", err)
	}
	if observer.calls != 1 || !observer.liveContext {
		t.Fatalf("cancel reconciliation observations = %d, live = %t", observer.calls, observer.liveContext)
	}
	if !result.ReconciliationRequired || !result.MutationAttempted {
		t.Fatalf("mutate() result = %#v, want reconciliation evidence", result)
	}
}

func TestComposeRuntimeWaitHealthyTranslatesStableServiceIDs(t *testing.T) {
	helper := completedComposeHelper()
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{
		ProjectName: "gp-platform",
		Containers: []*agentpb.ObservedContainer{{
			ContainerId: "ctr_api", Name: "api-1", ServiceId: "svc_api",
			State:  agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
			Health: agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY,
		}},
	}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	assignment, _ := composeRuntimeAssignment()

	result, err := runtime.waitHealthy(context.Background(), assignment.Plan, &agentpb.WaitHealthy{
		ArtifactId: "artifact_platform", ServiceIds: []string{"svc_api"},
	})
	if err != nil {
		t.Fatalf("waitHealthy() error = %v", err)
	}
	if observer.calls != 1 {
		t.Fatalf("waitHealthy() observations = %d, want 1", observer.calls)
	}
	if result.Observed == nil {
		t.Fatal("waitHealthy() omitted terminal observation")
	}
}

func TestComposeRuntimeReturnsBoundedHelperFailure(t *testing.T) {
	helper := &fakeComposeHelper{response: &agentpb.ComposeHelperResponse{
		Schema: composeHelperSchema, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED,
		ExitCode: 17, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
	}}
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-platform"}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	assignment, step := composeRuntimeAssignment()

	result, err := runtime.executeStep(context.Background(), assignment, step)
	if result.ExitCode != 17 || !result.ReconciliationRequired ||
		!errors.Is(err, errs.New(errs.KindRequestFailed, "")) {
		t.Fatalf("executeStep() result = %#v, error = %v", result, err)
	}
}

func completedComposeHelper() *fakeComposeHelper {
	return &fakeComposeHelper{response: &agentpb.ComposeHelperResponse{
		Schema: composeHelperSchema, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
	}}
}

func composeRuntimeAssignment() (Assignment, *agentpb.ExecutionStep) {
	step := &agentpb.ExecutionStep{
		StepId: "step_apply", TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: "artifact_platform", ServiceIds: []string{"svc_api"},
		}},
	}
	plan := &agentpb.ExecutionPlan{Artifacts: []*agentpb.ComposeArtifact{{
		ArtifactId: "artifact_platform", ProjectName: "gp-platform",
		Services: []*agentpb.ComposeService{{
			ServiceId: "svc_api", ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
		}},
	}}}
	return Assignment{
		AssignmentID: "asgn_01J00000000000000000000000",
		TaskID:       "tsk_01J00000000000000000000000", OperationID: "op_01J00000000000000000000000",
		Plan: plan, Deadline: time.Now().Add(time.Minute),
	}, step
}

func TestComposeRuntimeComponentApplyDoesNotProduceProcedureImageResult(t *testing.T) {
	helper := completedComposeHelper()
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-platform"}}}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatal(err)
	}
	assignment, step := composeRuntimeAssignment()
	assignment.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_COMPONENT_APPLY
	result, err := runtime.executeStep(context.Background(), assignment, step)
	if err != nil || result.ExecutionStepResult != nil {
		t.Fatalf("Component ComposeApply result = %#v, %v", result, err)
	}
}

func TestComposeRuntimeReconnectVerifiesAcknowledgedCandidateWithoutMutation(t *testing.T) {
	assignment, step, evidence := procedureComposeAssignment(t)
	assignment.AcknowledgedStepResults = []*agentpb.ExecutionStepResult{evidence}
	helper := completedComposeHelper()
	observer := &fakeComposeObserver{}
	runtime, err := NewComposeRuntime(helper, observer)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.executeStep(context.Background(), assignment, step)
	if err != nil || helper.request != nil || observer.verifyCalls != 1 || result.MutationAttempted || result.ExecutionStepResult != nil {
		t.Fatalf("reconnect result = %#v, helper = %#v, verifies = %d, error = %v", result, helper.request, observer.verifyCalls, err)
	}

	observer.verifyErr = errs.New(errs.KindStateConflict, "candidate container differs")
	result, err = runtime.executeStep(context.Background(), assignment, step)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired || helper.request != nil {
		t.Fatalf("mismatch result = %#v, helper = %#v, error = %v", result, helper.request, err)
	}
}
