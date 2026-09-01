package agent

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type releaseExecutionHelper struct {
	mu        sync.Mutex
	responses map[string]*agentpb.ComposeHelperResponse
	calls     []releaseExecutionCall
}

type releaseExecutionCall struct {
	taskID string
	stepID string
}

func (helper *releaseExecutionHelper) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	helper.mu.Lock()
	defer helper.mu.Unlock()
	helper.calls = append(helper.calls, releaseExecutionCall{taskID: request.TaskId, stepID: request.StepId})
	return helper.responses[request.StepId], nil
}

// Rationale: switch_back is physical compensation, not a status label. A
// later member failure must restore each prior successful proxy in reverse.
func TestExecuteReleaseCompensatesPriorSuccessfulSwitch(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-api": releaseExecutionSuccess("api", false),
		"switch-worker": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"compensate-api": releaseExecutionSuccess("api", true),
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseForwardSwitch("switch-api"), releaseForwardSwitch("switch-worker"),
		releaseCompensate("compensate-api", "switch-api"),
	}}
	result := runReleaseExecution(t, pool, "task-initial", "", plan)
	if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() ||
		len(result.Compose.GetProxyEvidence()) != 1 || !result.Compose.GetProxyEvidence()[0].GetCompensated() {
		t.Fatalf("release result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-initial", stepID: "switch-api"},
		{taskID: "task-initial", stepID: "switch-worker"},
		{taskID: "task-initial", stepID: "compensate-api"},
	})
}

func TestExecuteReleaseRecreateCompensatesSingletonReplacement(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"apply-worker": {Schema: 1, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED, Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE},
		"fail-next": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"restore-worker": {
			Schema: 1, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
			Diagnostic:       agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
			RecreateEvidence: &agentpb.ServiceRecreateEvidence{ServiceId: "worker", ReleaseId: "baseline", ArtifactId: "prior-artifact", Compensated: true, Target: "singleton"},
		},
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseForwardApply("apply-worker"), releaseForwardSwitch("fail-next"), releaseRecreateCompensate("restore-worker", "apply-worker"),
	}}
	result := runReleaseExecution(t, pool, "task-recreate", "", plan)
	if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() ||
		len(result.Compose.GetRecreateEvidence()) != 1 || !result.Compose.GetRecreateEvidence()[0].GetCompensated() {
		t.Fatalf("recreate result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-recreate", stepID: "apply-worker"},
		{taskID: "task-recreate", stepID: "fail-next"},
		{taskID: "task-recreate", stepID: "restore-worker"},
	})
}

// Rationale: retry reconstruction cannot replay forward mutations. A fresh
// Agent process executes only recover probes, then enabled compensation.
func TestExecuteReleaseRetryAfterRestartRunsRecoveryOnly(t *testing.T) {
	t.Parallel()
	post := releaseHookStep("post-deploy-api", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK)
	post.PrerequisiteStepId = "switch-api"
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_ROLLBACK, Steps: []*agentpb.ExecutionStep{
		releaseForwardSwitch("switch-api"), post, releaseProbe("probe-api"), releaseCompensate("compensate-api", "switch-api"),
	}}
	for _, taskID := range []string{"task-retry-one", "task-retry-two"} {
		helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
			"probe-api": releaseExecutionSuccess("api", false), "compensate-api": releaseExecutionSuccess("api", true),
		}}
		result := runReleaseExecution(t, releaseExecutionPool(t, helper), taskID, "task-original", plan)
		if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() ||
			len(result.Compose.GetProxyEvidence()) != 1 || !result.Compose.GetProxyEvidence()[0].GetCompensated() {
			t.Fatalf("restart retry result = %#v", result)
		}
		assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
			{taskID: taskID, stepID: "probe-api"}, {taskID: taskID, stepID: "compensate-api"},
		})
	}
}

func TestExecuteReleaseRetryCompensatesOnlyTouchedMember(t *testing.T) {
	t.Parallel()
	apiProbe := releaseProbe("probe-api")
	workerProbe := releaseProbe("probe-worker")
	workerProbe.GetServiceProxyProbe().ServiceId = "worker"
	workerProbe.GetServiceProxyProbe().ExpectedTarget = "blue"
	workerProbe.GetServiceProxyProbe().ReleaseId = "release-worker"
	workerProbe.GetServiceProxyProbe().AlternateTarget = "green"
	workerProbe.GetServiceProxyProbe().AlternateReleaseId = "candidate-worker"
	apiCompensate := releaseCompensate("compensate-api", "switch-api")
	workerCompensate := releaseCompensate("compensate-worker", "switch-worker")
	workerCompensate.GetServiceProxyCompensate().ServiceId = "worker"
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-api":         releaseExecutionSuccess("api", false),
		"probe-worker":      releaseExecutionSuccess("worker", false),
		"compensate-api":    releaseExecutionSuccess("api", true),
		"compensate-worker": releaseExecutionSuccess("worker", true),
	}}
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		apiProbe, workerProbe, apiCompensate, workerCompensate,
	}}
	result := runReleaseExecution(t, releaseExecutionPool(t, helper), "task-selective-recovery", "task-original", plan)
	if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() {
		t.Fatalf("selective recovery result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-selective-recovery", stepID: "probe-api"},
		{taskID: "task-selective-recovery", stepID: "probe-worker"},
		{taskID: "task-selective-recovery", stepID: "compensate-api"},
	})
}

func TestExecuteReleaseRestartCompensatesAfterAmbiguousTransitionProbe(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-transition": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 19,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		},
		"restore-prior": releaseExecutionSuccess("api", true),
	}}
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseProbe("probe-transition"), releaseCompensate("restore-prior", ""),
	}}
	result := runReleaseExecution(t, releaseExecutionPool(t, helper), "task-transition-retry", "task-original", plan)
	if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() ||
		len(result.Compose.GetProxyEvidence()) != 1 || !result.Compose.GetProxyEvidence()[0].GetCompensated() {
		t.Fatalf("transition recovery result = %#v", result)
	}
	assertReleaseExecutionCalls(t, helper, []releaseExecutionCall{
		{taskID: "task-transition-retry", stepID: "probe-transition"},
		{taskID: "task-transition-retry", stepID: "restore-prior"},
	})
}

// Rationale: parallel retry Tasks share a WorkerPool but not execution state;
// proxy evidence and step selection stay scoped to the owning Task.
func TestExecuteReleaseConcurrentRetriesKeepEvidenceIsolated(t *testing.T) {
	t.Parallel()
	helper := &releaseExecutionHelper{responses: map[string]*agentpb.ComposeHelperResponse{
		"probe-api": releaseExecutionSuccess("api", false),
	}}
	pool := releaseExecutionPool(t, helper)
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseForwardSwitch("switch-api"), releaseProbe("probe-api"),
	}}
	var wait sync.WaitGroup
	for _, taskID := range []string{"task-a", "task-b"} {
		wait.Add(1)
		go func() {
			defer wait.Done()
			runReleaseExecution(t, pool, taskID, "task-original", plan)
		}()
	}
	wait.Wait()
	helper.mu.Lock()
	defer helper.mu.Unlock()
	seen := map[string]int{}
	for _, call := range helper.calls {
		if call.stepID != "probe-api" {
			t.Fatalf("concurrent retry executed non-recovery step %#v", call)
		}
		seen[call.taskID]++
	}
	if seen["task-a"] != 1 || seen["task-b"] != 1 {
		t.Fatalf("concurrent retry calls = %#v", helper.calls)
	}
}

func releaseExecutionPool(t *testing.T, helper *releaseExecutionHelper) *WorkerPool {
	t.Helper()
	runtime, err := NewComposeRuntime(helper, &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-release"}}})
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.compose = runtime
	return pool
}

func runReleaseExecution(
	t *testing.T,
	pool *WorkerPool,
	taskID string,
	retryOf string,
	plan *agentpb.ExecutionPlan,
) TaskResult {
	t.Helper()
	taskCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	reservation := &taskReservation{assignment: Assignment{
		AssignmentID: "assignment-" + taskID, TaskID: taskID, OperationID: "operation-" + taskID,
		RetryOf: retryOf, Plan: plan, Deadline: time.Now().Add(5 * time.Second),
	}, ctx: taskCtx, cancel: cancel}
	pool.executeRelease(context.Background(), reservation)
	for len(pool.outputs) > 0 {
		output := <-pool.outputs
		if output.Result != nil && output.Result.TaskID == taskID {
			return *output.Result
		}
	}
	t.Fatalf("release execution for %s emitted no result", taskID)
	return TaskResult{}
}

func releaseForwardSwitch(stepID string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5,
		Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ServiceProxySwitch{ServiceProxySwitch: &agentpb.ServiceProxySwitch{}},
	}
}

func releaseForwardApply(stepID string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5, Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{ArtifactId: "candidate-artifact", ServiceIds: []string{"worker"}, ForceRecreate: true, NoDependencies: true}},
	}
}

func releaseProbe(stepID string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5,
		Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_RECOVERY_PROBE,
		Payload: &agentpb.ExecutionStep_ServiceProxyProbe{ServiceProxyProbe: &agentpb.ServiceProxyProbe{
			ServiceId: "api", ExpectedTarget: "green", ReleaseId: "prior-api",
			AlternateTarget: "blue", AlternateReleaseId: "release-api",
		}},
	}
}

func releaseCompensate(stepID string, prerequisite string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, PrerequisiteStepId: prerequisite, TimeoutSeconds: 5,
		Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
		Payload: &agentpb.ExecutionStep_ServiceProxyCompensate{ServiceProxyCompensate: &agentpb.ServiceProxyCompensate{ServiceId: "api", Enabled: true}},
	}
}

func releaseRecreateCompensate(stepID string, prerequisite string) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, PrerequisiteStepId: prerequisite, TimeoutSeconds: 5,
		Policy:  agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_COMPENSATE,
		Payload: &agentpb.ExecutionStep_ServiceRecreateCompensate{ServiceRecreateCompensate: &agentpb.ServiceRecreateCompensate{Enabled: true, ArtifactId: "prior-artifact", ServiceId: "worker", PriorReleaseId: "baseline"}},
	}
}

func releaseExecutionSuccess(serviceID string, compensated bool) *agentpb.ComposeHelperResponse {
	return &agentpb.ComposeHelperResponse{
		Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
		ProxyEvidence: &agentpb.ServiceProxyEvidence{
			ServiceId: serviceID, Target: "blue", ProxyGeneration: 1, ConfigSha256: make([]byte, 32),
			ReleaseId: "release-" + serviceID, Compensated: compensated,
		},
	}
}

func assertReleaseExecutionCalls(t *testing.T, helper *releaseExecutionHelper, want []releaseExecutionCall) {
	t.Helper()
	helper.mu.Lock()
	defer helper.mu.Unlock()
	if len(helper.calls) != len(want) {
		t.Fatalf("release calls = %#v, want %#v", helper.calls, want)
	}
	for index := range want {
		if helper.calls[index] != want[index] {
			t.Fatalf("release calls = %#v, want %#v", helper.calls, want)
		}
	}
}
