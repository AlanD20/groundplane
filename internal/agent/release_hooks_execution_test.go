package agent

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

type orderedReleaseRuntime struct {
	mu         sync.Mutex
	events     []string
	responses  map[string]*agentpb.ComposeHelperResponse
	scriptExit map[string]int32
	scriptErr  map[string]error
}

func (runtime *orderedReleaseRuntime) Execute(
	_ context.Context,
	request *agentpb.ComposeHelperRequest,
) (*agentpb.ComposeHelperResponse, error) {
	runtime.record("compose:" + request.StepId)
	return runtime.responses[request.StepId], nil
}

func (runtime *orderedReleaseRuntime) ExecuteScript(
	_ context.Context,
	_ Assignment,
	step *agentpb.ExecutionStep,
	_ func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) (int32, error) {
	runtime.record("script:" + step.StepId)
	return runtime.scriptExit[step.StepId], runtime.scriptErr[step.StepId]
}

func (runtime *orderedReleaseRuntime) CompleteScriptWithoutStart(
	_ context.Context,
	_ Assignment,
	step *agentpb.ExecutionStep,
	reason agentpb.ScriptOutcomeReason,
	_ func(context.Context, *agentpb.ScriptCheckpointRequest) error,
) error {
	runtime.record("script-without-start:" + step.StepId + ":" + reason.String())
	return nil
}

func (runtime *orderedReleaseRuntime) record(event string) {
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	runtime.events = append(runtime.events, event)
}

func TestExecuteReleaseHooksRespectPhasesAndPreservePrimaryFailure(t *testing.T) {
	runtime := &orderedReleaseRuntime{
		responses: map[string]*agentpb.ComposeHelperResponse{
			"switch-ok": releaseExecutionSuccess("api", false),
			"switch-fail": {
				Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
				Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
			},
			"compensate": {
				Outcome:    agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
				Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
				ProxyEvidence: &agentpb.ServiceProxyEvidence{
					ServiceId: "api", Target: "green", ReleaseId: "prior-api", Compensated: true,
				},
			},
		},
		scriptExit: map[string]int32{"failure": 29},
		scriptErr:  map[string]error{"failure": errors.New("secondary failure hook error")},
	}
	compose, err := NewComposeRuntime(runtime, &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-release"}}})
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.compose = compose
	pool.SetScriptRuntime(runtime)
	post := releaseHookStep("post", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK)
	post.PrerequisiteStepId = "switch-fail"
	failure := releaseHookStep("failure", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK)
	failure.GetRunScript().ReleaseId = "prior-api"
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseHookStep("pre", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK),
		releaseForwardSwitch("switch-ok"), releaseForwardSwitch("switch-fail"),
		releaseCompensate("compensate", "switch-ok"),
		post,
		failure,
	}}
	result := runReleaseExecution(t, pool, "task-hooks", "", plan)
	if result.Terminal != TaskTerminalFailed || result.ExitCode != 17 {
		t.Fatalf("release hook result = %#v", result)
	}
	want := []string{"script:pre", "compose:switch-ok", "compose:switch-fail", "compose:compensate", "script:failure"}
	if len(runtime.events) != len(want) {
		t.Fatalf("release hook events = %#v, want %#v", runtime.events, want)
	}
	for index := range want {
		if runtime.events[index] != want[index] {
			t.Fatalf("release hook events = %#v, want %#v", runtime.events, want)
		}
	}
}

func TestExecuteReleaseFailureHookRequiresMatchingServingEvidence(t *testing.T) {
	failureResponse := &agentpb.ComposeHelperResponse{
		Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
		Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
	}
	tests := []struct {
		name          string
		responses     map[string]*agentpb.ComposeHelperResponse
		releaseID     string
		wantLastEvent string
		wantReconcile bool
	}{
		{
			name: "matching candidate executes",
			responses: map[string]*agentpb.ComposeHelperResponse{
				"activate": releaseExecutionSuccess("api", false), "fail": failureResponse,
			},
			releaseID: "release-api", wantLastEvent: "script:failure",
		},
		{
			name:          "absent serving evidence records no serving release",
			responses:     map[string]*agentpb.ComposeHelperResponse{"fail": failureResponse},
			releaseID:     "release-api",
			wantLastEvent: "script-without-start:failure:SCRIPT_OUTCOME_REASON_NO_SERVING_RELEASE",
		},
		{
			name: "different serving release fails closed",
			responses: map[string]*agentpb.ComposeHelperResponse{
				"activate": releaseExecutionSuccess("api", false), "fail": failureResponse,
			},
			releaseID: "different-release", wantReconcile: true,
			wantLastEvent: "script-without-start:failure:SCRIPT_OUTCOME_REASON_RECOVERY_INVARIANT_FAILURE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &orderedReleaseRuntime{responses: test.responses}
			compose, err := NewComposeRuntime(runtime, &fakeComposeObserver{
				projects: []*agentpb.ObservedProject{{ProjectName: "gp-release"}},
			})
			if err != nil {
				t.Fatal(err)
			}
			pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
			pool.compose = compose
			pool.SetScriptRuntime(runtime)
			failure := releaseHookStep("failure", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK)
			failure.GetRunScript().ReleaseId = test.releaseID
			steps := []*agentpb.ExecutionStep{}
			if _, exists := test.responses["activate"]; exists {
				steps = append(steps, releaseForwardSwitch("activate"))
			}
			steps = append(steps, releaseForwardSwitch("fail"), failure)
			result := runReleaseExecution(t, pool, "task-failure-evidence", "", &agentpb.ExecutionPlan{
				Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: steps,
			})
			if result.Terminal != TaskTerminalFailed ||
				result.Compose.GetReconciliationRequired() != test.wantReconcile {
				t.Fatalf("release result = %#v", result)
			}
			if len(runtime.events) == 0 || runtime.events[len(runtime.events)-1] != test.wantLastEvent {
				t.Fatalf("release events = %#v, want final %q", runtime.events, test.wantLastEvent)
			}
		})
	}
}

func TestExecuteReleaseCompensationFailureSuppressesFailureHooks(t *testing.T) {
	runtime := &orderedReleaseRuntime{responses: map[string]*agentpb.ComposeHelperResponse{
		"switch-ok": releaseExecutionSuccess("api", false),
		"switch-fail": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 17,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPONENT_ACTIVATION_FAILED,
		},
		"compensate": {
			Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED, ExitCode: 31,
			Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED,
		},
	}}
	compose, err := NewComposeRuntime(runtime, &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-release"}}})
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.compose = compose
	pool.SetScriptRuntime(runtime)
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseForwardSwitch("switch-ok"), releaseForwardSwitch("switch-fail"),
		releaseCompensate("compensate", "switch-ok"),
		releaseHookStep("failure", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK),
	}}
	result := runReleaseExecution(t, pool, "task-compensation-failure", "", plan)
	if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() || result.ExitCode != 17 {
		t.Fatalf("compensation failure result = %#v", result)
	}
	want := []string{"compose:switch-ok", "compose:switch-fail", "compose:compensate"}
	if len(runtime.events) != len(want) {
		t.Fatalf("release hook events = %#v, want %#v", runtime.events, want)
	}
	for index := range want {
		if runtime.events[index] != want[index] {
			t.Fatalf("release hook events = %#v, want %#v", runtime.events, want)
		}
	}
}

func TestExecuteReleaseRunsPostDeployAfterCandidateStartBeforeReadiness(t *testing.T) {
	// Rationale: readiness may depend on a post-deploy migration, so it cannot
	// precede the Script runner.
	tests := []struct {
		name      string
		start     string
		readiness string
		finalize  string
	}{
		{name: "blue-green", start: "candidate-apply", readiness: "candidate-readiness", finalize: "proxy-switch"},
		{name: "recreate", start: "candidate-start", readiness: "candidate-readiness", finalize: "recreate-acknowledge"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime := &orderedReleaseRuntime{responses: map[string]*agentpb.ComposeHelperResponse{
				test.start: releaseExecutionSuccess("api", false), test.readiness: releaseExecutionSuccess("api", false),
				test.finalize: releaseExecutionSuccess("api", false),
			}}
			compose, err := NewComposeRuntime(runtime, &fakeComposeObserver{projects: []*agentpb.ObservedProject{{ProjectName: "gp-release"}}})
			if err != nil {
				t.Fatalf("NewComposeRuntime() error = %v", err)
			}
			pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
			pool.compose = compose
			pool.SetScriptRuntime(runtime)
			post := releaseHookStep("post-deploy", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK)
			post.PrerequisiteStepId = test.start
			plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
				releaseForwardSwitch(test.start), releaseForwardSwitch(test.readiness), releaseForwardSwitch(test.finalize), post,
			}}
			result := runReleaseExecution(t, pool, "task-"+test.name, "", plan)
			if result.Terminal != TaskTerminalCompleted {
				t.Fatalf("release result = %#v", result)
			}
			want := []string{
				"compose:" + test.start, "script:post-deploy",
				"compose:" + test.readiness, "compose:" + test.finalize,
			}
			if len(runtime.events) != len(want) {
				t.Fatalf("release events = %#v, want %#v", runtime.events, want)
			}
			for index := range want {
				if runtime.events[index] != want[index] {
					t.Fatalf("release events = %#v, want %#v", runtime.events, want)
				}
			}
		})
	}
}

func releaseHookStep(stepID string, policy agentpb.ExecutionStepPolicy) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5, Policy: policy,
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{
			ScriptExecutionId: stepID, ServiceId: "api", ReleaseId: "release-api",
		}},
	}
}
