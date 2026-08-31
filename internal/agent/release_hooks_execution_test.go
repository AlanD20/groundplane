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
			"compensate": releaseExecutionSuccess("api", true),
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
	plan := &agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, Steps: []*agentpb.ExecutionStep{
		releaseHookStep("pre", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK),
		releaseForwardSwitch("switch-ok"), releaseForwardSwitch("switch-fail"),
		releaseCompensate("compensate", "switch-ok"),
		releaseHookStep("post", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK),
		releaseHookStep("failure", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FAILURE_HOOK),
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

func releaseHookStep(stepID string, policy agentpb.ExecutionStepPolicy) *agentpb.ExecutionStep {
	return &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 5, Policy: policy,
		Payload: &agentpb.ExecutionStep_RunScript{RunScript: &agentpb.RunScript{ScriptExecutionId: stepID}},
	}
}
