package agent

import (
	"errors"
	"testing"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func TestBlueprintPrehookFailureRequiresRestorationBeforeTerminal(t *testing.T) {
	runtime := &orderedReleaseRuntime{
		scriptExit: map[string]int32{"pre": 1},
		scriptErr:  map[string]error{"pre": errors.New("connection check failed")},
	}
	compose, err := NewComposeRuntime(
		runtime,
		&fakeComposeObserver{projects: []*agentpb.ObservedProject{releaseObservedProject()}},
	)
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.compose = compose
	pool.SetScriptRuntime(runtime)
	result := runReleaseExecution(t, pool, "task-blueprint-prehook-failure", "", &agentpb.ExecutionPlan{
		Operation:                 agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		CandidateReleaseProcedure: &agentpb.CandidateReleaseProcedure{},
		Steps: []*agentpb.ExecutionStep{
			releaseHookStep("pre", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK),
			releaseForwardApply("apply"),
			releaseCompensate("compensate", "apply"),
		},
	})
	if result.Terminal != TaskTerminalFailed || result.ExitCode != 1 || !result.Compose.GetReconciliationRequired() {
		t.Fatalf("failed prehook must retain restoration obligation: %#v", result)
	}
	if len(runtime.events) != 1 || runtime.events[0] != "script:pre" {
		t.Fatalf("prehook failure must not start or compensate the untouched candidate: %v", runtime.events)
	}
}
