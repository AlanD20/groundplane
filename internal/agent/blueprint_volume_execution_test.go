package agent

import (
	"slices"
	"testing"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Rationale: first-release hooks need owned Docker volumes, but preparing
// those resources must neither observe nonexistent consumers nor start them.
func TestBlueprintVolumePreparationGatesHooksAndConsumers(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "prepared before hook", true: "failure prevents hook and consumer"}[fail],
			func(t *testing.T) {
				response := &agentpb.ComposeHelperResponse{
					Schema: composehelper.SchemaVersion, Outcome: agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_COMPLETED,
					Diagnostic: agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_NONE,
				}
				if fail {
					response.Outcome = agentpb.ComposeHelperOutcome_COMPOSE_HELPER_OUTCOME_FAILED
					response.ExitCode = 17
					response.Diagnostic = agentpb.ComposeHelperDiagnostic_COMPOSE_HELPER_DIAGNOSTIC_COMPOSE_FAILED
				}
				consumer := releaseExecutionSuccess("api", false)
				consumer.Schema = composehelper.SchemaVersion
				runtime := &orderedReleaseRuntime{responses: map[string]*agentpb.ComposeHelperResponse{
					"volume": response, "consumer": consumer,
				}}
				compose, err := testcomposeruntime.New(runtime, blueprintPhaseObserver{runtime})
				if err != nil {
					t.Fatal(err)
				}
				pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
				pool.compose = compose
				pool.SetScriptRuntime(runtime)
				volume := &agentpb.ExecutionStep{StepId: "volume", TimeoutSeconds: 5,
					Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					Payload: &agentpb.ExecutionStep_ManagedVolumeEnsure{
						ManagedVolumeEnsure: &agentpb.ManagedVolumeEnsure{
							ArtifactId: "candidate-artifact", VolumeId: "volume-id",
						},
					},
				}
				hook := releaseHookStep("pre", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK)
				hook.PrerequisiteStepId = volume.StepId
				apply := releaseForwardApply("consumer")
				apply.PrerequisiteStepId = hook.StepId
				result := runReleaseExecution(t, pool, "volume-prefix", "", &agentpb.ExecutionPlan{
					Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
					Steps:     []*agentpb.ExecutionStep{volume, hook, apply},
				})
				want := []string{"compose:volume"}
				if fail {
					if result.Terminal != TaskTerminalFailed || result.ExitCode != 17 ||
						!result.Compose.GetReconciliationRequired() {
						t.Fatalf("failed volume result = %#v", result)
					}
				} else {
					want = append(want, "script:pre", "compose:consumer", "observe:candidate-artifact")
					if result.Terminal != TaskTerminalCompleted {
						t.Fatalf("volume preparation result = %#v", result)
					}
				}
				if !slices.Equal(runtime.events, want) {
					t.Fatalf("runtime effects = %v, want %v", runtime.events, want)
				}
			},
		)
	}
}
