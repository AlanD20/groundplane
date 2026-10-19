package agent

import (
	"context"
	"errors"
	"slices"
	"testing"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	testenvironmentdirectory "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/docker/environmentdirectoryhelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintPrefixHelper struct{ runtime *orderedReleaseRuntime }

type blueprintPhaseObserver struct{ runtime *orderedReleaseRuntime }

func (observer blueprintPhaseObserver) Observe(
	_ context.Context,
	_ *agentpb.ExecutionPlan,
	artifactID string,
) (*agentpb.ObservedProject, error) {
	observer.runtime.record("observe:" + artifactID)
	return releaseObservedProject(), nil
}

func (helper blueprintPrefixHelper) Execute(
	_ context.Context,
	request *agentpb.EnvironmentDirectoryHelperRequest,
) (*agentpb.EnvironmentDirectoryHelperResponse, error) {
	helper.runtime.record("prefix:" + request.StepId)
	return &agentpb.EnvironmentDirectoryHelperResponse{Schema: environmentdirectoryhelper.SchemaVersion}, nil
}

func TestExecuteBlueprintReleaseMissingSetupRuntimePreventsHooks(t *testing.T) {
	runtime := &orderedReleaseRuntime{}
	compose, err := testcomposeruntime.New(runtime, blueprintPhaseObserver{runtime})
	if err != nil {
		t.Fatal(err)
	}
	pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
	pool.compose = compose
	pool.SetScriptRuntime(runtime)
	plan := &agentpb.ExecutionPlan{
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
		Steps: []*agentpb.ExecutionStep{
			{
				StepId:         "volumes",
				TimeoutSeconds: 5,
				Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
					ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{},
				},
			},
			releaseHookStep("pre", agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK),
		},
	}
	result := runReleaseExecution(t, pool, "missing-setup", "", plan)
	if result.Terminal != TaskTerminalFailed || len(runtime.events) != 0 {
		t.Fatalf("result = %#v; side effects = %v", result, runtime.events)
	}
}

func TestExecuteBlueprintReleaseRunsSetupBeforeHooksAndStopsOnHookFailure(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "two services", true: "pre-hook failure"}[fail], func(t *testing.T) {
			runtime := &orderedReleaseRuntime{responses: map[string]*agentpb.ComposeHelperResponse{
				"apply-api":    releaseExecutionSuccess("api", false),
				"apply-worker": releaseExecutionSuccess("worker", false),
			}, scriptErr: map[string]error{}, scriptExit: map[string]int32{}}
			for _, response := range runtime.responses {
				response.Schema = composehelper.SchemaVersion
			}
			if fail {
				runtime.scriptErr["pre-worker"] = errors.New("migration failed")
				runtime.scriptExit["pre-worker"] = 23
			}
			compose, err := testcomposeruntime.New(runtime, blueprintPhaseObserver{runtime})
			if err != nil {
				t.Fatal(err)
			}
			directories, err := testenvironmentdirectory.New(blueprintPrefixHelper{runtime})
			if err != nil {
				t.Fatal(err)
			}
			pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
			pool.compose, pool.environmentDirectories = compose, directories
			pool.SetScriptRuntime(runtime)
			steps := []*agentpb.ExecutionStep{{StepId: "volumes", TimeoutSeconds: 5,
				Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
					ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{},
				},
			}}
			for _, service := range []string{"api", "worker"} {
				hook := releaseHookStep(
					"pre-"+service,
					agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK,
				)
				hook.GetRunScript().ServiceId, hook.GetRunScript().ReleaseId = service, "release-"+service
				steps = append(steps, hook)
			}
			for _, service := range []string{"api", "worker"} {
				apply := releaseForwardApply("apply-" + service)
				apply.GetComposeApply().ServiceIds = []string{service}
				steps = append(steps, apply)
			}
			for _, service := range []string{"api", "worker"} {
				hook := releaseHookStep(
					"post-"+service,
					agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_POST_HOOK,
				)
				hook.GetRunScript().ServiceId, hook.GetRunScript().ReleaseId = service, "release-"+service
				steps = append(steps, hook)
			}
			for _, service := range []string{"api", "worker"} {
				steps = append(steps, &agentpb.ExecutionStep{StepId: "health-" + service, TimeoutSeconds: 5,
					Policy: agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					Payload: &agentpb.ExecutionStep_WaitHealthy{
						WaitHealthy: &agentpb.WaitHealthy{
							ArtifactId: "candidate-artifact",
							ServiceIds: []string{service},
						},
					}})
			}
			for index := 1; index < len(steps); index++ {
				steps[index].PrerequisiteStepId = steps[index-1].StepId
			}
			steps = append(steps, releaseProbe("recovery-only"))
			result := runReleaseExecution(
				t,
				pool,
				"blueprint-phases",
				"",
				&agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, Steps: steps},
			)
			want := []string{"prefix:volumes", "script:pre-api", "script:pre-worker"}
			if fail {
				if result.Terminal != TaskTerminalFailed || result.ExitCode != 23 {
					t.Fatalf("result = %#v", result)
				}
			} else {
				want = append(want, "compose:apply-api", "observe:candidate-artifact", "compose:apply-worker", "observe:candidate-artifact", "script:post-api", "script:post-worker", "observe:candidate-artifact", "observe:candidate-artifact")
				if result.Terminal != TaskTerminalCompleted {
					t.Fatalf("result = %#v; events = %v", result, runtime.events)
				}
			}
			if !slices.Equal(runtime.events, want) {
				t.Fatalf("side effects = %v, want %v", runtime.events, want)
			}
		})
	}
}
