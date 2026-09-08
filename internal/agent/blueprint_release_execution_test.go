package agent

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

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
	return &agentpb.EnvironmentDirectoryHelperResponse{Schema: environmentDirectoryHelperSchema}, nil
}

func TestExecuteBlueprintReleaseMissingSetupRuntimePreventsHooks(t *testing.T) {
	runtime := &orderedReleaseRuntime{}
	compose, err := NewComposeRuntime(runtime, blueprintPhaseObserver{runtime})
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
				response.Schema = composeHelperSchema
			}
			if fail {
				runtime.scriptErr["pre-worker"] = errors.New("migration failed")
				runtime.scriptExit["pre-worker"] = 23
			}
			compose, err := NewComposeRuntime(runtime, blueprintPhaseObserver{runtime})
			if err != nil {
				t.Fatal(err)
			}
			directories, err := NewEnvironmentDirectoryRuntime(blueprintPrefixHelper{runtime})
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

func TestExecuteBlueprintReleaseWaitsForRealScriptCleanupAcknowledgement(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "cleanup before candidate", true: "failed runner cleaned without candidate"}[fail],
			func(t *testing.T) {
				assignment, hook, digest := capturedContainerFailureFixture()
				assignment.Plan.Operation = agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY
				assignment.Plan.ScriptRunnerSnapshots[0].LocalImageId = "sha256:" + strings.Repeat("b", 64)
				assignment.Plan.ScriptRunnerProjections[0].Image = assignment.Plan.ScriptRunnerSnapshots[0].LocalImageId
				hook.Policy, hook.TimeoutSeconds = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_PRE_HOOK, 5
				prefix := &agentpb.ExecutionStep{
					StepId:         "volumes",
					TimeoutSeconds: 5,
					Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					Payload: &agentpb.ExecutionStep_ManagedVolumeDirectoriesEnsure{
						ManagedVolumeDirectoriesEnsure: &agentpb.ManagedVolumeDirectoriesEnsure{},
					},
				}
				hook.PrerequisiteStepId = prefix.StepId
				apply := releaseForwardApply("candidate")
				apply.PrerequisiteStepId = hook.StepId
				assignment.Plan.Steps = []*agentpb.ExecutionStep{prefix, hook, apply}
				assignment.Plan.Artifacts = []*agentpb.ComposeArtifact{releaseTestArtifact("candidate-artifact")}
				events := []string{}
				engine := &checkpointOrderScriptEngine{events: &events, bodyDigest: digest}
				if fail {
					engine.runErr = errors.New("migration failed")
				}
				scripts, err := NewDockerScriptRuntime(engine)
				if err != nil {
					t.Fatal(err)
				}
				response := releaseExecutionSuccess("worker", false)
				response.Schema = composeHelperSchema
				runtime := &orderedReleaseRuntime{
					responses: map[string]*agentpb.ComposeHelperResponse{"candidate": response},
				}
				compose, err := NewComposeRuntime(
					runtime,
					&fakeComposeObserver{projects: []*agentpb.ObservedProject{releaseObservedProject()}},
				)
				if err != nil {
					t.Fatal(err)
				}
				directories, err := NewEnvironmentDirectoryRuntime(blueprintPrefixHelper{runtime})
				if err != nil {
					t.Fatal(err)
				}
				pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
				pool.compose, pool.environmentDirectories = compose, directories
				pool.SetScriptRuntime(scripts)
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				reservation := &taskReservation{assignment: assignment, ctx: ctx, cancel: cancel}
				done := make(chan struct{})
				go func() { defer close(done); pool.executeRelease(ctx, reservation) }()
				defer func() { cancel(); <-done }()
				resultTimeout := time.NewTimer(5 * time.Second)
				defer resultTimeout.Stop()
				for {
					select {
					case output := <-pool.outputs:
						if request := output.ScriptCheckpoint; request != nil {
							runtime.record("ack:" + request.State.String())
							err := pool.AcceptScriptCheckpointAck(&agentpb.ScriptCheckpointAck{
								TaskId: request.TaskId, OperationId: request.OperationId, AssignmentId: request.AssignmentId,
								StepId: request.StepId, ScriptExecutionId: request.ScriptExecutionId, State: request.State,
								ControlPayloadSha256: append([]byte(nil), request.ControlPayloadSha256...),
							})
							if err != nil {
								t.Fatal(err)
							}
						}
						if output.Result == nil {
							continue
						}
						<-done
						wantTerminal := TaskTerminalCompleted
						if fail {
							wantTerminal = TaskTerminalFailed
						}
						if output.Result.Terminal != wantTerminal {
							t.Fatalf("result = %#v; events = %v", output.Result, runtime.events)
						}
						want := []string{
							"prefix:volumes",
							"ack:SCRIPT_EXECUTION_STATE_START_AUTHORIZED",
							"ack:SCRIPT_EXECUTION_STATE_BODY_PREPARED",
							"ack:SCRIPT_EXECUTION_STATE_CONTAINER_CREATED",
							"ack:SCRIPT_EXECUTION_STATE_OUTCOME_RECORDED",
							"ack:SCRIPT_EXECUTION_STATE_CLEANUP_PROVEN",
						}
						if !fail {
							want = append(want, "compose:candidate")
						}
						if !slices.Equal(runtime.events, want) {
							t.Fatalf("side effects and ACKs = %v, want %v", runtime.events, want)
						}
						if !slices.Equal(
							events,
							[]string{
								"engine:prepare",
								"engine:recover",
								"engine:create",
								"engine:run",
								"engine:cleanup",
							},
						) {
							t.Fatalf("engine calls = %v", events)
						}
						return
					case <-resultTimeout.C:
						t.Fatal("executor did not complete")
					}
				}
			},
		)
	}
}
