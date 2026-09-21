package agent

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	testcomponentaction "github.com/AlanD20/groundplane/internal/agent/componentaction"
	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	testenvironmentdirectory "github.com/AlanD20/groundplane/internal/agent/environmentdirectory"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintComponentRuntime struct {
	runtime *orderedReleaseRuntime
	result  *testcomponentaction.ComponentActionResult
	err     error
}

func TestExecuteBlueprintComponentRequiresAcceptedRunningEvent(t *testing.T) {
	for _, accept := range []bool{false, true} {
		t.Run(
			map[bool]string{false: "unaccepted event prevents action", true: "accepted event permits action"}[accept],
			func(t *testing.T) {
				runtime := &orderedReleaseRuntime{}
				compose, err := testcomposeruntime.New(runtime, blueprintPhaseObserver{runtime})
				if err != nil {
					t.Fatal(err)
				}
				pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
				pool.compose = compose
				pool.componentActions = blueprintComponentRuntime{runtime: runtime}
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				step := &agentpb.ExecutionStep{
					StepId:         "activate",
					TimeoutSeconds: 5,
					Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
					Payload:        &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{}},
				}
				reservation := &taskReservation{
					eventsDurable: true,
					ctx:           ctx,
					cancel:        cancel,
					assignment: testtaskassignment.Assignment{
						AssignmentID:   "assignment",
						TaskID:         "task",
						ExecutionEpoch: 1,
						Plan: &agentpb.ExecutionPlan{
							Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
							Steps:     []*agentpb.ExecutionStep{step},
						},
					},
				}
				done := make(chan struct{})
				go func() { defer close(done); pool.executeRelease(context.Background(), reservation) }()
				defer func() { cancel(); <-done }()
				var progress *TaskProgress
				select {
				case output := <-pool.outputs:
					progress = output.Progress
				case <-ctx.Done():
					t.Fatal("missing running event")
				}
				if progress == nil || progress.State != TaskProgressRunning {
					t.Fatal("first output was not running event")
				}
				if accept {
					runtime.record("accept:running")
					if err := pool.AcceptTaskEventAck(ctx, &agentpb.TaskEventAck{TaskId: progress.TaskID, AssignmentId: progress.AssignmentID, StepId: progress.StepID, PlanHash: progress.PlanHash[:], ExecutionEpoch: progress.ExecutionEpoch, Ordinal: progress.Ordinal, State: agentpb.TaskState_TASK_STATE_RUNNING}); err != nil {
						t.Fatal(err)
					}
				} else {
					cancel()
				}
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("executor did not finish")
				}
				want := []string{}
				if accept {
					want = []string{"accept:running", "component:activate"}
				}
				if !slices.Equal(runtime.events, want) {
					t.Fatalf("effects = %v, want %v", runtime.events, want)
				}
			},
		)
	}
}

func (runtime blueprintComponentRuntime) ExecuteComponentAction(
	_ context.Context,
	_ testtaskassignment.Assignment,
	step *agentpb.ExecutionStep,
	payload testcomponentaction.ManagedConfigPayload,
) (*testcomponentaction.ComponentActionResult, error) {
	runtime.runtime.record("component:" + step.StepId)
	if payload.Source != nil {
		return nil, errors.New("unexpected managed payload")
	}
	return runtime.result, runtime.err
}

func (runtime blueprintComponentRuntime) FinalizeManagedConfig(
	context.Context, testtaskassignment.Assignment,

	*agentpb.ExecutionStep,
	bool,
) (testcomponentaction.ManagedConfigTransactionState, error) {
	runtime.runtime.record("forbidden:finalize")
	return testcomponentaction.ManagedConfigTransactionState{}, errors.New("host-only finalization")
}

func TestExecuteBlueprintComponentAfterHealthPreservesUncertainEffects(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure after workload restoration"}[fail], func(t *testing.T) {
			runtime := &orderedReleaseRuntime{responses: map[string]*agentpb.ComposeHelperResponse{
				"apply": releaseExecutionSuccess("api", false), "restore": releaseExecutionSuccess("api", true),
			}}
			for _, response := range runtime.responses {
				response.Schema = composehelper.SchemaVersion
			}
			compose, err := testcomposeruntime.New(runtime, blueprintPhaseObserver{runtime})
			if err != nil {
				t.Fatal(err)
			}
			pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
			pool.compose = compose
			actionRuntime := blueprintComponentRuntime{runtime: runtime}
			if fail {
				actionRuntime.err = errors.New("reload outcome unknown")
			}
			pool.componentActions = actionRuntime
			apply := releaseForwardApply("apply")
			apply.GetComposeApply().ServiceIds = []string{"api"}
			health := &agentpb.ExecutionStep{
				StepId:             "health",
				PrerequisiteStepId: "apply",
				TimeoutSeconds:     5,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload: &agentpb.ExecutionStep_WaitHealthy{
					WaitHealthy: &agentpb.WaitHealthy{ArtifactId: "candidate-artifact", ServiceIds: []string{"api"}},
				},
			}
			action := &agentpb.ExecutionStep{
				StepId:             "activate",
				PrerequisiteStepId: "health",
				TimeoutSeconds:     5,
				Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload:            &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{}},
			}
			result := runReleaseExecution(
				t,
				pool,
				"blueprint-component",
				"",
				&agentpb.ExecutionPlan{
					Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
					Steps:     []*agentpb.ExecutionStep{apply, health, releaseCompensate("restore", "apply"), action},
				},
			)
			want := []string{
				"compose:apply",
				"observe:candidate-artifact",
				"observe:candidate-artifact",
				"component:activate",
			}
			if fail {
				want = append(want, "compose:restore", "observe:prior-artifact")
				if result.Terminal != TaskTerminalFailed || !result.Compose.GetReconciliationRequired() {
					t.Fatalf("result = %#v / %v", result, result.Compose)
				}
			} else if result.Terminal != TaskTerminalCompleted || result.Compose.GetReconciliationRequired() {
				t.Fatalf("result = %#v / %v", result, result.Compose)
			}
			if !slices.Equal(runtime.events, want) {
				t.Fatalf("side effects = %v, want %v", runtime.events, want)
			}
		})
	}
}

func TestExecuteBlueprintComponentRejectsHostLifecycleAuthorityAndEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		action *agentpb.ComponentApply
		mode   agentpb.ComponentLifecycleMode
		result *testcomponentaction.ComponentActionResult
		called bool
	}{
		{name: "managed content", action: &agentpb.ComponentApply{ManagedConfigContent: true}},
		{name: "previous artifact", action: &agentpb.ComponentApply{ExpectedPreviousArtifactId: "host-artifact"}},
		{name: "previous digest", action: &agentpb.ComponentApply{ExpectedPreviousArtifactDigest: []byte("digest")}},
		{name: "previous generation", action: &agentpb.ComponentApply{ExpectedPreviousGeneration: 1}},
		{name: "host mode", action: &agentpb.ComponentApply{}, mode: agentpb.ComponentLifecycleMode_COMPONENT_LIFECYCLE_MODE_ENABLE},
		{name: "DNS evidence", action: &agentpb.ComponentApply{}, result: &testcomponentaction.ComponentActionResult{DNSResolverObservation: &agentpb.DNSResolverObservationEvidence{}}, called: true},
		{name: "managed evidence", action: &agentpb.ComponentApply{}, result: &testcomponentaction.ComponentActionResult{ManagedConfig: &testcomponentaction.ManagedConfigTransactionState{}}, called: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime := &orderedReleaseRuntime{}
			compose, err := testcomposeruntime.New(runtime, blueprintPhaseObserver{runtime})
			if err != nil {
				t.Fatal(err)
			}
			pool := NewWorkerPool(64, "/var/lib/groundplane/volumes", nil, nil)
			pool.compose = compose
			pool.componentActions = blueprintComponentRuntime{runtime: runtime, result: test.result}
			step := &agentpb.ExecutionStep{
				StepId:         "activate",
				TimeoutSeconds: 5,
				Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
				Payload:        &agentpb.ExecutionStep_ComponentApply{ComponentApply: test.action},
			}
			result := runReleaseExecution(
				t,
				pool,
				"host-action",
				"",
				&agentpb.ExecutionPlan{
					Operation:              agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY,
					ComponentLifecycleMode: test.mode,
					Steps:                  []*agentpb.ExecutionStep{step},
				},
			)
			if result.Terminal != TaskTerminalFailed || result.Compose.GetReconciliationRequired() != test.called {
				t.Fatalf("terminal/recovery = %v/%v", result.Terminal, result.Compose.GetReconciliationRequired())
			}
			want := []string{}
			if test.called {
				want = append(want, "component:activate")
			}
			if !slices.Equal(runtime.events, want) {
				t.Fatalf("side effects = %v, want %v", runtime.events, want)
			}
		})
	}
}

func TestExecuteBlueprintEnvironmentDirectoryBeforeComponent(t *testing.T) {
	runtime := &orderedReleaseRuntime{}
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
	pool.componentActions = blueprintComponentRuntime{runtime: runtime}
	steps := []*agentpb.ExecutionStep{
		{
			StepId:         "directory",
			TimeoutSeconds: 5,
			Policy:         agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryCreate{
				EnvironmentDirectoryCreate: &agentpb.EnvironmentDirectoryCreate{},
			},
		},
		{
			StepId:             "activate",
			PrerequisiteStepId: "directory",
			TimeoutSeconds:     5,
			Policy:             agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD,
			Payload:            &agentpb.ExecutionStep_ComponentApply{ComponentApply: &agentpb.ComponentApply{}},
		},
	}
	result := runReleaseExecution(
		t,
		pool,
		"environment-setup",
		"",
		&agentpb.ExecutionPlan{Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, Steps: steps},
	)
	if result.Terminal != TaskTerminalCompleted ||
		!slices.Equal(runtime.events, []string{"prefix:directory", "component:activate"}) {
		t.Fatalf("terminal/effects = %v/%v", result.Terminal, runtime.events)
	}
}
