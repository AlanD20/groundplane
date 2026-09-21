package agent

import (
	"context"
	"errors"
	"testing"

	testcomposeruntime "github.com/AlanD20/groundplane/internal/agent/composeruntime"
	testtaskassignment "github.com/AlanD20/groundplane/internal/agent/taskassignment"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: the lifecycle dispatcher must enforce the complete sealed
// runtime footprint; the generic stopped-services check would accept a
// partially observed Service when every remaining container is stopped.
func TestComposeRuntimeDispatchesServiceLifecycleStopFootprint(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifactID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:  artifactID,
		ProjectName: "gp-env",
		Services: []*agentpb.ComposeService{
			{
				ServiceId:        serviceID,
				ComposeName:      "api",
				ExpectedReplicas: 1,
				ExpectedLabels:   lifecycleLabels(serviceID, "proxy"),
			},
			{ServiceId: serviceID, ComposeName: "api--singleton", ExpectedReplicas: 2,
				ExpectedLabels: lifecycleLabels(serviceID, "singleton")},
		},
	}
	step := &agentpb.ExecutionStep{
		StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
			ArtifactId: artifactID, ServiceIds: []string{serviceID}, GraceSeconds: 30,
		}},
	}
	observed := &agentpb.ObservedProject{ProjectName: "gp-env", Containers: []*agentpb.ObservedContainer{
		lifecycleContainer(serviceID, "proxy", agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED),
	}}
	helper := completedComposeHelper()
	runtime, err := testcomposeruntime.New(helper, &fakeComposeObserver{projects: []*agentpb.ObservedProject{observed}})
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	assignment := testtaskassignment.Assignment{
		TaskID: "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: &agentpb.ExecutionPlan{
			TargetId: serviceID, Artifacts: []*agentpb.ComposeArtifact{artifact},
			ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{Sources: []*agentpb.ServiceLifecycleSource{{
				ArtifactId: artifactID, ServiceId: serviceID, ComposeNames: []string{"api", "api--singleton"}, StepId: step.StepId,
			}}},
		},
	}
	for _, container := range observed.Containers {
		if container.GetState() != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_EXITED {
			t.Fatalf("fixture container is not stopped: %#v", container)
		}
	}
	result, err := runtime.ExecuteStep(context.Background(), assignment, step)
	if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired {
		t.Fatalf("executeStep() result = %#v, error = %v; expected sealed-footprint conflict", result, err)
	}
}

// Rationale: Compose reports a newly started healthy service as starting on
// the first observation; lifecycle execution must poll that sealed workload
// after one mutation instead of terminalizing the task during warm-up.
func TestComposeRuntimePollsServiceLifecycleStartUntilHealthy(t *testing.T) {
	serviceID := "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifactID := "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	stepID := "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	artifact := &agentpb.ComposeArtifact{
		ArtifactId:  artifactID,
		ProjectName: "gp-env",
		Services: []*agentpb.ComposeService{{
			ServiceId: serviceID, ComposeName: "api--singleton", ExpectedReplicas: 1, HasHealthcheck: true,
			ExpectedLabels: lifecycleLabels(serviceID, "singleton"),
		}},
	}
	step := &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, ServiceIds: []string{serviceID},
		}},
	}
	startingContainer := lifecycleContainer(
		serviceID, "singleton", agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING,
	)
	startingContainer.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING
	healthyContainer := proto.Clone(startingContainer).(*agentpb.ObservedContainer)
	healthyContainer.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY
	observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{{
		ProjectName: "gp-env", Containers: []*agentpb.ObservedContainer{startingContainer},
	}, {
		ProjectName: "gp-env", Containers: []*agentpb.ObservedContainer{healthyContainer},
	}}}
	helper := completedComposeHelper()
	runtime, err := testcomposeruntime.New(helper, observer)
	if err != nil {
		t.Fatalf("NewComposeRuntime() error = %v", err)
	}
	assignment := testtaskassignment.Assignment{
		TaskID: "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: &agentpb.ExecutionPlan{
			TargetId: serviceID, Artifacts: []*agentpb.ComposeArtifact{artifact},
			ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{Sources: []*agentpb.ServiceLifecycleSource{{
				ArtifactId: artifactID, ServiceId: serviceID, ComposeNames: []string{"api--singleton"}, StepId: stepID,
			}}},
		},
	}
	result, err := runtime.ExecuteStep(context.Background(), assignment, step)
	if err != nil {
		t.Fatalf("executeStep() error = %v", err)
	}
	if helper.request == nil || observer.calls != 2 || result.Observed == nil || result.ReconciliationRequired {
		t.Fatalf(
			"executeStep() result = %#v, helper=%#v, observations=%d; want one mutation and warm-up poll",
			result,
			helper.request,
			observer.calls,
		)
	}
}

func lifecycleLabels(serviceID, role string) []*agentpb.LabelPair {
	return []*agentpb.LabelPair{
		{Key: "com.groundplane.runtime-role", Value: role},
		{Key: "com.groundplane.service-id", Value: serviceID},
	}
}

func lifecycleContainer(
	serviceID, role string,
	state agentpb.ObservedContainerState,
) *agentpb.ObservedContainer {
	return &agentpb.ObservedContainer{ServiceId: serviceID, State: state, Labels: lifecycleLabels(serviceID, role)}
}
