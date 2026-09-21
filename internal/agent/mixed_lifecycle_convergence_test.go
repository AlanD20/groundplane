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

// Rationale: starting one native Service in a shared Compose project must
// converge through warm-up despite an unrelated Component ownership collision,
// while collision evidence for the sealed native target remains fail-closed.
func TestComposeRuntimeServiceLifecycleStartScopesMixedProjectCollisions(t *testing.T) {
	const (
		serviceID  = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		stepID     = "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	artifact := &agentpb.ComposeArtifact{
		ArtifactId: artifactID, ProjectName: "gp-env",
		Services: []*agentpb.ComposeService{
			{ServiceId: serviceID, ComposeName: "api", ExpectedReplicas: 1,
				Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY,
				ExpectedLabels: lifecycleLabels(serviceID, "proxy")},
			{ServiceId: serviceID, ComposeName: "api--singleton", ExpectedReplicas: 2, HasHealthcheck: true,
				Role:           agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_RECREATE_SINGLETON,
				ExpectedLabels: lifecycleLabels(serviceID, "singleton")},
		},
	}
	step := &agentpb.ExecutionStep{
		StepId: stepID, TimeoutSeconds: 30,
		Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, ServiceIds: []string{serviceID},
		}},
	}
	assignment := testtaskassignment.Assignment{
		TaskID: "tsk_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Plan: &agentpb.ExecutionPlan{
			TargetId: serviceID, Artifacts: []*agentpb.ComposeArtifact{artifact},
			ServiceLifecycleProcedure: &agentpb.ServiceLifecycleProcedure{Sources: []*agentpb.ServiceLifecycleSource{{
				ArtifactId: artifactID, ServiceId: serviceID,
				ComposeNames: []string{"api", "api--singleton"}, StepId: stepID,
			}}},
		},
	}
	warming := &agentpb.ObservedProject{
		ProjectName: "gp-env",
		Containers: []*agentpb.ObservedContainer{
			lifecycleContainer(serviceID, "proxy", agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING),
			lifecycleContainer(serviceID, "singleton", agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING),
			lifecycleContainer(serviceID, "singleton", agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING),
		},
		Collisions: []*agentpb.ObservedCollision{{
			Kind:               agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER,
			ComposeServiceName: "caddy", ServiceId: "cmp_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		}},
	}
	for _, container := range warming.GetContainers()[1:] {
		container.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_STARTING
	}
	healthy := proto.Clone(warming).(*agentpb.ObservedProject)
	for _, container := range healthy.GetContainers()[1:] {
		container.Health = agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY
	}

	// Rationale: a later sibling Zone in the shared Compose project cannot
	// enlarge a native Start's immutable applied dependency selection.
	t.Run("later sibling network outside applied artifact", func(t *testing.T) {
		observed := proto.CloneOf(healthy)
		observed.Collisions = append(observed.Collisions, &agentpb.ObservedCollision{
			Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK,
			Name: "gp_net_net_01M1V6VJ2Z6E1PWY17MMR17YHH",
		})
		helper := completedComposeHelper()
		runtime, err := testcomposeruntime.New(helper, &fakeComposeObserver{
			projects: []*agentpb.ObservedProject{observed},
		})
		if err != nil {
			t.Fatal(err)
		}
		result, err := runtime.ExecuteStep(context.Background(), assignment, step)
		if err != nil || result.ReconciliationRequired {
			t.Fatalf(
				"native Start with healthy selected footprint and later sibling network: result=%#v, error=%v",
				result,
				err,
			)
		}
	})

	// Rationale: lifecycle scoping must still reject foreign selected networks,
	// using either sealed identity, and evidence without an identifiable name.
	for _, networkName := range []string{"gp_net_frontend", "frontend", ""} {
		t.Run("selected or unnamed network "+networkName, func(t *testing.T) {
			selected := assignment
			selected.Plan = proto.CloneOf(assignment.Plan)
			selected.Plan.Artifacts[0].Networks = []*agentpb.ComposeNetwork{{
				ComposeName: "frontend", DockerName: "gp_net_frontend",
			}}
			observed := proto.CloneOf(healthy)
			observed.Collisions = append(observed.Collisions, &agentpb.ObservedCollision{
				Kind: agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_NETWORK, Name: networkName,
			})
			runtime, err := testcomposeruntime.New(completedComposeHelper(), &fakeComposeObserver{
				projects: []*agentpb.ObservedProject{observed},
			})
			if err != nil {
				t.Fatal(err)
			}
			result, err := runtime.ExecuteStep(context.Background(), selected, step)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired {
				t.Fatalf(
					"native Start accepted selected or unnamed network collision: result=%#v, error=%v",
					result,
					err,
				)
			}
		})
	}

	t.Run("unrelated Component collision", func(t *testing.T) {
		helper := completedComposeHelper()
		observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{warming, healthy}}
		runtime, err := testcomposeruntime.New(helper, observer)
		if err != nil {
			t.Fatalf("NewComposeRuntime() error = %v", err)
		}
		result, err := runtime.ExecuteStep(context.Background(), assignment, step)
		if err != nil {
			t.Fatalf("executeStep() error = %v", err)
		}
		if helper.request == nil || observer.calls != 2 || result.Observed != healthy || result.ReconciliationRequired {
			t.Fatalf(
				"executeStep() result = %#v, mutation=%t, observations=%d",
				result,
				helper.request != nil,
				observer.calls,
			)
		}
	})

	t.Run("selected native collision", func(t *testing.T) {
		selectedCollision := proto.Clone(warming).(*agentpb.ObservedProject)
		selectedCollision.Collisions[0].ComposeServiceName = "api--singleton"
		selectedCollision.Collisions[0].ServiceId = serviceID
		helper := completedComposeHelper()
		runtime, err := testcomposeruntime.New(
			helper,
			&fakeComposeObserver{projects: []*agentpb.ObservedProject{selectedCollision}},
		)
		if err != nil {
			t.Fatalf("NewComposeRuntime() error = %v", err)
		}
		result, err := runtime.ExecuteStep(context.Background(), assignment, step)
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired ||
			helper.request == nil {
			t.Fatalf("executeStep() result = %#v, error = %v; want selected ownership conflict", result, err)
		}
	})

	// Rationale: a renamed extra container still claims the selected stable
	// Service id and must prevent Start from accepting its healthy footprint.
	t.Run("renamed target collision during health poll", func(t *testing.T) {
		collision := proto.CloneOf(healthy)
		collision.Collisions[0].ServiceId = serviceID
		helper := completedComposeHelper()
		observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{warming, collision}}
		runtime, err := testcomposeruntime.New(helper, observer)
		if err != nil {
			t.Fatalf("NewComposeRuntime() error = %v", err)
		}
		result, err := runtime.ExecuteStep(context.Background(), assignment, step)
		if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired {
			t.Fatalf("executeStep() result = %#v, error = %v; want renamed target conflict", result, err)
		}
		if helper.request == nil || observer.calls != 2 {
			t.Fatalf(
				"mutation=%t, observations=%d; want collision detected during health poll",
				helper.request != nil,
				observer.calls,
			)
		}
	})

	// Rationale: Stop and Destroy must detect an extra target-owned container
	// before issuing any mutation, even when its Compose name is unsealed.
	for _, operation := range []string{"stop", "destroy"} {
		t.Run(operation+" rejects renamed target before mutation", func(t *testing.T) {
			collision := proto.CloneOf(healthy)
			collision.Collisions[0].ServiceId = serviceID
			mutation := proto.CloneOf(step)
			if operation == "stop" {
				mutation.Payload = &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
					ArtifactId: artifactID, ServiceIds: []string{serviceID}, GraceSeconds: 30,
				}}
			} else {
				mutation.Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
					ArtifactId: artifactID, ServiceIds: []string{serviceID},
				}}
			}
			helper := completedComposeHelper()
			observer := &fakeComposeObserver{projects: []*agentpb.ObservedProject{collision}}
			runtime, err := testcomposeruntime.New(helper, observer)
			if err != nil {
				t.Fatalf("NewComposeRuntime() error = %v", err)
			}
			result, err := runtime.ExecuteStep(context.Background(), assignment, mutation)
			if !errors.Is(err, errs.New(errs.KindStateConflict, "")) || !result.ReconciliationRequired ||
				helper.request != nil {
				t.Fatalf(
					"executeStep() result = %#v, error = %v, mutation=%t; want preflight conflict",
					result,
					err,
					helper.request != nil,
				)
			}
		})
	}
}
