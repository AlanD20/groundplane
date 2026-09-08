package composeobserver

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const (
	observerPlanID     = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	observerArtifactID = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	observerServiceID  = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
)

// Rationale: observation returns only typed, sorted, ownership-validated
// evidence and excludes ambient Docker labels from the Agent contract.
func TestObserveReturnsOwnedContainerEvidence(t *testing.T) {
	plan := observerPlan(t)
	labels := dockerLabels(plan)
	engine := &fakeEngine{
		containers: []container.Summary{{ID: "container-b", Labels: labels}},
		inspects: map[string]client.ContainerInspectResult{
			"container-b": {Container: container.InspectResponse{
				ID: "container-b", Name: "/groundplane-infra-api-1", Image: "sha256:image-id",
				Config: &container.Config{Image: "registry.example/api@sha256:digest", Labels: labels},
				State:  &container.State{Status: container.StateRunning, Health: &container.Health{Status: "healthy"}},
			}},
		},
	}
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatalf("NewWithEngine() error = %v", err)
	}
	wantTime := time.Date(2026, time.August, 22, 12, 0, 0, 0, time.UTC)
	observer.now = func() time.Time { return wantTime }
	result, err := observer.Observe(context.Background(), plan, observerArtifactID)
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if result.ProjectName != "groundplane-infra" || result.ObservedAt.AsTime() != wantTime ||
		len(result.Containers) != 1 || len(result.Collisions) != 0 {
		t.Fatalf("observed project = %#v", result)
	}
	observed := result.Containers[0]
	if observed.ContainerId != "container-b" || observed.Name != "groundplane-infra-api-1" ||
		observed.ServiceId != observerServiceID ||
		observed.State != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_RUNNING ||
		observed.Health != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_HEALTHY ||
		observed.ExitCode != nil || len(observed.Labels) != 5 {
		t.Fatalf("observed container = %#v", observed)
	}
}

// Rationale: a resource in the generated Compose project without the complete
// expected ownership tuple is collision evidence, never mutation authority.
func TestObserveReportsProjectContainerWithIncompleteOwnershipAsCollision(t *testing.T) {
	plan := observerPlan(t)
	labels := dockerLabels(plan)
	delete(labels, "com.groundplane.managed")
	engine := &fakeEngine{
		containers: []container.Summary{{ID: "foreign", Labels: labels}},
		inspects: map[string]client.ContainerInspectResult{
			"foreign": {Container: container.InspectResponse{
				ID: "foreign", Name: "/groundplane-infra-api-1",
				Config: &container.Config{Labels: labels},
				State:  &container.State{Status: container.StateRunning},
			}},
		},
	}
	observer, err := NewWithEngine(engine)
	if err != nil {
		t.Fatalf("NewWithEngine() error = %v", err)
	}
	result, err := observer.Observe(context.Background(), plan, observerArtifactID)
	if err != nil {
		t.Fatalf("Observe() error = %v", err)
	}
	if len(result.Containers) != 0 || len(result.Collisions) != 1 ||
		result.Collisions[0].Kind != agentpb.ObservedCollisionKind_OBSERVED_COLLISION_KIND_CONTAINER ||
		result.Collisions[0].Name != "groundplane-infra-api-1" {
		t.Fatalf("observed project = %#v", result)
	}
}

// Rationale: Docker reports transient lifecycle states while restart policies
// converge. They are valid non-running evidence, not an observation failure.
func TestContainerStateNormalizesTransientDockerStates(t *testing.T) {
	for _, status := range []container.ContainerState{
		container.StateCreated,
		container.StatePaused,
		container.StateRestarting,
		container.StateRemoving,
	} {
		t.Run(string(status), func(t *testing.T) {
			state, exitCode, err := containerState(&container.State{Status: status})
			if err != nil {
				t.Fatalf("containerState() error = %v", err)
			}
			if state != agentpb.ObservedContainerState_OBSERVED_CONTAINER_STATE_CREATED || exitCode != nil {
				t.Fatalf("containerState() = %v/%v, want created/nil", state, exitCode)
			}
		})
	}
}

// Rationale: the closed state contract remains fail-closed for new Docker
// states until Groundplane explicitly defines their safe normalization.
func TestContainerStateRejectsUnknownDockerState(t *testing.T) {
	if _, _, err := containerState(&container.State{Status: container.ContainerState("unknown")}); err == nil {
		t.Fatal("containerState() error = nil, want unsupported-state error")
	}
}

// Rationale: Docker may explicitly report the absence of a healthcheck rather
// than omitting its Health object; both representations mean typed none.
func TestContainerHealthAcceptsExplicitNoHealthcheck(t *testing.T) {
	health, err := containerHealth(&container.State{Health: &container.Health{Status: container.NoHealthcheck}})
	if err != nil {
		t.Fatalf("containerHealth() error = %v", err)
	}
	if health != agentpb.ObservedContainerHealth_OBSERVED_CONTAINER_HEALTH_NONE {
		t.Fatalf("containerHealth() = %v, want none", health)
	}
}

func observerPlan(t *testing.T) *agentpb.ExecutionPlan {
	t.Helper()
	yaml := []byte("services:\n  api:\n    image: registry.example/api@sha256:digest\n")
	digest := sha256.Sum256(yaml)
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: observerPlanID, RenderGeneration: 1,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_DEPLOY, TargetId: observerServiceID,
		Artifacts: []*agentpb.ComposeArtifact{{
			ArtifactId:  observerArtifactID,
			OwnerKind:   agentpb.ComposeOwnerKind_COMPOSE_OWNER_KIND_PLATFORM,
			ProjectName: "groundplane-infra", CanonicalYaml: yaml, YamlSha256: digest[:],
			Services: []*agentpb.ComposeService{{
				ServiceId: observerServiceID, ComposeName: "api", ExpectedReplicas: 1, HasHealthcheck: true,
				ExpectedLabels: []*agentpb.LabelPair{
					{Key: "com.groundplane.kind", Value: "service"},
					{Key: "com.groundplane.managed", Value: "true"},
					{Key: "com.groundplane.plan-id", Value: observerPlanID},
					{Key: "com.groundplane.render-generation", Value: "1"},
					{Key: "com.groundplane.service-id", Value: observerServiceID},
				},
			}},
		}},
		Steps: []*agentpb.ExecutionStep{{
			StepId: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV", TimeoutSeconds: 30,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: observerArtifactID, ServiceIds: []string{observerServiceID},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("seal observation plan: %v", err)
	}
	return plan
}

func dockerLabels(plan *agentpb.ExecutionPlan) map[string]string {
	labels := map[string]string{
		composeProjectLabel: "groundplane-infra",
		composeServiceLabel: "api",
	}
	for _, pair := range plan.Artifacts[0].Services[0].ExpectedLabels {
		labels[pair.Key] = pair.Value
	}
	return labels
}

type fakeEngine struct {
	containers []container.Summary
	inspects   map[string]client.ContainerInspectResult
}

func (engine *fakeEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	return client.ContainerListResult{Items: engine.containers}, nil
}

func (engine *fakeEngine) ContainerInspect(
	_ context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	return engine.inspects[id], nil
}

func (engine *fakeEngine) NetworkList(
	context.Context,
	client.NetworkListOptions,
) (client.NetworkListResult, error) {
	return client.NetworkListResult{}, nil
}

func (engine *fakeEngine) NetworkInspect(
	context.Context,
	string,
	client.NetworkInspectOptions,
) (client.NetworkInspectResult, error) {
	return client.NetworkInspectResult{}, nil
}

func (engine *fakeEngine) VolumeList(
	context.Context,
	client.VolumeListOptions,
) (client.VolumeListResult, error) {
	return client.VolumeListResult{}, nil
}

func (engine *fakeEngine) VolumeInspect(
	context.Context,
	string,
	client.VolumeInspectOptions,
) (client.VolumeInspectResult, error) {
	return client.VolumeInspectResult{}, nil
}

func (engine *fakeEngine) Close() error { return nil }
