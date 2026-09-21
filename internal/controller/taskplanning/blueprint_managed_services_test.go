package taskplanning

import (
	"context"
	"encoding/hex"
	"runtime"
	"strings"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/infra/docker/composeobserver"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/client"
	"google.golang.org/protobuf/proto"
)

// Rationale: the strict project observer sees every Component, including one
// still carrying the previous plan's labels. All applies must precede health.
// Only Docker effects/reads are doubled; rendering, startup production, plan
// sealing, durable step reconstruction and ownership observation are real.
func TestBlueprintManagedStartupReplaysBeforeFullProjectHealth(t *testing.T) {
	project := &composetypes.Project{Services: composetypes.Services{}}
	input := composeRenderTestInput(project)
	for index, name := range []string{"router", "tunnel"} {
		image := controllerTestOCIImage("example/" + name)
		platform, reference, ok := image.Select(runtime.GOOS, runtime.GOARCH)
		if !ok {
			t.Fatal("fixture image platform missing")
		}
		project.Services[name] = composetypes.ServiceConfig{Name: name, Image: reference}
		input.Identities.Services = append(input.Identities.Services, testcomposeidentity.Resource{
			ID: composeIdentityTestID(ids.KindService, int64(70+index)), Name: name,
			ComponentID: composeIdentityTestID(ids.KindComponent, int64(80+index)),
			ComponentImage: &testcomposeidentity.ComponentImage{Repository: image.Repository,
				IndexDigest: image.IndexDigest, Reference: reference, Platform: platform},
		})
	}
	artifact, err := testcomposerender.RenderCompose(input)
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{PlanID: input.PlanID, RenderGeneration: int32(input.RenderGeneration), TimeoutSeconds: 30}
	steps, err := BlueprintManagedServiceSteps(task, artifact, "", false)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range steps {
		task.Steps = append(
			task.Steps,
			testtaskjournal.TaskStepRecord{ID: step.StepId, Kind: testtaskjournal.TaskStepOperation},
		)
	}
	replayed, err := BlueprintManagedServiceSteps(task, proto.CloneOf(artifact), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if err := blueprintManagedStepsMatch(task, 0, replayed); err != nil {
		t.Fatal(err)
	}
	for index, step := range replayed {
		if !proto.Equal(step, steps[index]) {
			t.Fatal("startup replay changed sealed step")
		}
	}
	plan, err := executionplan.Seal(&agentpb.ExecutionPlan{
		Schema: executionplan.SchemaVersion, PlanId: input.PlanID, RenderGeneration: input.RenderGeneration,
		Operation: agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY, TargetId: input.EnvironmentID,
		Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: replayed,
	})
	if err != nil {
		t.Fatal(err)
	}
	engine := &managedStartupEngine{containers: map[string]container.InspectResponse{}}
	for index, service := range artifact.Services {
		labels := map[string]string{"com.docker.compose.project": artifact.ProjectName,
			"com.docker.compose.service": service.ComposeName}
		for _, label := range service.ExpectedLabels {
			labels[label.Key] = label.Value
		}
		labels["com.groundplane.plan-id"] = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAX"
		labels["com.groundplane.render-generation"] = "6"
		id := strings.Repeat(string(rune('a'+index)), 64)
		engine.containers[service.ServiceId] = container.InspectResponse{
			ID: id, Name: "/" + service.ComposeName,
			Image:  "sha256:" + hex.EncodeToString(service.ImageConfigDigest),
			Config: &container.Config{Image: service.ImageReference, Labels: labels},
			State:  &container.State{Status: container.StateRunning, Health: &container.Health{Status: "healthy"}},
		}
	}
	observer, err := composeobserver.NewWithEngine(engine)
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range plan.Steps {
		if apply := step.GetComposeApply(); apply != nil {
			if len(apply.ServiceIds) != 1 || !apply.NoDependencies || apply.ForceRecreate || apply.FullReconcile {
				t.Fatal("managed apply widened mutation authority")
			}
			selected := engine.containers[apply.ServiceIds[0]]
			for _, service := range artifact.Services {
				if service.ServiceId == apply.ServiceIds[0] {
					for _, label := range service.ExpectedLabels {
						selected.Config.Labels[label.Key] = label.Value
					}
				}
			}
		}
		observed, err := observer.Observe(t.Context(), plan, artifact.ArtifactId)
		if err != nil {
			t.Fatal(err)
		}
		if step.GetWaitHealthy() != nil && (len(observed.Collisions) != 0 || len(observed.Containers) != 2) {
			t.Fatalf("health ran before all managed ownership converged: containers=%d collisions=%d",
				len(observed.Containers), len(observed.Collisions))
		}
	}
	foreign := engine.containers[artifact.Services[0].ServiceId]
	foreign.Config.Labels["com.groundplane.managed"] = "false"
	observed, err := observer.Observe(t.Context(), plan, artifact.ArtifactId)
	if err != nil || len(observed.GetCollisions()) != 1 {
		t.Fatalf("foreign ownership stopped failing closed: %v, %v", observed, err)
	}
}

// Rationale: both Blueprint procedures preserve the native workload boundary,
// exact serial dependencies, stable identities and durable replay guards.
func TestBlueprintManagedStartupPoliciesAndReplayGuards(t *testing.T) {
	for _, forward := range []bool{false, true} {
		task := etcd.TaskRecord{PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", TimeoutSeconds: 30}
		first, second := composeIdentityTestID(ids.KindService, 1), composeIdentityTestID(ids.KindService, 2)
		artifact := &agentpb.ComposeArtifact{ArtifactId: composeRenderTestArtifactID,
			Services: []*agentpb.ComposeService{
				{ServiceId: second, OwnerComponentId: composeIdentityTestID(ids.KindComponent, 2)},
				{ServiceId: composeIdentityTestID(ids.KindService, 3)},
				{ServiceId: first, OwnerComponentId: composeIdentityTestID(ids.KindComponent, 1)},
			}}
		steps, err := BlueprintManagedServiceSteps(task, artifact, "prefix", forward)
		if err != nil || len(steps) != 4 {
			t.Fatalf("managed startup = %v, %v", steps, err)
		}
		prerequisite := "prefix"
		for index, step := range steps {
			if step.PrerequisiteStepId != prerequisite || step.TimeoutSeconds != 30 {
				t.Fatal("managed startup lost its serial prerequisite or timeout")
			}
			policy := agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_UNSPECIFIED
			if forward {
				policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
			}
			if step.Policy != policy {
				t.Fatal("managed startup changed execution policy")
			}
			want := first
			if index%2 == 1 {
				want = second
			}
			if index < 2 {
				apply := step.GetComposeApply()
				if apply == nil || len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != want ||
					!apply.NoDependencies || apply.ForceRecreate || apply.FullReconcile {
					t.Fatal("managed startup selected native or widened apply")
				}
			} else {
				health := step.GetWaitHealthy()
				if health == nil || len(health.ServiceIds) != 1 || health.ServiceIds[0] != want {
					t.Fatal("managed startup selected wrong health target")
				}
			}
			prerequisite = step.StepId
			task.Steps = append(
				task.Steps,
				testtaskjournal.TaskStepRecord{ID: step.StepId, Kind: testtaskjournal.TaskStepOperation},
			)
		}
		if err := blueprintManagedStepsMatch(task, 0, steps); err != nil {
			t.Fatal(err)
		}
		task.Steps[1], task.Steps[2] = task.Steps[2], task.Steps[1]
		if err := blueprintManagedStepsMatch(task, 0, steps); err == nil {
			t.Fatal("durable replay accepted old interleaved ordering")
		}
	}
}

type managedStartupEngine struct {
	containers map[string]container.InspectResponse
}

func (e *managedStartupEngine) ContainerList(
	context.Context,
	client.ContainerListOptions,
) (client.ContainerListResult, error) {
	result := client.ContainerListResult{}
	for _, item := range e.containers {
		result.Items = append(result.Items, container.Summary{ID: item.ID, Labels: item.Config.Labels})
	}
	return result, nil
}

func (e *managedStartupEngine) ContainerInspect(
	_ context.Context,
	id string,
	_ client.ContainerInspectOptions,
) (client.ContainerInspectResult, error) {
	for _, item := range e.containers {
		if item.ID == id {
			return client.ContainerInspectResult{Container: item}, nil
		}
	}
	panic("unknown fixture container")
}
func (*managedStartupEngine) NetworkList(context.Context, client.NetworkListOptions) (client.NetworkListResult, error) {
	return client.NetworkListResult{}, nil
}

func (*managedStartupEngine) NetworkInspect(
	context.Context, string,

	client.NetworkInspectOptions,
) (client.NetworkInspectResult, error) {
	panic("unexpected network inspection")
}
func (*managedStartupEngine) VolumeList(context.Context, client.VolumeListOptions) (client.VolumeListResult, error) {
	return client.VolumeListResult{}, nil
}

func (*managedStartupEngine) VolumeInspect(
	context.Context, string,

	client.VolumeInspectOptions,
) (client.VolumeInspectResult, error) {
	panic("unexpected volume inspection")
}
func (*managedStartupEngine) Close() error { return nil }
