package controller

import (
	"bytes"
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: lifecycle planning must select the sealed runtime represented by
// the applied artifact, never reconstruct a workload from normalized desired.
func TestServiceLifecyclePlanSelectsAppliedSealedAddressableRuntime(t *testing.T) {
	t.Parallel()
	reader, _ := blueprintPlanTestState(t)
	const (
		serviceID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		releaseID = "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	replicas := 2
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {
			Name: "api", Image: "example/api:desired", Expose: []string{"8080"},
			Deploy:   &composetypes.DeployConfig{Replicas: &replicas},
			Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
		}},
		Networks: composetypes.Networks{"frontend": {}},
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	reader.projection.NormalizedCompose = normalized
	reader.projection.DesiredServices[0].Desired.Image = "example/api:desired"
	reader.projection.DesiredServices[0].Desired.Expose = []string{"8080"}
	reader.projection.DesiredServices[0].Desired.Replicas = 2
	reader.projection.Volumes = nil
	seal := domain.WorkloadSeal{
		RequestedReference: "example/api:deployed",
		LocalImageID:       "sha256:" + strings.Repeat("d", 64), ReplicaCount: 2,
	}
	source, err := RenderCompose(ComposeRenderInput{
		Project: project, ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAY",
		ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID:         reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAX", RenderGeneration: reader.projection.RenderGeneration,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		Identities:          mustComposeIdentitySnapshotFromProjection(t, reader.projection),
		Releases: map[string]ComposeReleaseIdentity{serviceID: {
			ProxyImage: testServiceProxyImage(), ReleaseID: releaseID,
			Target: domain.WorkloadSingleton, Image: seal.LocalImageID,
			ServingReleaseID: releaseID, ServingTarget: domain.WorkloadSingleton,
			ServingProxyGeneration: 4, Strategy: domain.StrategyRecreate,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	reader.projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(source)
	if err != nil {
		t.Fatal(err)
	}
	releaseRender := etcd.ReleaseRenderInput{
		ReleaseID: releaseID, PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAX",
		ArtifactID: source.ArtifactId, ServiceID: serviceID, ServiceName: "api",
		CandidateWorkload: seal, Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
		CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
		ProxyGeneration: 4, ProxyPorts: []uint16{8080}, ProxyConfigDigest: "sealed",
		ProxyImage: testServiceProxyImage(),
		TenantID:   reader.tenant.ID, TenantSlug: reader.tenant.Slug,
		ProjectID: reader.project.ID, ProjectSlug: reader.project.Slug,
		EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
		AuthorizedVolumeDir: reader.environment.VolumeDir, Projection: reader.projection,
	}
	input := etcd.ServiceLifecycleRenderInput{
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAW", ServiceID: serviceID,
		TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug,
		ProjectID: reader.project.ID, ProjectSlug: reader.project.Slug,
		EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		ArtifactID:          source.ArtifactId, Projection: reader.projection, AppliedProjectionRevision: 20,
		Release: etcd.ServiceLifecycleRelease{
			ServingReleaseID: releaseID, ProjectionRevision: 21, IntentRevision: 22,
			RenderRevision: 23, Current: releaseRender,
		},
	}
	task := etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAW", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		Executor: etcd.TaskExecutorAgent, PlanID: input.PlanID, Type: etcd.TaskStart,
		Target: serviceID, TimeoutSeconds: 120, Status: etcd.TaskStatusPending,
		Params: map[string]string{
			etcd.TaskServiceEnvironmentParam: input.EnvironmentID,
			etcd.TaskComposeArtifactParam:    input.ArtifactID,
		},
		Steps:            []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"}},
		RenderGeneration: int32(reader.projection.RenderGeneration),
	}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.buildServiceLifecyclePlan(context.Background(), task, input)
	if err != nil {
		t.Fatalf("buildServiceLifecyclePlan() error = %v", err)
	}
	services := make(map[string]*agentpb.ComposeService, len(plan.GetArtifacts()[0].GetServices()))
	for _, service := range plan.GetArtifacts()[0].GetServices() {
		services[service.GetComposeName()] = service
	}
	if len(services) != 2 || services["api"] == nil ||
		services["api"].GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY ||
		services["api--singleton"] == nil || services["api--singleton"].GetExpectedReplicas() != 2 ||
		services["api--singleton"].GetImageReference() != seal.LocalImageID {
		t.Fatalf("lifecycle selected services = %#v", services)
	}
}

func TestServiceLifecycleProcedureUsesTargetedComposeOperations(t *testing.T) {
	// Rationale: Start, Stop, and Destroy intentionally preserve different
	// Docker resources and must never collapse into a full reconcile/remove.
	t.Parallel()
	at := time.Date(2026, time.August, 23, 12, 0, 0, 0, time.UTC)
	serviceID := ids.NewAt(ids.KindService, at, 1)
	artifactID := ids.NewAt(ids.KindConfig, at, 2)
	for _, test := range []struct {
		taskType  etcd.TaskType
		operation agentpb.PlanOperation
		assert    func(*testing.T, *agentpb.ExecutionStep)
	}{
		{taskType: etcd.TaskStart, operation: agentpb.PlanOperation_PLAN_OPERATION_START, assert: func(t *testing.T, step *agentpb.ExecutionStep) {
			if apply := step.GetComposeApply(); apply == nil || apply.FullReconcile || len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != serviceID {
				t.Fatalf("Start step = %#v", step)
			}
		}},
		{taskType: etcd.TaskStop, operation: agentpb.PlanOperation_PLAN_OPERATION_STOP, assert: func(t *testing.T, step *agentpb.ExecutionStep) {
			if stop := step.GetComposeStop(); stop == nil || stop.GraceSeconds != ServiceStopGraceSeconds || len(stop.ServiceIds) != 1 || stop.ServiceIds[0] != serviceID {
				t.Fatalf("Stop step = %#v", step)
			}
		}},
		{taskType: etcd.TaskDestroy, operation: agentpb.PlanOperation_PLAN_OPERATION_DESTROY, assert: func(t *testing.T, step *agentpb.ExecutionStep) {
			if remove := step.GetComposeRemove(); remove == nil || remove.WholeProject || len(remove.ServiceIds) != 1 || remove.ServiceIds[0] != serviceID {
				t.Fatalf("Destroy step = %#v", step)
			}
		}},
	} {
		task := etcd.TaskRecord{
			Type: test.taskType, Target: serviceID, TimeoutSeconds: 120,
			Steps: []etcd.TaskStepRecord{{Kind: etcd.TaskStepOperation, ID: ids.New(ids.KindStep)}},
		}
		operation, steps, err := serviceLifecycleProcedure(task, []*agentpb.ComposeArtifact{{ArtifactId: artifactID}})
		if err != nil || operation != test.operation || len(steps) != 1 || steps[0].TimeoutSeconds != 120 {
			t.Fatalf("serviceLifecycleProcedure(%s) = %v, %#v, %v", test.taskType, operation, steps, err)
		}
		test.assert(t, steps[0])
	}
}

func TestServiceLifecyclePlanCompilesPinnedStartDependenciesAcrossRestart(t *testing.T) {
	t.Parallel()
	reader, _ := blueprintPlanTestState(t)
	const (
		apiID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		migrateID = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	)
	reader.revision.Files[0].Content = []byte("not runtime authority")
	reader.projection.DesiredServices = []etcd.EnvironmentServiceProjection{
		{EnvironmentID: reader.environment.ID, Desired: core.Service{
			ID: apiID, Name: "api", Image: "example/api:1", Zones: []string{"frontend"},
		}},
		{EnvironmentID: reader.environment.ID, Desired: core.Service{
			ID: migrateID, Name: "migrate", Image: "example/migrate:1", Zones: []string{"frontend"},
		}},
	}
	reader.projection.Volumes = nil
	input := etcd.ServiceLifecycleRenderInput{
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceID: apiID,
		TenantID: reader.tenant.ID, TenantSlug: reader.tenant.Slug,
		ProjectID: reader.project.ID, ProjectSlug: reader.project.Slug,
		EnvironmentID: reader.environment.ID, EnvironmentName: reader.environment.Name,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		ArtifactID:          "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Projection:          reader.projection,
	}
	project := &composetypes.Project{
		Services: composetypes.Services{
			"api": {
				Name: "api", Image: "example/api:1",
				Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
				DependsOn: map[string]composetypes.ServiceDependency{
					"migrate": {Condition: "service_completed_successfully", Required: true},
				},
			},
			"migrate": {
				Name: "migrate", Image: "example/migrate:1",
				Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
			},
		},
		Networks: composetypes.Networks{"frontend": {}},
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatalf("marshal normalized lifecycle Compose fixture: %v", err)
	}
	reader.projection.NormalizedCompose = normalized
	artifact, err := RenderCompose(ComposeRenderInput{
		Project: project, ArtifactID: input.ArtifactID,
		ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID:         reader.tenant.ID, ProjectID: reader.project.ID, EnvironmentID: reader.environment.ID,
		PlanID: input.PlanID, RenderGeneration: reader.projection.RenderGeneration,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		Identities:          mustComposeIdentitySnapshotFromProjection(t, reader.projection),
	})
	if err != nil {
		t.Fatal(err)
	}
	reader.projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	input.Projection = reader.projection
	workload := domain.WorkloadSeal{
		RequestedReference: "example/api:deployed",
		LocalImageID:       "sha256:" + strings.Repeat("a", 64), ReplicaCount: 1,
	}
	input.AppliedProjectionRevision = 20
	input.Release = etcd.ServiceLifecycleRelease{
		ServingReleaseID:   "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		ProjectionRevision: 21, IntentRevision: 22, RenderRevision: 23,
		Current: etcd.ReleaseRenderInput{
			ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", PlanID: input.PlanID,
			ArtifactID: input.ArtifactID, ServiceID: apiID, ServiceName: "api",
			CandidateWorkload: workload, Strategy: domain.StrategyRecreate, PriorStrategy: domain.StrategyRecreate,
			CandidateTarget: domain.WorkloadSingleton, PriorTarget: domain.WorkloadSingleton,
			TenantID: input.TenantID, TenantSlug: input.TenantSlug,
			ProjectID: input.ProjectID, ProjectSlug: input.ProjectSlug,
			EnvironmentID: input.EnvironmentID, EnvironmentName: input.EnvironmentName,
			AuthorizedVolumeDir: input.AuthorizedVolumeDir, Projection: reader.projection,
		},
	}
	task := etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAW", OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Executor: etcd.TaskExecutorAgent, PlanID: input.PlanID, Type: etcd.TaskStart,
		Target: apiID, TimeoutSeconds: 120, Status: etcd.TaskStatusPending,
	}
	firstResolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	prepared, err := firstResolver.PrepareServiceLifecycleTask(
		context.Background(), task, input, []string{"step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
	)
	if err != nil {
		t.Fatalf("PrepareServiceLifecycleTask() error = %v", err)
	}
	first, err := firstResolver.buildServiceLifecyclePlan(context.Background(), prepared, input)
	if err != nil {
		t.Fatalf("buildServiceLifecyclePlan() error = %v", err)
	}
	reader.projection.RevisionID = "task_01ARZ3NDEKTSV4RRFFQ69G5FAX"
	secondResolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints(restart) error = %v", err)
	}
	second, err := secondResolver.buildServiceLifecyclePlan(context.Background(), prepared, input)
	if err != nil {
		t.Fatalf("buildServiceLifecyclePlan(restart) error = %v", err)
	}
	if !bytes.Equal(first.PlanHash, second.PlanHash) || prepared.PlanHash != hex.EncodeToString(first.PlanHash) {
		t.Fatalf("restart plans changed: %x / %x", first.PlanHash, second.PlanHash)
	}
	yaml := string(first.Artifacts[0].CanonicalYaml)
	if !strings.Contains(yaml, workload.LocalImageID) || strings.Contains(yaml, "example/api:deployed") {
		t.Fatalf("compiled start artifact did not preserve sealed image authority:\n%s", yaml)
	}
}

func TestApplyServiceDependencyPhaseCompilesDeployRollbackAndAlways(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		phase      core.ServiceLifecyclePhase
		dependency string
	}{
		{phase: core.ServiceLifecycleStart, dependency: "shared"},
		{phase: core.ServiceLifecycleDeploy, dependency: "migrate"},
		{phase: core.ServiceLifecycleRollback, dependency: "previous"},
	} {
		project := &composetypes.Project{Services: composetypes.Services{
			"api":      {Name: "api", Image: "api"},
			"shared":   {Name: "shared", Image: "shared"},
			"migrate":  {Name: "migrate", Image: "migrate"},
			"previous": {Name: "previous", Image: "previous"},
		}}
		extensions := map[string]core.ServiceExtensionSpec{"api": {DependsOn: map[string]core.ServiceDependency{
			"shared": {
				Condition: core.ServiceDependencyHealthy,
				Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseAlways},
			},
			"migrate": {
				Condition: core.ServiceDependencyCompletedSuccessfully,
				Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseDeploy},
			},
			"previous": {
				Condition: core.ServiceDependencyStarted,
				Phases:    []core.ServiceDependencyPhase{core.ServiceDependencyPhaseRollback},
			},
		}}}
		if err := applyServiceDependencyPhase(project, extensions, test.phase); err != nil {
			t.Fatalf("applyServiceDependencyPhase(%s) error = %v", test.phase, err)
		}
		dependencies := project.Services["api"].DependsOn
		if _, exists := dependencies["shared"]; !exists {
			t.Fatalf("applyServiceDependencyPhase(%s) omitted always dependency", test.phase)
		}
		if _, exists := dependencies[test.dependency]; !exists {
			t.Fatalf("applyServiceDependencyPhase(%s) omitted %s", test.phase, test.dependency)
		}
	}
}
