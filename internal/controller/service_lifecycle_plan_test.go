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
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

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
		operation, step, err := serviceLifecycleProcedure(task, artifactID)
		if err != nil || operation != test.operation || step.TimeoutSeconds != 120 {
			t.Fatalf("serviceLifecycleProcedure(%s) = %v, %#v, %v", test.taskType, operation, step, err)
		}
		test.assert(t, step)
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
		context.Background(), task, input, "step_01ARZ3NDEKTSV4RRFFQ69G5FAV",
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
	if !strings.Contains(yaml, "depends_on:") ||
		!strings.Contains(yaml, "condition: service_completed_successfully") {
		t.Fatalf("compiled start artifact omitted dependency:\n%s", yaml)
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
