package controller

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type environmentRemovalPlanReader struct {
	environment etcd.Versioned[etcd.EnvironmentRecord]
}

func (reader environmentRemovalPlanReader) GetTenant(context.Context, string) (etcd.Versioned[etcd.TenantRecord], error) {
	return etcd.Versioned[etcd.TenantRecord]{}, nil
}
func (reader environmentRemovalPlanReader) GetProject(context.Context, string) (etcd.Versioned[etcd.ProjectRecord], error) {
	return etcd.Versioned[etcd.ProjectRecord]{}, nil
}
func (reader environmentRemovalPlanReader) GetEnvironment(context.Context, string) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return reader.environment, nil
}
func (reader environmentRemovalPlanReader) GetEnvironmentBlueprintRevision(
	context.Context, string, string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return etcd.Versioned[etcd.EnvironmentBlueprintRevision]{}, false, nil
}
func (reader environmentRemovalPlanReader) GetEnvironmentComposeProjection(
	context.Context, string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{}, false, nil
}

// Rationale: deleting an Environment that never received a Blueprint must remain restart-safe and remove only its
// Controller-authorized directory without inventing an empty Compose artifact.
func TestResolveArtifactFreeEnvironmentRemovalPlan(t *testing.T) {
	const (
		root          = "/srv/groundplane/vol"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		directory     = root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID
	)
	resolver, err := NewTaskPlanResolverWithBlueprints(root, environmentRemovalPlanReader{
		environment: etcd.Versioned[etcd.EnvironmentRecord]{Record: etcd.EnvironmentRecord{
			ID: environmentID, ProjectID: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "main",
			VolumeDir: directory, ProvisioningState: etcd.EnvironmentProvisioningReady,
		}},
	})
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Executor: etcd.TaskExecutorAgent,
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Type: etcd.TaskRemove, Target: environmentID,
		Params: map[string]string{EnvironmentRemoveVolumeDirectoryParam: directory},
		Steps:  []etcd.TaskStepRecord{{ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"}}, TimeoutSeconds: 120,
	})
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	remove := plan.Steps[0].GetEnvironmentDirectoryRemove()
	if len(plan.Artifacts) != 0 || remove == nil || remove.EnvironmentId != environmentID ||
		remove.ExpectedVolumeDir != directory {
		t.Fatalf("removal plan = %#v", plan)
	}
}
