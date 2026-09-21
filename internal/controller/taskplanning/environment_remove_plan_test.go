package taskplanning

import (
	"context"
	"testing"

	"github.com/AlanD20/groundplane/internal/controller/taskcontract"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testhierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

type environmentRemovalPlanReader struct {
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
}

func (reader environmentRemovalPlanReader) GetTenant(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.TenantRecord]{}, nil
}

func (reader environmentRemovalPlanReader) GetProject(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.ProjectRecord]{}, nil
}

func (reader environmentRemovalPlanReader) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return reader.environment, nil
}
func (reader environmentRemovalPlanReader) GetEnvironmentBlueprintRevision(
	context.Context, string, string,
) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{}, false, nil
}
func (reader environmentRemovalPlanReader) GetEnvironmentComposeProjection(
	context.Context, string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
}
func (reader environmentRemovalPlanReader) GetEnvironmentComposeProjectionRevision(
	context.Context, string, string,
) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{}, false, nil
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
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{
			Record: testhierarchy.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
				ID: environmentID, ProjectID: "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "main",
				VolumeDir: directory, ProvisioningState: testhierarchy.EnvironmentProvisioningReady,
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Type: testtaskjournal.TaskRemove, Target: environmentID,
		Params: map[string]string{taskcontract.EnvironmentRemoveVolumeDirectoryParam: directory},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		}, TimeoutSeconds: 120,
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

func TestResolveHierarchyEnvironmentCleanupPlan(t *testing.T) {
	const (
		root          = "/srv/groundplane/vol"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		directory     = root + "/tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV/prj_01ARZ3NDEKTSV4RRFFQ69G5FAV/" + environmentID
	)
	resolver, err := NewTaskPlanResolverWithBlueprints(root, environmentRemovalPlanReader{
		environment: testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: testhierarchy.EnvironmentRecord{
			ID: environmentID, VolumeDir: directory,
		}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{
		ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FAV", Executor: testtaskjournal.TaskExecutorAgent,
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV", RenderGeneration: 1,
		Type: testtaskjournal.TaskRemove, Target: environmentID, TimeoutSeconds: 120,
		Params: map[string]string{
			testtaskjournal.TaskResourceKindParam:                testtaskjournal.TaskResourceHierarchyDeletion,
			testtaskjournal.TaskHierarchyDeletionParentParam:     "del_0123456789abcdef0123456789abcdef",
			testtaskjournal.TaskHierarchyDeletionChildParam:      "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			testtaskjournal.TaskHierarchyDeletionAttemptParam:    "attempt_0123456789abcdef0123456789abcdef",
			testtaskjournal.TaskHierarchyDeletionGenerationParam: "1",
			testtaskjournal.TaskHierarchyDeletionOrdinalParam:    "2",
			testtaskjournal.TaskHierarchyDeletionActionKindParam: string(
				testhierarchydeletion.HierarchyDeletionEnvironmentAgentCleanup,
			),
			testtaskjournal.TaskHierarchyDeletionProcedureParam: "environment.cleanup",
			testtaskjournal.TaskHierarchyDeletionInputParam:     "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
		},
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	remove := plan.Steps[0].GetEnvironmentDirectoryRemove()
	if remove == nil || remove.EnvironmentId != environmentID || remove.ExpectedVolumeDir != directory {
		t.Fatalf("hierarchy cleanup plan = %#v", plan)
	}
}
