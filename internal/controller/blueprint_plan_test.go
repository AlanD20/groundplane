package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type blueprintPlanReader struct {
	tenant      etcd.TenantRecord
	project     etcd.ProjectRecord
	environment etcd.EnvironmentRecord
	revision    etcd.EnvironmentBlueprintRevision
	projection  etcd.EnvironmentComposeProjection
}

func (reader *blueprintPlanReader) GetTenant(
	context.Context,
	string,
) (etcd.Versioned[etcd.TenantRecord], error) {
	return etcd.Versioned[etcd.TenantRecord]{Record: reader.tenant}, nil
}

func (reader *blueprintPlanReader) GetProject(
	context.Context,
	string,
) (etcd.Versioned[etcd.ProjectRecord], error) {
	return etcd.Versioned[etcd.ProjectRecord]{Record: reader.project}, nil
}

func (reader *blueprintPlanReader) GetEnvironment(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentRecord], error) {
	return etcd.Versioned[etcd.EnvironmentRecord]{Record: reader.environment}, nil
}

func (reader *blueprintPlanReader) GetEnvironmentBlueprintRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentBlueprintRevision], bool, error) {
	return etcd.Versioned[etcd.EnvironmentBlueprintRevision]{Record: reader.revision}, true, nil
}

func (reader *blueprintPlanReader) GetEnvironmentComposeProjection(
	context.Context,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: reader.projection}, true, nil
}

// Rationale: a Controller restart must reproduce the exact canonical artifact
// and typed pre-apply procedure from immutable Blueprint state, not a stored render.
func TestTaskPlanResolverRebuildsBlueprintComposeProcedure(t *testing.T) {
	reader, task := blueprintPlanTestState()
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	first, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	task.PlanHash = hex.EncodeToString(first.PlanHash)
	second, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(replay) error = %v", err)
	}
	if !bytes.Equal(first.PlanHash, second.PlanHash) || len(first.Artifacts) != 1 || len(first.Steps) != 3 ||
		first.Operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE ||
		first.Steps[0].GetMaterializeFile() == nil ||
		first.Steps[1].GetManagedVolumeDirectoriesEnsure() == nil ||
		first.Steps[2].GetComposeApply() == nil || !first.Steps[2].GetComposeApply().FullReconcile {
		t.Fatalf("resolved Blueprint plans = %#v / %#v", first, second)
	}
}

func blueprintPlanTestState() (*blueprintPlanReader, etcd.TaskRecord) {
	const (
		tenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
	)
	at := time.Date(2026, 8, 22, 20, 0, 0, 0, time.UTC)
	content := []byte(`kind: environment
schema: 1
metadata: {tenant: acme, project: shop, environment: production}
services:
  api:
    image: example/api:latest
    networks: [frontend]
    volumes: [app-data:/data]
networks:
  frontend: {}
volumes:
  app-data: {}
`)
	reader := &blueprintPlanReader{
		tenant: etcd.TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"},
		project: etcd.ProjectRecord{
			ID: projectID, TenantID: tenantID, Slug: "shop", Name: "Shop", Kind: etcd.ProjectKindTenant,
		},
		environment: etcd.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
			ID: environmentID, ProjectID: projectID, Name: "production",
			VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
			ProvisioningState: etcd.EnvironmentProvisioningReady, CreatedAt: at,
		},
		revision: etcd.EnvironmentBlueprintRevision{
			EnvironmentID: environmentID, RevisionID: taskID, RootPath: "blueprint.yaml",
			ComposeSources: []string{"blueprint.yaml"},
			Files:          []etcd.EnvironmentBlueprintFile{{Path: "blueprint.yaml", Content: content}}, CreatedAt: at,
		},
		projection: etcd.EnvironmentComposeProjection{
			EnvironmentID: environmentID, BlueprintRevisionID: taskID, RenderGeneration: 1,
			Services: []etcd.EnvironmentComposeIdentity{{
				ID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "api",
			}},
			Networks: []etcd.EnvironmentComposeIdentity{{
				ID: "net_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "frontend",
			}},
			Volumes: []etcd.EnvironmentComposeIdentity{{
				ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Name: "app-data",
			}},
		},
	}
	task := etcd.TaskRecord{
		ID: taskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Executor: etcd.TaskExecutorAgent, PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		RenderGeneration: 1, Type: etcd.TaskUpdate, Target: environmentID,
		Params: map[string]string{
			etcd.EnvironmentBlueprintRevisionParam:   taskID,
			etcd.TaskMaterializationEnvironmentParam: environmentID,
			EnvironmentBlueprintArtifactParam:        artifactID,
		},
		Steps: []etcd.TaskStepRecord{
			{ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
			{ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"},
		},
		TimeoutSeconds: 120, Status: etcd.TaskStatusPending, CreatedAt: at,
	}
	emptyDigest := sha256.Sum256(nil)
	task.Materializations = []etcd.TaskMaterializationRecord{{
		StepID: task.Steps[0].ID, MaterializationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		EnvironmentID: environmentID, Destination: "secrets/.env." + environmentID + ".api",
		ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api",
		OutputKind: etcd.TaskMaterializationOutputGeneratedEnvironment, Mode: 0o600,
		Length: 0, SHA256: hex.EncodeToString(emptyDigest[:]),
		Source: etcd.TaskMaterializationSource{
			Kind:                 etcd.TaskMaterializationSourceGeneratedEnvironment,
			GeneratedEnvironment: &etcd.TaskGeneratedEnvironmentValueReference{FormatVersion: 1},
		},
	}}
	return reader, task
}
