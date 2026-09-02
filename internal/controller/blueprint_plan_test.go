package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
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

func (reader *blueprintPlanReader) GetEnvironmentComposeProjectionRevision(
	context.Context,
	string,
	string,
) (etcd.Versioned[etcd.EnvironmentComposeProjection], bool, error) {
	return etcd.Versioned[etcd.EnvironmentComposeProjection]{Record: reader.projection}, true, nil
}

// Rationale: a Controller restart must reproduce the exact canonical artifact
// and typed pre-apply procedure from immutable Blueprint state, not a stored render.
func TestTaskPlanResolverRebuildsBlueprintComposeProcedure(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
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

// Rationale: a Blueprint with no candidate Release has no Compose procedure;
// restart must retain its materialization and Volume prefix without inventing
// a full reconcile step from the absence of Release publication state.
func TestTaskPlanResolverRebuildsNoOpBlueprintPrefixWithoutComposeApply(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
		taskcontract.BlueprintComposeProcedureNone,
	)
	task.Steps = append([]etcd.TaskStepRecord(nil), task.Steps[:2]...)

	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_BLUEPRINT_APPLY || len(plan.Steps) != 2 ||
		plan.Steps[0].GetMaterializeFile() == nil ||
		plan.Steps[1].GetManagedVolumeDirectoriesEnsure() == nil ||
		plan.Steps[0].GetComposeApply() != nil || plan.Steps[1].GetComposeApply() != nil {
		t.Fatalf("no-op Blueprint plan = %#v", plan)
	}
}

func TestTaskPlanResolverRebuildsProfileOnlyBlueprintAsReconcileToEmpty(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	artifact := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(reader.projection.ComposeArtifact, artifact); err != nil {
		t.Fatalf("unmarshal Blueprint fixture artifact: %v", err)
	}
	artifact.Services = nil
	artifact.CanonicalYaml = []byte("services: {}\n")
	digest := sha256.Sum256(artifact.CanonicalYaml)
	artifact.YamlSha256 = digest[:]
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal profile-only Blueprint fixture artifact: %v", err)
	}
	reader.projection.DesiredServices = nil
	reader.projection.ComposeArtifact = encoded
	task.Materializations = nil
	task.Steps = append([]etcd.TaskStepRecord(nil), task.Steps[1:]...)

	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	plan, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan() error = %v", err)
	}
	apply := plan.Steps[len(plan.Steps)-1].GetComposeApply()
	if plan.Operation != agentpb.PlanOperation_PLAN_OPERATION_RECONCILE || apply == nil ||
		!apply.FullReconcile || apply.ArtifactId != artifact.ArtifactId {
		t.Fatalf("profile-only Blueprint plan = %#v", plan)
	}
}

func blueprintPlanTestState(t *testing.T) (*blueprintPlanReader, etcd.TaskRecord) {
	const (
		tenantID      = "tnt_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		projectID     = "prj_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		environmentID = "env_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		taskID        = "task_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		artifactID    = "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		planID        = "plan_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		serviceID     = "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV"
		networkID     = "net_01ARZ3NDEKTSV4RRFFQ69G5FAV"
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
			EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 1,
			DesiredZones: []etcd.EnvironmentZoneProjection{{EnvironmentID: environmentID, Desired: core.Zone{
				ID: networkID, Name: "frontend", Subnet: "10.40.0.0/24",
				OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
			}}},
			DesiredServices: []etcd.EnvironmentServiceProjection{{EnvironmentID: environmentID, Desired: core.Service{
				ID: serviceID, Name: "api", Image: "example/api:latest", Zones: []string{"frontend"},
			}}},
			Volumes: []etcd.EnvironmentVolumeIdentity{{
				ID: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV", Slug: "app-data", Key: "app-data",
			}},
		},
	}
	project := &composetypes.Project{
		Services: composetypes.Services{"api": {
			Name: "api", Image: "example/api:latest",
			Networks: map[string]*composetypes.ServiceNetworkConfig{"frontend": {}},
			Volumes: []composetypes.ServiceVolumeConfig{{
				Type: composetypes.VolumeTypeVolume, Source: "app-data", Target: "/data",
			}},
		}},
		Networks: composetypes.Networks{"frontend": {}},
		Volumes:  composetypes.Volumes{"app-data": {}},
	}
	normalized, err := project.MarshalYAML()
	if err != nil {
		t.Fatalf("marshal normalized Blueprint Compose fixture: %v", err)
	}
	reader.projection.NormalizedCompose = normalized
	artifact, err := RenderCompose(ComposeRenderInput{
		Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: ComposeProjectOwnerTenant,
		TenantID:         tenantID, ProjectID: projectID, EnvironmentID: environmentID,
		PlanID: planID, RenderGeneration: reader.projection.RenderGeneration,
		AuthorizedVolumeDir: reader.environment.VolumeDir,
		Identities:          mustComposeIdentitySnapshotFromProjection(t, reader.projection),
	})
	if err != nil {
		t.Fatalf("RenderCompose(Blueprint fixture) error = %v", err)
	}
	reader.projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatalf("marshal Blueprint fixture artifact: %v", err)
	}
	intentDigest := sha256.Sum256([]byte("protected Blueprint intent"))
	task := etcd.TaskRecord{
		ID: taskID, OperationID: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV",
		Executor: etcd.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: 1, Type: etcd.TaskUpdate, Target: environmentID,
		Params: map[string]string{
			etcd.EnvironmentDesiredRevisionParam:     taskID,
			etcd.TaskMaterializationEnvironmentParam: environmentID,
			EnvironmentBlueprintArtifactParam:        artifactID,
			EnvironmentBlueprintManagedVolumesParam:  "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			VolumeTaskIntentSHA256Param:              hex.EncodeToString(intentDigest[:]),
			taskcontract.EnvironmentBlueprintProcedureParam: string(
				taskcontract.BlueprintComposeProcedureFullReconcile,
			),
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
