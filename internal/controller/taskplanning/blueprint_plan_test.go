package taskplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testtaskmaterialization "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type blueprintPlanReader struct {
	tenant      testhierarchy.TenantRecord
	project     testhierarchy.ProjectRecord
	environment testhierarchy.EnvironmentRecord
	revision    testblueprints.EnvironmentBlueprintRevision
	projection  testenvironmentprojection.EnvironmentComposeProjection
}

func (reader *blueprintPlanReader) GetTenant(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.TenantRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.TenantRecord]{Record: reader.tenant}, nil
}

func (reader *blueprintPlanReader) GetProject(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.ProjectRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.ProjectRecord]{Record: reader.project}, nil
}

func (reader *blueprintPlanReader) GetEnvironment(
	context.Context, string,

) (testkeyvalue.Versioned[testhierarchy.EnvironmentRecord], error) {
	return testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]{Record: reader.environment}, nil
}

func (reader *blueprintPlanReader) GetEnvironmentBlueprintRevision(
	context.Context, string, string,

) (testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision], bool, error) {
	return testkeyvalue.Versioned[testblueprints.EnvironmentBlueprintRevision]{Record: reader.revision}, true, nil
}

func (reader *blueprintPlanReader) GetEnvironmentComposeProjection(
	context.Context, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: reader.projection,
	}, true, nil
}

func (reader *blueprintPlanReader) GetEnvironmentComposeProjectionRevision(
	context.Context, string, string,

) (testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection], bool, error) {
	return testkeyvalue.Versioned[testenvironmentprojection.EnvironmentComposeProjection]{
		Record: reader.projection,
	}, true, nil
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

// A retry has a new Task identity but retains the authored desired revision;
// replay must use that durable selector even when no prior teardown exists.
func TestTaskPlanResolverRebuildsBlueprintRetryAgainstAuthoredRevision(t *testing.T) {
	reader, task := blueprintPlanTestState(t)
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
		taskcontract.BlueprintComposeProcedureNone,
	)
	task.Steps = append([]testtaskjournal.TaskStepRecord(nil), task.Steps[:2]...)
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
	}
	original, err := resolver.ResolveExecutionPlan(context.Background(), task)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(original) error = %v", err)
	}
	retry := task
	retry.ID = ids.NewAt(ids.KindTask, time.Date(2026, 8, 22, 20, 1, 0, 0, time.UTC), 1)
	retry.RetryOf = task.ID
	replayed, err := resolver.ResolveExecutionPlan(context.Background(), retry)
	if err != nil {
		t.Fatalf("ResolveExecutionPlan(retry) error = %v", err)
	}
	if !bytes.Equal(original.PlanHash, replayed.PlanHash) || !proto.Equal(original, replayed) {
		t.Fatalf("retry changed authored-revision plan = %#v / %#v", original, replayed)
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
	task.Steps = append([]testtaskjournal.TaskStepRecord(nil), task.Steps[:2]...)

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

// Rationale: restart shape validation must not infer a prerequisite gate from
// either Task parameters or projection state alone; both sealed authorities
// must agree before an Environment update can be assigned.
func TestTaskPlanResolverRejectsBlueprintRequirementMarkerProjectionMismatch(t *testing.T) {
	requirements := core.BlueprintRequirements{
		Authored: []core.Requirement{{
			Target: core.RequirementTarget{
				Kind: core.RequirementTargetBackingAttach,
				Name: "database",
			},
			Condition: core.RequirementReady,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		Resolved: []core.ResolvedRequirement{{
			Target: core.ResolvedRequirementTarget{
				Kind:     core.RequirementTargetBackingAttach,
				Name:     "database",
				ID:       "att_01ARZ3NDEKTSV4RRFFQ69G5FAV",
				TaskID:   "task_01ARZ3NDEKTSV4RRFFQ69G5FAW",
				Revision: 1,
			},
			Condition: core.RequirementReady,
			Phases:    []core.RequirementPhase{core.RequirementPhaseDeploy},
		}},
		ResolutionRevision: 1,
	}
	if err := requirements.Validate(); err != nil {
		t.Fatalf("Blueprint requirement fixture is invalid: %v", err)
	}
	for name, mutate := range map[string]func(*blueprintPlanReader, *etcd.TaskRecord){
		"marker without projection": func(_ *blueprintPlanReader, task *etcd.TaskRecord) {
			task.Params[etcd.TaskBlueprintRequirementGateSHA256Param] = "gate-digest"
		},
		"projection without marker": func(reader *blueprintPlanReader, _ *etcd.TaskRecord) {
			reader.projection.BlueprintRequirements = requirements
		},
	} {
		t.Run(name, func(t *testing.T) {
			reader, task := blueprintPlanTestState(t)
			mutate(reader, &task)
			resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
			if err != nil {
				t.Fatalf("NewTaskPlanResolverWithBlueprints() error = %v", err)
			}
			if _, err := resolver.ResolveExecutionPlan(context.Background(), task); err == nil {
				t.Fatal("ResolveExecutionPlan() error = nil")
			}
		})
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
	task.Steps = append([]testtaskjournal.TaskStepRecord(nil), task.Steps[1:]...)

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
		tenant: testhierarchy.TenantRecord{ID: tenantID, Slug: "acme", Name: "Acme"},
		project: testhierarchy.ProjectRecord{
			ID: projectID, TenantID: tenantID, Slug: "shop", Name: "Shop", Kind: testhierarchy.ProjectKindTenant,
		},
		environment: testhierarchy.EnvironmentRecord{NetworkPool: "10.40.0.0/16",
			ID: environmentID, ProjectID: projectID, Name: "production",
			VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + projectID + "/" + environmentID,
			ProvisioningState: testhierarchy.EnvironmentProvisioningReady, CreatedAt: at,
		},
		revision: testblueprints.EnvironmentBlueprintRevision{
			EnvironmentID: environmentID, RevisionID: taskID, RootPath: "blueprint.yaml",
			ComposeSources: []string{"blueprint.yaml"},
			Files: []testblueprints.EnvironmentBlueprintFile{
				{Path: "blueprint.yaml", Content: content},
			}, CreatedAt: at,
		},
		projection: testenvironmentprojection.EnvironmentComposeProjection{
			EnvironmentID: environmentID, RevisionID: taskID, RenderGeneration: 1,
			DesiredZones: []testenvironmentprojection.EnvironmentZoneProjection{
				{EnvironmentID: environmentID, Desired: core.Zone{
					ID: networkID, Name: "frontend", Subnet: "10.40.0.0/24",
					OwnerKind: core.ZoneOwnerEnvironment, OwnerID: environmentID,
				}},
			},
			DesiredServices: []testservices.EnvironmentServiceProjection{
				{EnvironmentID: environmentID, Desired: core.Service{
					ID: serviceID, Name: "api", Image: "example/api:latest", Zones: []string{"frontend"},
				}},
			},
			Volumes: []testenvironmentprojection.EnvironmentVolumeIdentity{{
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
	artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
		Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant,
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
		Executor: testtaskjournal.TaskExecutorAgent, PlanID: planID,
		RenderGeneration: 1, Type: testtaskjournal.TaskUpdate, Target: environmentID,
		Params: map[string]string{
			testblueprints.EnvironmentDesiredRevisionParam:       taskID,
			testtaskjournal.TaskMaterializationEnvironmentParam:  environmentID,
			taskcontract.EnvironmentBlueprintArtifactParam:       artifactID,
			taskcontract.EnvironmentBlueprintManagedVolumesParam: "vol_01ARZ3NDEKTSV4RRFFQ69G5FAV",
			VolumeTaskIntentSHA256Param:                          hex.EncodeToString(intentDigest[:]),
			taskcontract.EnvironmentBlueprintProcedureParam: string(
				taskcontract.BlueprintComposeProcedureFullReconcile,
			),
		},
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAV"},
			{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAW"},
			{Kind: testtaskjournal.TaskStepOperation, ID: "step_01ARZ3NDEKTSV4RRFFQ69G5FAX"},
		},
		TimeoutSeconds: 120, Status: testtaskjournal.TaskStatusPending, CreatedAt: at,
	}
	emptyDigest := sha256.Sum256(nil)
	task.Materializations = []testtaskmaterialization.Record{{
		StepID: task.Steps[0].ID, MaterializationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FAW",
		EnvironmentID: environmentID, Destination: "secrets/.env." + environmentID + ".api",
		ServiceID: "svc_01ARZ3NDEKTSV4RRFFQ69G5FAV", ServiceName: "api",
		OutputKind: testtaskmaterialization.OutputGeneratedEnvironment, Mode: 0o600,
		Length: 0, SHA256: hex.EncodeToString(emptyDigest[:]),
		Source: testtaskmaterialization.Source{
			Kind:                 testtaskmaterialization.SourceGeneratedEnvironment,
			GeneratedEnvironment: &testtaskmaterialization.GeneratedEnvironmentValueReference{FormatVersion: 1},
		},
	}}
	return reader, task
}
