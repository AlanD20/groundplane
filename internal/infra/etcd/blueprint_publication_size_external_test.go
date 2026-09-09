package etcd_test

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type publicationSizeImages struct{}

func (publicationSizeImages) ResolveWorkloadImages(
	_ context.Context, _ string, selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	resolved := make([]*agentpb.WorkloadImageResolution, len(selectors))
	for index, selector := range selectors {
		resolved[index] = &agentpb.WorkloadImageResolution{
			Selector: proto.CloneOf(selector), LocalImageId: "sha256:" + strings.Repeat("a", 64),
		}
	}
	return &agentpb.WorkloadImageResolutionResult{RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: resolved,
		}}}, nil
}

// Rationale: two running Services repeat the same legal Environment projection
// in the aggregate marker. Their individual render and assignment bounds fit;
// the update must publish and retain exact recovery/terminal bytes without
// enlarging the durable record ceiling or dropping historical witnesses.
func TestBlueprintRunningUpdateBoundedPublication(t *testing.T) {
	ctx := context.Background()
	fixture := etcd.NewExecutedArtifactFixture(t)
	audit := fixture.AuditBlueprintPublicationSize()
	resolver, err := controller.NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", fixture.Hierarchy, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
		t.Fatal(err)
	}
	scripts, sources := fixture.HookDependencies(t)
	if err := resolver.EnableScriptPlans(scripts); err != nil {
		t.Fatal(err)
	}
	artifacts, err := controller.NewScriptArtifactService(scripts, unexpectedHookEntryResolver{t: t})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := blueprintrelease.NewService(fixture.Ledger, scripts, resolver, artifacts, sources,
		fixture.ImageLookupAgent(t), publicationSizeImages{})
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := fixture.Hierarchy.GetTenant(ctx, fixture.Project.Record.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	serviceIDs := []string{ids.New(ids.KindService), ids.New(ids.KindService)}
	previous := &composetypes.Project{Name: "test", Services: composetypes.Services{}}
	var priorArtifact []byte
	for pass := 0; pass < 2; pass++ {
		task := fixture.Task(t, 2600+int64(pass))
		task.CreatedAt = time.Now().UTC()
		task.UpdatedAt = task.CreatedAt
		task.RenderGeneration = int32(pass + 1)
		artifactID := ids.New(ids.KindConfig)
		task.Params[controller.EnvironmentBlueprintArtifactParam] = artifactID
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
			taskcontract.BlueprintComposeProcedureNone,
		)
		project := &composetypes.Project{Name: "test", Services: composetypes.Services{}}
		changes := make([]etcd.EnvironmentBlueprintServiceChange, len(serviceIDs))
		identities := make([]controller.ComposeResourceIdentity, len(serviceIDs))
		projection := etcd.EnvironmentComposeProjection{EnvironmentID: fixture.Environment.Record.ID,
			RevisionID: task.ID, RenderGeneration: uint64(pass + 1)}
		for index, serviceID := range serviceIDs {
			name := fmt.Sprintf("worker-%d", index)
			desired := core.Service{ID: serviceID, Name: name, Image: "example/api:1",
				Replicas: pass + 1, Strategy: core.StrategyRecreate}
			record, err := etcd.NewServiceRecord(fixture.Environment.Record.ID, desired, "")
			if err != nil {
				t.Fatal(err)
			}
			changes[index].Record = record
			if pass > 0 {
				scope, err := fixture.Ledger.LoadPlanningScope(ctx, fixture.Environment.Record.ID)
				if err != nil {
					t.Fatal(err)
				}
				planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
				if err != nil || len(planning) != 1 || planning[0].Projection.ServingReleaseID == "" {
					t.Fatalf("running predecessor: %v", err)
				}
				changes[index].Current = &planning[0].Service
			}
			definition := composetypes.ServiceConfig{Name: name, Image: desired.Image, Scale: &desired.Replicas,
				NetworkMode: "none", Environment: composetypes.MappingWithEquals{},
				HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}}}
			for field := 0; field < 8; field++ {
				value := strings.Repeat("x", 3000)
				definition.Environment[fmt.Sprintf("PUBLIC_FIXTURE_%d", field)] = &value
			}
			project.Services[name] = definition
			identities[index] = controller.ComposeResourceIdentity{ID: serviceID, Name: name}
			projection.DesiredServices = append(projection.DesiredServices,
				etcd.EnvironmentServiceProjection{EnvironmentID: fixture.Environment.Record.ID, Desired: desired})
		}
		memberships, err := blueprintrelease.BuildNormalizedServiceMemberships(previous, project)
		if err != nil {
			t.Fatal(err)
		}
		workloads, err := producer.Preflight(ctx, fixture.Environment.Record.ID, changes, memberships, nil)
		if err != nil {
			t.Fatalf("pass%d preflight: %v", pass, err)
		}
		artifact, err := controller.RenderCompose(
			controller.ComposeRenderInput{Project: project, ArtifactID: artifactID,
				ProjectOwnerKind: controller.ComposeProjectOwnerTenant, TenantID: tenant.Record.ID,
				ProjectID: fixture.Project.Record.ID, EnvironmentID: fixture.Environment.Record.ID, PlanID: task.PlanID,
				RenderGeneration: uint64(pass + 1), AuthorizedVolumeDir: fixture.Environment.Record.VolumeDir,
				Identities: controller.ComposeIdentitySnapshot{Services: identities}},
		)
		if err != nil {
			t.Fatal(err)
		}
		projection.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		projection.NormalizedCompose, err = project.MarshalYAML()
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := producer.Prepare(ctx, blueprintrelease.PrepareInput{Workloads: workloads,
			VolumeRoot: "/var/lib/groundplane/vol", Task: task, Projection: projection, Tenant: tenant,
			Project: fixture.Project, Environment: fixture.Environment, Memberships: memberships,
			ServiceChanges: changes, Artifact: artifact, CreatedAt: task.CreatedAt,
			AllocateNamed: func(kind ids.Kind, purpose string) string {
				return ids.DeriveAt(kind, task.CreatedAt, task.ID, purpose)
			}})
		if err != nil {
			t.Fatalf("pass%d real producer publication: %v", pass, err)
		}
		if pass > 0 {
			fixture.AssertBlueprintNativePublicationSourceFences(t, prepared.Task, prepared.Publication)
		}
		fixture.Publish(t, prepared.Task, projection, prepared.Publication)
		if pass > 0 {
			fixture.AssertBlueprintNativeReferenceRejection(t, prepared.Task)
		}
		frozen, err := fixture.Ledger.GetBlueprintTaskRenderInput(ctx, prepared.Task)
		if err != nil || len(frozen.Members) != 2 {
			t.Fatalf("pass%d immutable publication read: %v", pass, err)
		}
		replayed, err := resolver.ResolveExecutionPlan(ctx, prepared.Task)
		if err != nil || !proto.Equal(prepared.Plan, replayed) {
			t.Fatalf("pass%d immutable plan replay: %v", pass, err)
		}
		agentID := ids.New(ids.KindAgent)
		claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
		if err != nil || !found || claim.Task.Record.ID != task.ID {
			t.Fatalf("pass%d assignment: %t %v", pass, found, err)
		}
		if pass > 0 {
			authority := claim.Assignment.Record.RestorationAuthority
			if authority == nil || authority.AppliedPredecessor == nil ||
				!bytes.Equal(
					authority.AppliedPredecessor.ComposeArtifact,
					priorArtifact,
				) || len(authority.NativePredecessors) != 2 {
				t.Fatal("assignment lost the exact applied or native predecessor witnesses")
			}
			for index, native := range frozen.NativePredecessors {
				if native.Serving == nil ||
					!bytes.Equal(native.CurrentArtifact, authority.NativePredecessors[index].CurrentArtifact) {
					t.Fatal("assignment changed the native predecessor artifact")
				}
			}
		}
		fixture.AssertBlueprintPublicationRecordSizes(t, prepared.Task, claim)
		publicationComparisons, assignmentComparisons := 26, 13
		if pass > 0 {
			publicationComparisons, assignmentComparisons = 37, 17
		}
		if audit.Publication.Comparisons != publicationComparisons || audit.Publication.Mutations != 19 ||
			audit.Assignment.Comparisons != assignmentComparisons || audit.Assignment.Mutations != 7 {
			t.Fatalf("pass%d changed transaction shapes: publication=%+v assignment=%+v", pass,
				audit.Publication, audit.Assignment)
		}
		if audit.Publication.Comparisons == 0 || audit.Publication.Mutations == 0 || audit.Publication.Bytes > 1<<20 ||
			audit.Assignment.Comparisons+audit.Assignment.Mutations > 96 || audit.Assignment.Bytes > 1<<20 {
			t.Fatal("complete publication/assignment transaction exceeds its existing budget")
		}
		proveBoundedBlueprintAgentAdmission(t, claim, replayed)
		result := etcd.TaskResultRecord{Kind: etcd.TaskResultCompose, ExecutionEpoch: 1,
			Diagnostic: etcd.TaskResultDiagnosticNone}
		completed, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID,
			etcd.TaskStatusCompleted, result, task.CreatedAt.Add(time.Minute))
		if err != nil || completed.Record.Status != etcd.TaskStatusCompleted {
			t.Fatalf("pass%d terminal completion: %v", pass, err)
		}
		replayedTerminal, err := fixture.Tasks.AcknowledgeTask(
			ctx,
			agentID,
			1,
			task.ID,
			claim.Assignment.Record.AssignmentID,
			etcd.TaskStatusCompleted,
			result,
			task.CreatedAt.Add(2*time.Minute),
		)
		if err != nil || replayedTerminal.Revision != completed.Revision {
			t.Fatalf("pass%d terminal replay: %v", pass, err)
		}
		latest, present, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, task.Target)
		executed, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(prepared.Plan.Artifacts[0])
		if err != nil || marshalErr != nil || !present || !bytes.Equal(latest.Record.ComposeArtifact, executed) {
			t.Fatal("terminal completion did not retain the exact executed artifact")
		}
		previous, priorArtifact = project, executed
	}
}
