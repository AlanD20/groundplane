package etcd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
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

type retainedUnexpectedImageResolver struct{ t *testing.T }

func (r retainedUnexpectedImageResolver) ResolveWorkloadImages(
	context.Context,
	string,
	[]*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	r.t.Fatal("managed-only preflight unexpectedly resolved native images")
	return nil, nil
}

// This starts from a real published, acknowledged native Release, then uses
// the same preflight, pre-stage merge, producer and durable replay as Apply.
func proveRetainedBlueprintProducer(
	t *testing.T,
	fixture *etcd.ExecutedArtifactFixture,
	resolver *controller.TaskPlanResolver,
	nativeProject *composetypes.Project,
	serviceID string,
	servingRace bool,
) {
	t.Helper()
	ctx := context.Background()
	scope, err := fixture.Ledger.LoadPlanningScope(ctx, fixture.Environment.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{serviceID})
	if err != nil {
		t.Fatal(err)
	}
	applied, found, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil || !found {
		t.Fatalf("applied source: %t %v", found, err)
	}
	prior := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(applied.Record.ComposeArtifact, prior); err != nil {
		t.Fatal(err)
	}
	producer, err := blueprintrelease.NewService(
		fixture.Ledger,
		&etcd.ScriptRepository{},
		resolver,
		&controller.ScriptArtifactService{},
		&etcd.ScriptSourceReferenceAuthority{},
		&etcd.LocalAgentRepository{},
		retainedUnexpectedImageResolver{t},
	)
	if err != nil {
		t.Fatal(err)
	}
	changes := []etcd.EnvironmentBlueprintServiceChange{
		{Current: &planning[0].Service, Record: planning[0].Service.Record},
	}
	project := &composetypes.Project{Name: nativeProject.Name, Services: make(composetypes.Services)}
	for name, service := range nativeProject.Services {
		project.Services[name] = service
	}
	catalog := blueprintTestProxyCatalog("b")
	platform, reference, found := catalog.Select(runtime.GOOS, runtime.GOARCH)
	if !found {
		t.Fatal("unsupported test platform")
	}
	project.Services["managed-proof"] = composetypes.ServiceConfig{
		Name:        "managed-proof",
		Image:       reference,
		NetworkMode: "none",
		HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}},
	}
	memberships, err := blueprintrelease.BuildNormalizedServiceMemberships(nativeProject, project)
	if err != nil {
		t.Fatal(err)
	}
	workloads, err := producer.Preflight(ctx, fixture.Environment.Record.ID, changes, memberships, nil)
	if err != nil {
		t.Fatal(err)
	}
	task := fixture.Task(t, 1200)
	desired, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, fixture.Environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("desired head: %t %v", found, err)
	}
	task.RenderGeneration = int32(desired.Record.RenderGeneration + 1)
	artifactID, managedID, componentID := ids.New(ids.KindConfig), ids.New(ids.KindService), ids.New(ids.KindComponent)
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(taskcontract.BlueprintComposeProcedureNone)
	task.Params[controller.EnvironmentBlueprintArtifactParam] = artifactID
	task.Params[etcd.EnvironmentDesiredRevisionParam] = task.ID
	artifact, err := controller.RenderCompose(controller.ComposeRenderInput{Project: project, ArtifactID: artifactID,
		ProjectOwnerKind: controller.ComposeProjectOwnerTenant, TenantID: fixture.Project.Record.TenantID, ProjectID: fixture.Project.Record.ID,
		EnvironmentID: fixture.Environment.Record.ID, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration), AuthorizedVolumeDir: fixture.Environment.Record.VolumeDir,
		Identities: controller.ComposeIdentitySnapshot{
			Services: []controller.ComposeResourceIdentity{
				{ID: serviceID, Name: "api"},
				{ID: managedID, Name: "managed-proof", ComponentID: componentID,
					ComponentImage: &controller.SelectedComponentImage{
						Repository:  catalog.Repository,
						IndexDigest: catalog.IndexDigest,
						Reference:   reference,
						Platform:    platform,
					}},
			},
		}})
	if err != nil {
		t.Fatal(err)
	}
	artifact, err = producer.PrepareRuntimeArtifact(workloads, artifact, changes)
	if err != nil {
		t.Fatal(err)
	}
	value, err := proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	projection := applied.Record
	projection.RevisionID, projection.RenderGeneration, projection.ComposeArtifact = task.ID, uint64(
		task.RenderGeneration,
	), value
	caddy, err := etcd.NewComponentRecord(
		core.Component{
			ID:      ids.New(ids.KindComponent),
			Owner:   core.ComponentOwnerEnvironment,
			OwnerID: fixture.Environment.Record.ID,
			Kind:    core.ComponentKindIngressCaddy,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	tunnel, err := etcd.NewComponentRecord(
		core.Component{
			ID:      componentID,
			Owner:   core.ComponentOwnerEnvironment,
			OwnerID: fixture.Environment.Record.ID,
			Kind:    core.ComponentKindEdgeCloudflare,
			Enabled: true,
			Config: core.ComponentConfig{CloudflareTunnel: &core.CloudflareTunnelComponentConfig{
				ZoneIDs: []string{ids.New(ids.KindNetwork)}, SecretID: ids.New(ids.KindSecret),
			}},
			GeneratedServices: []string{managedID},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	projection.Components = []etcd.ComponentRecord{caddy, tunnel}
	projection.NormalizedCompose, err = project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := producer.Prepare(
		ctx,
		blueprintrelease.PrepareInput{
			Workloads:      workloads,
			VolumeRoot:     "/var/lib/groundplane/vol",
			Projection:     projection,
			Memberships:    memberships,
			Task:           task,
			ServiceChanges: changes,
			Artifact:       artifact,
			CreatedAt:      task.CreatedAt,
			AllocateNamed:  func(kind ids.Kind, name string) string { return ids.DeriveAt(kind, task.CreatedAt, task.ID, name) },
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range prior.Services {
		matched := false
		for _, actual := range prepared.Plan.Artifacts[0].Services {
			matched = matched || proto.Equal(expected, actual)
		}
		if !matched {
			t.Fatalf("native runtime lost: %s", expected.ComposeName)
		}
	}
	if servingRace {
		fixture.RejectChangedServingPublication(t, prepared.Task, projection, prepared.Publication, serviceID)
		return
	}
	fixture.Publish(t, prepared.Task, projection, prepared.Publication)
	encoded, err := json.Marshal(prepared.Task)
	if err != nil {
		t.Fatal(err)
	}
	var persisted etcd.TaskRecord
	if err := json.Unmarshal(encoded, &persisted); err != nil {
		t.Fatal(err)
	}
	replayed, err := resolver.ResolveExecutionPlan(ctx, persisted)
	if err != nil || !proto.Equal(prepared.Plan, replayed) {
		t.Fatalf("retained runtime replay: %v", err)
	}
	agentID := ids.New(ids.KindAgent)
	claim, claimed, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !claimed || claim.Task.Record.ID != task.ID {
		t.Fatalf("retained Task claim: %t %v", claimed, err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, etcd.TaskStatusCompleted,
		etcd.TaskResultRecord{Kind: etcd.TaskResultCompose, ExecutionEpoch: 1, Diagnostic: etcd.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, fixture.Environment.Record.ID)
	if err != nil || !found || latest.Revision != applied.Revision ||
		!bytes.Equal(latest.Record.ComposeArtifact, applied.Record.ComposeArtifact) {
		t.Fatal("managed publication changed native acknowledgement")
	}
}
