package app

import (
	"bytes"
	"context"
	"encoding/json"
	"runtime"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	migratedagentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	migratedscriptsourcepublication "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourcepublication"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	fixture *ExecutedArtifactFixture,
	resolver *testtaskplanning.TaskPlanResolver,
	nativeProject *composetypes.Project,
	serviceID string,
	servingRace bool,
) {
	t.Helper()
	ctx := context.Background()
	nativeBefore := fixture.AcknowledgedRuntime(t, serviceID)
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
		&testtaskplanning.ScriptArtifactService{},
		&migratedscriptsourcepublication.Authority{},
		&migratedagentregistration.Repository{},
		retainedUnexpectedImageResolver{t},
	)
	if err != nil {
		t.Fatal(err)
	}
	changes := []testblueprints.EnvironmentBlueprintServiceChange{
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
	task.Params[taskcontract.EnvironmentBlueprintArtifactParam] = artifactID
	task.Params[testblueprints.EnvironmentDesiredRevisionParam] = task.ID
	artifact, err := testcomposerender.RenderCompose(
		testcomposerender.ComposeRenderInput{Project: project, ArtifactID: artifactID,
			ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: fixture.Project.Record.TenantID, ProjectID: fixture.Project.Record.ID,
			EnvironmentID: fixture.Environment.Record.ID, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration), AuthorizedVolumeDir: fixture.Environment.Record.VolumeDir,
			Identities: testcomposeidentity.Snapshot{
				Services: []testcomposeidentity.Resource{
					{ID: serviceID, Name: "api"},
					{ID: managedID, Name: "managed-proof", ComponentID: componentID,
						ComponentImage: &testcomposeidentity.ComponentImage{
							Repository:  catalog.Repository,
							IndexDigest: catalog.IndexDigest,
							Reference:   reference,
							Platform:    platform,
						}},
				},
			}},
	)
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
	caddy, err := testcomponents.NewRecord(
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
	tunnel, err := testcomponents.NewRecord(
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
	projection.Components = []testcomponents.Record{caddy, tunnel}
	// Generated Component Services are runtime output, not authored native input.
	projection.NormalizedCompose, err = nativeProject.MarshalYAML()
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
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1, Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, fixture.Environment.Record.ID)
	// BP-05: Component execution advances the aggregate applied artifact, not
	// the independent native Service receipt retained for rollout recovery.
	if err != nil || !found || latest.Record.RevisionID != task.ID ||
		!bytes.Equal(latest.Record.ComposeArtifact, projection.ComposeArtifact) {
		t.Fatal("managed publication omitted its executed Component artifact")
	}
	if fixture.AcknowledgedRuntime(t, serviceID).Revision != nativeBefore.Revision {
		t.Fatal("managed publication changed the native Service receipt")
	}
}
