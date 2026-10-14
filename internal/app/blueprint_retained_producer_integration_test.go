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
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testcomponents "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// This starts from a real published, acknowledged native Release, then uses
// the native Service-unit producer and durable replay used by Apply.
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
	if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.Ledger.LoadPlanningScope(ctx, fixture.Environment.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	acknowledged, err := fixture.Ledger.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx, fixture.Environment.Record.ID, []string{serviceID}, scope.ReadRevision,
	)
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
	seal := domain.WorkloadSeal{
		RequestedReference: planning[0].Service.Record.Desired.Image,
		ReplicaCount:       uint32(planning[0].Service.Record.Desired.Replicas),
	}
	for _, member := range prior.Services {
		if member.ServiceId == serviceID &&
			member.Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			seal.LocalImageID = member.ImageReference
			break
		}
	}
	if seal.LocalImageID == "" {
		t.Fatal("acknowledged native image is absent")
	}
	scripts, sources := fixture.HookDependencies(t)
	if err := resolver.EnableScriptPlans(scripts); err != nil {
		t.Fatal(err)
	}
	artifacts, err := testtaskplanning.NewScriptArtifactService(scripts, unexpectedHookEntryResolver{t: t})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := blueprintrelease.NewService(
		fixture.Ledger,
		scripts,
		resolver,
		artifacts,
		sources,
		blueprintImageLookupAgent(t, fixture),
		&mixedCandidateImageResolver{t: t, seal: seal, withPredecessor: true},
		unexpectedBlueprintRollingObserver{t},
	)
	if err != nil {
		t.Fatal(err)
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
	task := fixture.Task(t, 1200)
	desired, found, err := fixture.Hierarchy.GetEnvironmentComposeProjection(ctx, fixture.Environment.Record.ID)
	if err != nil || !found {
		t.Fatalf("desired head: %t %v", found, err)
	}
	task.RenderGeneration = int32(desired.Record.RenderGeneration + 1)
	artifactID, managedID, componentID := ids.New(ids.KindConfig), ids.New(ids.KindService), ids.New(ids.KindComponent)
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
	revision := fixture.PublishNativeBlueprintRevision(t, task, projection, nil)
	prepared := fixture.PrepareNativeServiceUnit(t, producer, revision, serviceID, nil, nil)
	predecessor := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(acknowledged[0].Record.Runtime.CurrentArtifact, predecessor); err != nil {
		t.Fatal(err)
	}
	matchedPredecessor := false
	for _, artifact := range prepared.Plan.Artifacts {
		// Capture assigns a new artifact handle; every physical identity, byte,
		// label and image must still equal the acknowledged source.
		expected := proto.CloneOf(predecessor)
		expected.ArtifactId = artifact.ArtifactId
		matchedPredecessor = matchedPredecessor || proto.Equal(artifact, expected)
	}
	if !matchedPredecessor {
		t.Fatal("native Service unit lost exact acknowledged predecessor artifact")
	}
	if servingRace {
		fixture.RejectChangedServingChildPublication(t, revision, serviceID, prepared)
		return
	}
	fixture.PublishNativeServiceUnit(t, revision, serviceID, prepared)
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
	claim, claimed, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, prepared.Task.CreatedAt.Add(time.Second), 32)
	if err != nil || !claimed || claim.Task.Record.ID != prepared.Task.ID {
		t.Fatalf("retained Task claim: %t %v", claimed, err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, prepared.Task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1, Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, prepared.Task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, fixture.Environment.Record.ID)
	executed, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(prepared.Plan.Artifacts[0])
	if err != nil || marshalErr != nil || !found || latest.Record.RevisionID != task.ID ||
		!bytes.Equal(latest.Record.ComposeArtifact, executed) {
		t.Fatal("native Service publication omitted its exact executed artifact")
	}
}
