package etcd_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// SVC-15/BP-04: Rationale: native deploy/rollback advances serving identity independently
// of the original acknowledged Blueprint artifact. A new candidate must not
// require rollback C to carry original Blueprint A's Release identity.
func TestBlueprintCandidateAfterNativeRollbackUsesSealedPredecessor(t *testing.T) {
	testBlueprintExecutedArtifact(
		t,
		true,
		false,
		func(fixture *etcd.ExecutedArtifactFixture, resolver *testtaskplanning.TaskPlanResolver, original testreleaserender.ReleaseRenderInput, intent domain.Intent, applied *agentpb.ComposeArtifact) {
			current, _ := fixture.SeedRetainedRollback(t, original, intent)
			fixture.SeedNativeCandidateDesired(t, current, 3)
			proveNativeCandidatePreparation(t, fixture, resolver, current, applied, "")
		},
	)
}

func proveNativeCandidatePreparation(
	t *testing.T,
	fixture *etcd.ExecutedArtifactFixture,
	resolver *testtaskplanning.TaskPlanResolver,
	current testreleaserender.ReleaseRenderInput,
	applied *agentpb.ComposeArtifact,
	fault string,
) {
	t.Helper()
	ctx := context.Background()
	if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
		t.Fatal(err)
	}
	// Complete the simulated successful native transition before the real
	// producer reads it. Original Blueprint A remains independently unchanged.
	serving, err := fixture.Ledger.ResolveServing(ctx, current.EnvironmentID, current.ServiceID, 0)
	if err != nil {
		t.Fatal(err)
	}
	native := testreleaserender.ServiceLifecycleRelease{ServingReleaseID: current.ReleaseID, Current: current}
	if current.Strategy == domain.StrategyBlueGreen {
		prior, readErr := fixture.Ledger.GetReleaseRenderInputAt(
			ctx,
			serving.Intent.PriorServingReleaseID,
			serving.Revision,
		)
		if readErr != nil {
			t.Fatal(readErr)
		}
		native.PriorServingReleaseID, native.RetainedPrior = prior.Record.ReleaseID, &prior.Record
	}
	fragments, err := resolver.RenderRetainedServiceRuntime(ctx, native)
	if err != nil {
		t.Fatal(err)
	}
	fixture.SeedEntryAcknowledgedRuntime(t, current, fragments)
	scope, err := fixture.Ledger.LoadPlanningScope(ctx, current.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{current.ServiceID})
	if err != nil {
		t.Fatal(err)
	}
	if planning[0].Service.Record.Desired.Replicas != int(current.CandidateWorkload.ReplicaCount)+1 ||
		planning[0].Projection.ServingReleaseID != current.ReleaseID {
		t.Fatal("candidate fixture does not differ from its sealed serving workload")
	}
	imageResolver := &mixedCandidateImageResolver{t: t, seal: current.CandidateWorkload, withPredecessor: true}
	scripts, sources := fixture.HookDependencies(t)
	if err := resolver.EnableScriptPlans(scripts); err != nil {
		t.Fatal(err)
	}
	scriptArtifacts, err := testtaskplanning.NewScriptArtifactService(scripts, unexpectedHookEntryResolver{t: t})
	if err != nil {
		t.Fatal(err)
	}
	producer, err := blueprintrelease.NewService(
		fixture.Ledger,
		scripts,
		resolver,
		scriptArtifacts,
		sources,
		blueprintImageLookupAgent(t, fixture),
		imageResolver,
	)
	if err != nil {
		t.Fatal(err)
	}
	desired := planning[0].Service.Record
	desired.Desired.Replicas = int(current.CandidateWorkload.ReplicaCount)
	changes := []testblueprints.EnvironmentBlueprintServiceChange{{Current: &planning[0].Service, Record: desired}}
	project := &composetypes.Project{Name: "test", Services: composetypes.Services{current.ServiceName: {
		Name: current.ServiceName, Image: current.CandidateWorkload.RequestedReference, Scale: &desired.Desired.Replicas,
		NetworkMode: "none", Expose: []string{"8080"}, HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}},
	}}}
	memberships, err := blueprintrelease.BuildNormalizedServiceMemberships(project, project)
	if err != nil {
		t.Fatal(err)
	}
	before := fixture.ReadRevision()
	workloads, err := producer.Preflight(ctx, current.EnvironmentID, changes, memberships, nil)
	if err != nil || fixture.ReadRevision() != before {
		t.Fatalf("read-only native preflight: %v", err)
	}
	task := fixture.Task(t, 1700)
	task.CreatedAt = time.Now().UTC()
	task.UpdatedAt = task.CreatedAt
	task.RenderGeneration = 3
	artifactID := ids.New(ids.KindConfig)
	task.Params[taskcontract.EnvironmentBlueprintArtifactParam] = artifactID
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(taskcontract.BlueprintComposeProcedureNone)
	artifact, err := testcomposerender.RenderCompose(
		testcomposerender.ComposeRenderInput{Project: project, ArtifactID: artifactID,
			ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: current.TenantID, ProjectID: current.ProjectID,
			EnvironmentID: current.EnvironmentID, PlanID: task.PlanID, RenderGeneration: 3, AuthorizedVolumeDir: current.AuthorizedVolumeDir,
			Identities: testcomposeidentity.Snapshot{
				Services: []testcomposeidentity.Resource{{ID: current.ServiceID, Name: current.ServiceName}},
			}},
	)
	if err != nil {
		t.Fatal(err)
	}
	projection := current.Projection
	projection.RevisionID, projection.RenderGeneration = task.ID, 3
	projection.DesiredServices = []testservices.EnvironmentServiceProjection{
		{EnvironmentID: current.EnvironmentID, Desired: desired.Desired},
	}
	projection.ComposeArtifact, err = (proto.MarshalOptions{Deterministic: true}).Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	projection.NormalizedCompose, err = project.MarshalYAML()
	if err != nil {
		t.Fatal(err)
	}
	tenant, err := fixture.Hierarchy.GetTenant(ctx, current.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := producer.Prepare(
		ctx,
		blueprintrelease.PrepareInput{Workloads: workloads, VolumeRoot: "/var/lib/groundplane/vol", Task: task,
			Projection: projection, Tenant: tenant, Project: fixture.Project, Environment: fixture.Environment, Memberships: memberships,
			ServiceChanges: changes, Artifact: artifact, CreatedAt: task.CreatedAt, AllocateNamed: func(kind ids.Kind, purpose string) string {
				return ids.DeriveAt(kind, task.CreatedAt, task.ID, purpose)
			}},
	)
	if err != nil {
		t.Fatal(err)
	}
	members := prepared.Plan.GetCandidateReleaseProcedure().GetMembers()
	if len(members) != 1 || members[0].GetServiceId() != current.ServiceID ||
		members[0].GetCandidateReleaseId() == current.ReleaseID ||
		members[0].GetServingPredecessor().GetPriorReleaseId() != current.ReleaseID {
		t.Fatal("desired3/bundle2 did not publish one new candidate bound to C")
	}
	if fault != "" && fault != "bg-snapshot" {
		fixture.AssertNativeCandidateSourceRace(t, prepared.Task, projection, prepared.Publication, current, fault)
		return
	}
	fixture.Publish(t, prepared.Task, projection, prepared.Publication)
	frozen, err := fixture.Ledger.GetBlueprintTaskRenderInput(ctx, prepared.Task)
	if err != nil || len(frozen.NativePredecessors) != 1 || len(frozen.Members) != 1 ||
		frozen.Members[0].Intent.PriorServingReleaseID != current.ReleaseID ||
		frozen.Members[0].Render.PriorWorkload == nil ||
		frozen.Members[0].Render.PriorWorkload.ReplicaCount != current.CandidateWorkload.ReplicaCount {
		t.Fatalf("published native predecessor: %v", err)
	}
	replayed, err := resolver.ResolveExecutionPlan(ctx, prepared.Task)
	if err != nil || !proto.Equal(prepared.Plan, replayed) || imageResolver.calls != 1 {
		t.Fatalf("immutable native replay: %v", err)
	}
	agentID := ids.New(ids.KindAgent)
	claim, found, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("native claim: %v", err)
	}
	authority := claim.Assignment.Record.RestorationAuthority
	wantApplied, err := (proto.MarshalOptions{Deterministic: true}).Marshal(applied)
	if err != nil || authority == nil || authority.AppliedPredecessor == nil ||
		!bytes.Equal(authority.AppliedPredecessor.ComposeArtifact, wantApplied) ||
		len(authority.NativePredecessors) != 1 ||
		!bytes.Equal(authority.NativePredecessors[0].CurrentArtifact, frozen.NativePredecessors[0].CurrentArtifact) {
		t.Fatal("claim replaced A or lost immutable C")
	}
	if fault == "bg-snapshot" {
		witness := frozen.NativePredecessors[0]
		active, inactive := &agentpb.ComposeArtifact{}, &agentpb.ComposeArtifact{}
		if proto.Unmarshal(witness.CurrentArtifact, active) != nil ||
			proto.Unmarshal(witness.RetainedPriorArtifact, inactive) != nil ||
			witness.Serving == nil ||
			witness.Serving.RetainedPriorReleaseID == "" ||
			len(active.Services) != 2 ||
			len(inactive.Services) != 1 ||
			members[0].GetServingPredecessor().GetPriorArtifactId() != active.ArtifactId ||
			members[0].GetServingPredecessor().GetRetainedPriorArtifactId() != inactive.ArtifactId ||
			!bytes.Equal(authority.NativePredecessors[0].RetainedPriorArtifact, witness.RetainedPriorArtifact) {
			t.Fatal("candidate BG marker/plan/claim lost exact current or inactive source")
		}
		proxies := 0
		for _, service := range active.Services {
			if service.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
				proxies++
			} else if service.Slot != "green" || service.ExpectedReplicas != 1 {
				t.Fatal("BG current workload diverged")
			}
		}
		if proxies != 1 || inactive.Services[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT ||
			inactive.Services[0].Slot != "blue" ||
			inactive.Services[0].ExpectedReplicas != 1 {
			t.Fatal("BG source included an extra proxy or lost its inactive workload")
		}
		return
	}
	fixture.ProveNativeCandidateRecovery(t, agentID, claim, prepared.Plan, current.ReleaseID)
}

// SVC-15/BP-04: Rationale: the selected-candidate path carries both immutable blue/green
// sources and guards inactive source replacement/pruning before publication.
func TestBlueprintNativePredecessorBlueGreenSnapshot(t *testing.T) {
	for _, fault := range []string{"bg-snapshot", "inactive-replace", "inactive-prune"} {
		t.Run(fault, func(t *testing.T) {
			testBlueprintExecutedArtifact(
				t,
				true,
				false,
				func(fixture *etcd.ExecutedArtifactFixture, resolver *testtaskplanning.TaskPlanResolver, original testreleaserender.ReleaseRenderInput, intent domain.Intent, applied *agentpb.ComposeArtifact) {
					current, serving := fixture.SeedRetainedRollback(t, original, intent)
					current = fixture.SeedNativeBlueGreenPredecessor(t, current, serving)
					fixture.SeedNativeCandidateDesired(t, current, 2)
					proveNativeCandidatePreparation(t, fixture, resolver, current, applied, fault)
				},
			)
		})
	}
}

// SVC-15/BP-04: Rationale: replacement with identical bytes and pruning both invalidate the
// source revision; no candidate Task, marker, or desired head may publish.
func TestBlueprintNativePredecessorSourceCAS(t *testing.T) {
	for _, fault := range []string{"intent-replace", "intent-prune", "render-replace", "render-prune", "projection-replace", "applied-replace"} {
		t.Run(fault, func(t *testing.T) {
			testBlueprintExecutedArtifact(
				t,
				true,
				false,
				func(fixture *etcd.ExecutedArtifactFixture, resolver *testtaskplanning.TaskPlanResolver, original testreleaserender.ReleaseRenderInput, intent domain.Intent, applied *agentpb.ComposeArtifact) {
					current, _ := fixture.SeedRetainedRollback(t, original, intent)
					fixture.SeedNativeCandidateDesired(t, current, 3)
					proveNativeCandidatePreparation(t, fixture, resolver, current, applied, fault)
				},
			)
		})
	}
}
