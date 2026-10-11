package app

import (
	"bytes"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: a first-deploy Service child must not execute or replace an
// unrelated Service's exact acknowledged runtime, including across an ACK CAS.
func TestBlueprintMixedCandidatePreservesAcknowledgedRuntime(t *testing.T) {
	for _, servingRace := range []bool{false, true} {
		name := "publish-replay-ack"
		if servingRace {
			name = "unrelated-runtime-race"
		}
		t.Run(name, func(t *testing.T) { testBlueprintMixedCandidate(t, servingRace) })
	}
}

func testBlueprintMixedCandidate(t *testing.T, servingRace bool) {
	testBlueprintExecutedArtifact(t, false, false, func(fixture *ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
		_ *agentpb.ComposeArtifact,
	) {
		ctx := t.Context()
		if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
			t.Fatal(err)
		}
		acknowledged := fixture.AcknowledgedRuntime(t, prior.ServiceID)
		project, err := testcomposerender.LoadNormalizedEnvironmentProject(ctx, prior.Projection)
		if err != nil {
			t.Fatal(err)
		}
		owned, found, err := fixture.Hierarchy.GetEnvironmentOwnedIdentities(ctx, prior.EnvironmentID)
		if err != nil || !found {
			t.Fatalf("current owned identities: %t %v", found, err)
		}
		workerID := ids.New(ids.KindService)
		project.Services["worker"] = composetypes.ServiceConfig{
			Name:        "worker",
			Image:       prior.CandidateWorkload.RequestedReference,
			NetworkMode: "none",
			HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}},
		}
		task := fixture.Task(t, 1500)
		task.RenderGeneration = int32(owned.Record.RenderGeneration + 1)
		artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
			Project: project, ArtifactID: ids.New(ids.KindConfig),
			ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant,
			TenantID:         prior.TenantID, ProjectID: prior.ProjectID, EnvironmentID: prior.EnvironmentID,
			PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			AuthorizedVolumeDir: prior.AuthorizedVolumeDir,
			Identities: testcomposeidentity.Snapshot{Services: []testcomposeidentity.Resource{
				{ID: prior.ServiceID, Name: prior.ServiceName}, {ID: workerID, Name: "worker"},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		projection := prior.Projection
		projection.RevisionID, projection.RenderGeneration = task.ID, uint64(task.RenderGeneration)
		projection.ComposeArtifact, err = proto.MarshalOptions{Deterministic: true}.Marshal(artifact)
		if err != nil {
			t.Fatal(err)
		}
		projection.NormalizedCompose, err = project.MarshalYAML()
		if err != nil {
			t.Fatal(err)
		}
		worker := projection.DesiredServices[0]
		worker.Desired.ID, worker.Desired.Name = workerID, "worker"
		projection.DesiredServices = append(
			[]testservices.EnvironmentServiceProjection(nil),
			projection.DesiredServices[0],
			worker,
		)

		scripts, sources := fixture.HookDependencies(t)
		if err := resolver.EnableScriptPlans(scripts); err != nil {
			t.Fatal(err)
		}
		scriptArtifacts, err := testtaskplanning.NewScriptArtifactService(
			scripts, unexpectedHookEntryResolver{t: t},
		)
		if err != nil {
			t.Fatal(err)
		}
		imageResolver := &mixedCandidateImageResolver{t: t, seal: prior.CandidateWorkload}
		producer, err := blueprintrelease.NewService(
			fixture.Ledger,
			scripts,
			resolver,
			scriptArtifacts,
			sources,
			blueprintImageLookupAgent(t, fixture),
			imageResolver,
			unexpectedBlueprintRollingObserver{t},
		)
		if err != nil {
			t.Fatal(err)
		}
		revision := fixture.PublishNativeBlueprintRevision(t, task, projection, nil)
		prepared := fixture.PrepareNativeServiceUnit(t, producer, revision, workerID, nil, nil)
		members := prepared.Plan.GetCandidateReleaseProcedure().GetMembers()
		if len(members) != 1 || members[0].GetServiceId() != workerID || imageResolver.calls != 1 {
			t.Fatal("first-deploy child did not select exactly the new Service")
		}
		for _, candidate := range prepared.Plan.Artifacts {
			for _, service := range candidate.Services {
				if service.GetServiceId() == prior.ServiceID &&
					service.GetRole() != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_UNSPECIFIED {
					t.Fatal("new Service child included an unrelated executable runtime")
				}
			}
		}
		// Compose definitions may retain unselected Services; actual effects must
		// select only this child, never apply the entire Environment artifact.
		for _, step := range prepared.Plan.Steps {
			if apply := step.GetComposeApply(); apply != nil &&
				(len(apply.ServiceIds) != 1 || apply.ServiceIds[0] != workerID) {
				t.Fatal("new Service child applies an unrelated Service")
			}
		}
		if servingRace {
			fixture.AdvanceAcknowledgedRuntime(t, prior.ServiceID)
			acknowledged = fixture.AcknowledgedRuntime(t, prior.ServiceID)
		}
		fixture.PublishNativeServiceUnit(t, revision, workerID, prepared)
		replayed, err := resolver.ResolveExecutionPlan(ctx, prepared.Task)
		if err != nil || !proto.Equal(prepared.Plan, replayed) || imageResolver.calls != 1 {
			t.Fatalf("native first-deploy replay changed its captured rollout: %v", err)
		}
		agentID := ids.New(ids.KindAgent)
		claim, claimed, err := fixture.Tasks.ClaimNextTask(
			ctx, agentID, 1, prepared.Task.CreatedAt.Add(time.Second),
		)
		if err != nil || !claimed || claim.Task.Record.ID != prepared.Task.ID {
			t.Fatalf("native first-deploy claim: %t %v", claimed, err)
		}
		if _, err := fixture.Tasks.AcknowledgeTask(
			ctx,
			agentID,
			1,
			prepared.Task.ID,
			claim.Assignment.Record.AssignmentID,
			testtaskjournal.TaskStatusCompleted,
			testtaskjournal.TaskResultRecord{
				Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
				Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
			},
			prepared.Task.CreatedAt.Add(2*time.Second),
		); err != nil {
			t.Fatal(err)
		}
		current := fixture.AcknowledgedRuntime(t, prior.ServiceID)
		if current.Revision != acknowledged.Revision || !reflect.DeepEqual(current.Record, acknowledged.Record) {
			t.Fatal("new Service child changed unrelated acknowledged runtime authority")
		}
		latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, prior.EnvironmentID)
		executed, marshalErr := proto.MarshalOptions{Deterministic: true}.Marshal(prepared.Plan.Artifacts[0])
		if err != nil || marshalErr != nil || !found || !bytes.Equal(latest.Record.ComposeArtifact, executed) {
			t.Fatal("new Service completion did not persist its exact executed artifact")
		}
	})
}
