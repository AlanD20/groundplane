package etcd_test

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/servicelifecycle"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testreleases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

// Rationale: starting a new Service must preserve the exact physical metadata
// of an unselected Service already serving from a real acknowledged Release.
func TestBlueprintMixedCandidatePreservesAcknowledgedRuntime(t *testing.T) {
	for _, servingRace := range []bool{false, true} {
		name := "publish-replay-ack"
		if servingRace {
			name = "serving-race"
		}
		t.Run(name, func(t *testing.T) { testBlueprintMixedCandidate(t, servingRace) })
	}
}

func testBlueprintMixedCandidate(t *testing.T, servingRace bool) {
	testBlueprintExecutedArtifact(t, false, false, func(fixture *etcd.ExecutedArtifactFixture,
		resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, priorIntent domain.Intent,
		applied *agentpb.ComposeArtifact) {
		task := fixture.Task(t, 1500)
		if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
			t.Fatal(err)
		}
		task.RenderGeneration = 3
		artifactID, serviceID, releaseID := ids.New(
			ids.KindConfig,
		), ids.New(
			ids.KindService,
		), ids.New(
			ids.KindDeployment,
		)
		task.Params[testtaskjournal.TaskComposeArtifactParam] = artifactID
		task.Params[testreleaserender.TaskReleasePublicationParam] = ids.NewULID()
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(
			taskcontract.BlueprintComposeProcedureCandidateReleases,
		)
		project := &composetypes.Project{Name: "test", Services: composetypes.Services{}}
		for _, name := range []string{"api", "worker"} {
			project.Services[name] = composetypes.ServiceConfig{
				Name:        name,
				Image:       prior.CandidateWorkload.RequestedReference,
				NetworkMode: "none",
				HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}},
			}
		}
		artifact, err := testcomposerender.RenderCompose(testcomposerender.ComposeRenderInput{
			Project: project, ArtifactID: artifactID, ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant,
			TenantID: prior.TenantID, ProjectID: prior.ProjectID, EnvironmentID: prior.EnvironmentID,
			PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration), AuthorizedVolumeDir: prior.AuthorizedVolumeDir,
			Identities: testcomposeidentity.Snapshot{Services: []testcomposeidentity.Resource{
				{ID: prior.ServiceID, Name: "api"}, {ID: serviceID, Name: "worker"},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		artifact, err = testcomposerender.RetainBlueprintNativeRuntime(artifact, applied, []string{prior.ServiceID})
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
		worker.Desired.ID, worker.Desired.Name = serviceID, "worker"
		projection.DesiredServices = append(projection.DesiredServices[:1:1], worker)
		render := prior
		render.ReleaseID, render.PlanID, render.ArtifactID = releaseID, task.PlanID, artifactID
		render.ServiceID, render.ServiceName, render.Projection = serviceID, "worker", projection
		intent := priorIntent
		intent.ID, intent.ServiceID, intent.OperationID = releaseID, serviceID, task.OperationID
		intent.OriginatingTaskID, intent.RenderInputID, intent.CreatedAt = task.ID, artifactID, task.CreatedAt
		task, plan, err := resolver.PrepareBlueprintReleaseTask(
			context.Background(),
			task, testtaskplanning.BlueprintReleasePlanInput{
				Members:      []testreleaserender.ReleaseTaskRenderMember{{Intent: intent, Render: render}},
				ApplyStepIDs: []string{ids.New(ids.KindStep)}, HealthStepIDs: []string{ids.New(ids.KindStep)},
				RecoveryProbeStepIDs: []string{
					ids.New(ids.KindStep),
				}, RecoveryCompensateStepIDs: []string{ids.New(ids.KindStep)},
				PostStepIDs: [][]string{nil},
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range applied.Services {
			found := false
			for _, actual := range plan.Artifacts[0].Services {
				found = found || proto.Equal(expected, actual)
			}
			if !found {
				t.Fatalf("mixed candidate lost retained runtime %s", expected.ComposeName)
			}
		}
		publishMixedCandidate(t, fixture, resolver, task, render, intent, plan, prior.ServiceID, servingRace)
	})
}

func publishMixedCandidate(
	t *testing.T,
	fixture *etcd.ExecutedArtifactFixture,
	resolver *testtaskplanning.TaskPlanResolver,
	task etcd.TaskRecord,
	render testreleaserender.ReleaseRenderInput,
	intent domain.Intent,
	plan *agentpb.ExecutionPlan,
	retainedID string,
	servingRace bool,
) {
	t.Helper()
	ctx := context.Background()
	scope, err := fixture.Ledger.LoadPlanningScope(ctx, task.Target)
	if err != nil {
		t.Fatal(err)
	}
	planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{retainedID})
	if err != nil {
		t.Fatal(err)
	}
	applied, found, err := fixture.Ledger.GetPlanningAppliedProjection(ctx, scope)
	if err != nil || !found {
		t.Fatalf("capture applied source: %t %v", found, err)
	}
	raw, err := testreleaserender.EncodeReleaseRenderInput(render)
	if err != nil {
		t.Fatal(err)
	}
	intent.RenderInputDigest, err = domain.Digest(json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := fixture.Ledger.Stage(ctx, testreleases.ReleaseStage{
		PublicationID: task.Params[testreleaserender.TaskReleasePublicationParam], OperationID: task.OperationID, CreatedAt: task.CreatedAt,
		Members: []testreleases.ReleaseStageMember{{Intent: intent, RenderInput: raw,
			Checkpoint: domain.Checkpoint{
				ReleaseID: intent.ID,
				State:     domain.StatePending,
				UpdatedAt: task.CreatedAt,
			}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := executionplan.DescribeCandidateRelease(plan)
	if err != nil {
		t.Fatal(err)
	}
	publication, err := fixture.Ledger.PrepareBlueprintReleasePublication(ctx, nil,
		etcd.BlueprintReleasePublicationEvidence{Manifest: manifest, EnvironmentID: task.Target, Task: task,
			CandidateReleaseDescriptor: descriptor, Plan: plan, PublishedAt: task.CreatedAt})
	if err != nil {
		t.Fatal(err)
	}
	mixed := &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(render.Projection.ComposeArtifact, mixed); err != nil {
		t.Fatal(err)
	}
	retained, err := servicelifecycle.CaptureRelease(ctx, fixture.Ledger, applied, task.Target, retainedID)
	if err != nil {
		t.Fatal(err)
	}
	serving, err := fixture.Ledger.ResolveServing(ctx, task.Target, retainedID, applied.ReadRevision)
	if err != nil {
		t.Fatal(err)
	}
	publication, err = fixture.Ledger.PrepareBlueprintRuntimeRetention(
		ctx,
		publication,
		task,
		applied,
		[]etcd.BlueprintRetainedRuntimeSource{{Planning: planning[0], Release: &retained, Intent: &serving}},
		mixed,
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.AssertMixedRuntimeTamperingRejected(t, task, render.Projection, publication)
	if servingRace {
		fixture.RejectChangedServingPublication(t, task, render.Projection, publication, retainedID)
		return
	}
	fixture.Publish(t, task, render.Projection, publication)
	replayed, err := resolver.ResolveExecutionPlan(ctx, task)
	if err != nil || !proto.Equal(plan, replayed) {
		t.Fatalf("mixed candidate replay mismatch: %v", err)
	}
	agentID := ids.New(ids.KindAgent)
	claim, claimed, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !claimed || claim.Task.Record.ID != task.ID {
		t.Fatalf("mixed candidate claim: %t %v", claimed, err)
	}
	if _, err := fixture.Tasks.AcknowledgeTask(ctx, agentID, 1, task.ID, claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
		Diagnostic: testtaskjournal.TaskResultDiagnosticNone}, task.CreatedAt.Add(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, task.Target)
	if err != nil || !found {
		t.Fatalf("mixed candidate applied projection: %t %v", found, err)
	}
	expected, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.Artifacts[0])
	if err != nil || !bytes.Equal(latest.Record.ComposeArtifact, expected) {
		t.Fatalf("mixed candidate terminal did not preserve exact executed artifact: %v", err)
	}
}
