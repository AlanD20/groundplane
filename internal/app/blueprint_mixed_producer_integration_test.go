package app

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/blueprintrelease"
	testcomposeidentity "github.com/AlanD20/groundplane/internal/controller/composeidentity"
	testcomposerender "github.com/AlanD20/groundplane/internal/controller/composerender"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	testtaskplanning "github.com/AlanD20/groundplane/internal/controller/taskplanning"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testblueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	testreleaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	testservices "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
	composetypes "github.com/compose-spec/compose-go/v2/types"
	"google.golang.org/protobuf/proto"
)

type mixedCandidateImageResolver struct {
	t               *testing.T
	calls           int
	seal            domain.WorkloadSeal
	withPredecessor bool
}

func (r *mixedCandidateImageResolver) ResolveWorkloadImages(
	_ context.Context,
	agentID string,
	selectors []*agentpb.WorkloadImageSelector,
) (*agentpb.WorkloadImageResolutionResult, error) {
	r.calls++
	want := 1
	if r.withPredecessor {
		want = 2
	}
	if agentID == "" || len(selectors) != want || selectors[0].GetRequestedReference() != r.seal.RequestedReference {
		r.t.Fatal("mixed preflight image selection diverged")
	}
	resolutions := []*agentpb.WorkloadImageResolution{
		{Selector: proto.CloneOf(selectors[0]), LocalImageId: r.seal.LocalImageID},
	}
	if r.withPredecessor {
		if selectors[1].GetLocalImageId() != r.seal.LocalImageID {
			r.t.Fatal("mixed preflight did not select the sealed predecessor image")
		}
		resolutions = append(
			resolutions,
			&agentpb.WorkloadImageResolution{Selector: proto.CloneOf(selectors[1]), LocalImageId: r.seal.LocalImageID},
		)
	}
	return &agentpb.WorkloadImageResolutionResult{RequestId: strings.Repeat("1", 32),
		Outcome: &agentpb.WorkloadImageResolutionResult_Success{Success: &agentpb.WorkloadImageResolutions{
			Resolutions: resolutions,
		}}}, nil
}

// Rationale: full preflight/pre-stage merge/producer must preserve serving
// replicas and proxy while starting an independently selected new Service.
func TestBlueprintMixedProducerPreservesServingServices(t *testing.T) {
	for _, addressable := range []bool{false, true} {
		name := "portless"
		if addressable {
			name = "addressable"
		}
		t.Run(name, func(t *testing.T) {
			testBlueprintExecutedArtifact(t, addressable, false, func(fixture *ExecutedArtifactFixture,
				resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
				applied *agentpb.ComposeArtifact) {
				proveFullMixedProducer(t, fixture, resolver, prior, applied, addressable, false, 1, 0, false)
			})
		})
	}
}

func proveFullMixedProducer(
	t *testing.T,
	fixture *ExecutedArtifactFixture,
	resolver *testtaskplanning.TaskPlanResolver,
	prior testreleaserender.ReleaseRenderInput,
	applied *agentpb.ComposeArtifact,
	addressable, recoverMixed bool,
	workerCount, hookCount int,
	reconnectHooks bool,
) {
	t.Helper()
	ctx := context.Background()
	if err := resolver.EnableReleasePlans(fixture.Ledger); err != nil {
		t.Fatal(err)
	}
	scope, err := fixture.Ledger.LoadPlanningScope(ctx, prior.EnvironmentID)
	if err != nil {
		t.Fatal(err)
	}
	planning, err := fixture.Ledger.LoadPlanningServices(ctx, scope, []string{prior.ServiceID})
	if err != nil {
		t.Fatal(err)
	}
	imageResolver := &mixedCandidateImageResolver{t: t, seal: prior.CandidateWorkload, withPredecessor: recoverMixed}
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
		imageResolver,
	)
	if err != nil {
		t.Fatal(err)
	}
	worker := planning[0].Service.Record
	worker.Desired.ID, worker.Desired.Name = ids.New(ids.KindService), "worker"
	worker.Runtime.ServiceID, worker.Runtime.RuntimeIntent = worker.Desired.ID, core.ServiceRuntimeIntentRunning
	changes := []testblueprints.EnvironmentBlueprintServiceChange{
		{Current: &planning[0].Service, Record: planning[0].Service.Record}, {Record: worker},
	}
	for index := 1; index < workerCount; index++ {
		next := worker
		next.Desired.ID, next.Desired.Name = ids.New(ids.KindService), fmt.Sprintf("worker-%d", index)
		next.Runtime.ServiceID = next.Desired.ID
		changes = append(changes, testblueprints.EnvironmentBlueprintServiceChange{Record: next})
	}
	if recoverMixed {
		changes[0].Record.Desired.Replicas++
	}
	slices.SortFunc(changes, func(a, b testblueprints.EnvironmentBlueprintServiceChange) int {
		return strings.Compare(a.Record.Desired.Name, b.Record.Desired.Name)
	})
	project := &composetypes.Project{Name: "test", Services: composetypes.Services{}}
	identities := make([]testcomposeidentity.Resource, 0, len(changes))
	for _, change := range changes {
		name := change.Record.Desired.Name
		identities = append(identities, testcomposeidentity.Resource{ID: change.Record.Desired.ID, Name: name})
		definition := composetypes.ServiceConfig{Name: name, Image: prior.CandidateWorkload.RequestedReference,
			NetworkMode: "none", HealthCheck: &composetypes.HealthCheckConfig{Test: []string{"CMD", "true"}}}
		if hookCount > 0 && name == worker.Desired.Name {
			definition.NetworkMode = ""
		}
		if addressable {
			definition.Expose = []string{"8080"}
		}
		project.Services[name] = definition
	}
	previous := &composetypes.Project{
		Name:     project.Name,
		Services: composetypes.Services{"api": project.Services["api"]},
	}
	slices.SortFunc(identities, func(a, b testcomposeidentity.Resource) int {
		return strings.Compare(a.Name, b.Name)
	})
	if recoverMixed {
		definition := project.Services["api"]
		definition.Scale = &changes[0].Record.Desired.Replicas
		project.Services["api"] = definition
	}
	memberships, err := blueprintrelease.BuildNormalizedServiceMemberships(previous, project)
	if err != nil {
		t.Fatal(err)
	}
	before := fixture.ReadRevision()
	workloads, err := producer.Preflight(ctx, prior.EnvironmentID, changes, memberships, nil)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.ReadRevision() != before || imageResolver.calls != 1 {
		t.Fatal("preflight wrote state or repeated image lookup")
	}
	task := fixture.Task(t, 1700)
	if recoverMixed {
		task.CreatedAt = time.Now().UTC()
		task.UpdatedAt = task.CreatedAt
	}
	task.RenderGeneration = 3
	artifactID := ids.New(ids.KindConfig)
	task.Params[taskcontract.EnvironmentBlueprintArtifactParam] = artifactID
	task.Params[taskcontract.EnvironmentBlueprintProcedureParam] = string(taskcontract.BlueprintComposeProcedureNone)
	artifact, err := testcomposerender.RenderCompose(
		testcomposerender.ComposeRenderInput{Project: project, ArtifactID: artifactID,
			ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: prior.TenantID, ProjectID: prior.ProjectID,
			EnvironmentID: prior.EnvironmentID, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			AuthorizedVolumeDir: prior.AuthorizedVolumeDir, Identities: testcomposeidentity.Snapshot{Services: identities}},
	)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err = producer.PrepareRuntimeArtifact(workloads, artifact, changes)
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
	projection.DesiredServices = projection.DesiredServices[:1:1]
	for _, change := range changes[1:] {
		projection.DesiredServices = append(projection.DesiredServices, testservices.EnvironmentServiceProjection{
			EnvironmentID: prior.EnvironmentID, Desired: change.Record.Desired,
		})
	}
	projection.DesiredServices[0].Desired = changes[0].Record.Desired
	hooks := fixture.StageHookScripts(t, task, worker, hookCount)
	tenant, err := fixture.Hierarchy.GetTenant(ctx, prior.TenantID)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := producer.Prepare(ctx, blueprintrelease.PrepareInput{
		Scripts:   hooks,
		Workloads: workloads, VolumeRoot: "/var/lib/groundplane/vol", Task: task, Projection: projection,
		Tenant:  tenant,
		Project: fixture.Project, Environment: fixture.Environment, Memberships: memberships, ServiceChanges: changes,
		Artifact: artifact, CreatedAt: task.CreatedAt, AllocateNamed: func(kind ids.Kind, purpose string) string {
			return ids.DeriveAt(kind, task.CreatedAt, task.ID, purpose)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range applied.Services {
		if recoverMixed {
			break // This journey replaces the old native as well as starting worker.
		}
		found := false
		for _, actual := range prepared.Plan.Artifacts[0].Services {
			found = found || proto.Equal(expected, actual)
		}
		if !found {
			t.Fatal("full producer lost retained runtime metadata")
		}
	}
	fixture.Publish(t, prepared.Task, projection, prepared.Publication)
	replayed, err := resolver.ResolveExecutionPlan(ctx, prepared.Task)
	if err != nil || !proto.Equal(prepared.Plan, replayed) {
		t.Fatalf(
			"full producer replay: %v (params=%v timeout=%d)",
			err,
			prepared.Task.Params,
			prepared.Task.TimeoutSeconds,
		)
	}
	if imageResolver.calls != 1 {
		t.Fatal("producer or replay re-resolved images")
	}
	agentID := ids.New(ids.KindAgent)
	claim, claimed, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, task.CreatedAt.Add(time.Second))
	if err != nil || !claimed || claim.Task.Record.ID != task.ID {
		t.Fatalf("full producer claim: %t %v", claimed, err)
	}
	if reconnectHooks {
		claim = fixture.AtMaximumExecutionEpoch(t, claim)
	}
	if hookCount > 0 {
		fixture.CompleteHookCheckpoints(t, agentID, claim, prepared.Plan)
		fixture.RejectOversizedHookTerminal(t, agentID, claim)
	}
	if reconnectHooks {
		fixture.ProveHookTerminalReconnect(t, agentID, claim, prepared.Plan)
		return
	}
	if recoverMixed {
		fixture.ProveMixedMemberRecovery(
			t,
			agentID,
			claim,
			prepared.Plan,
			prior.ServiceID,
			prior.ReleaseID,
			func(recovery etcd.TaskAssignment, expected testtaskjournal.TaskResultRecord) testtaskjournal.TaskResultRecord {
				return executeMixedWorkerRecovery(t, fixture, recovery, prepared.Plan, expected)
			},
		)
		return
	}
	result := testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
		Diagnostic: testtaskjournal.TaskResultDiagnosticNone}
	completed, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(time.Minute),
	)
	if err != nil || completed.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("full producer terminal completion: %s %v", completed.Record.Status, err)
	}
	replayedTerminal, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		task.CreatedAt.Add(2*time.Minute),
	)
	if err != nil || replayedTerminal.Revision != completed.Revision ||
		replayedTerminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("full producer terminal replay changed authority: %v", err)
	}
	latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, task.Target)
	if err != nil || !found {
		t.Fatalf("full producer applied projection: %t %v", found, err)
	}
	executed, err := proto.MarshalOptions{Deterministic: true}.Marshal(prepared.Plan.Artifacts[0])
	if err != nil || !bytes.Equal(latest.Record.ComposeArtifact, executed) {
		t.Fatal("full producer completion changed the executed mixed artifact")
	}
}

// Rationale: a real selected serving member and a first candidate must carry
// distinct targets through publication, claim, recovery, and final replay.
func TestBlueprintMixedProducerRecoversServingAndFirstCandidate(t *testing.T) {
	for _, addressable := range []bool{false, true} {
		name := "portless"
		if addressable {
			name = "addressable"
		}
		t.Run(name, func(t *testing.T) {
			testBlueprintExecutedArtifact(t, addressable, false, func(fixture *ExecutedArtifactFixture,
				resolver *testtaskplanning.TaskPlanResolver, prior testreleaserender.ReleaseRenderInput, _ domain.Intent,
				applied *agentpb.ComposeArtifact) {
				proveFullMixedProducer(t, fixture, resolver, prior, applied, addressable, true, 1, 0, false)
			})
		})
	}
}
