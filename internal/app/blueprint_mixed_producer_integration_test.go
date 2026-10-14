package app

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
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
		r.t.Fatal("mixed Service-unit image selection diverged")
	}
	resolutions := []*agentpb.WorkloadImageResolution{
		{Selector: proto.CloneOf(selectors[0]), LocalImageId: r.seal.LocalImageID},
	}
	if r.withPredecessor {
		if selectors[1].GetLocalImageId() != r.seal.LocalImageID {
			r.t.Fatal("mixed Service unit did not select the sealed predecessor image")
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

// Rationale: the native Service-unit producer must preserve serving replicas
// and proxy while starting an independently selected new Service.
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
	acknowledged, err := fixture.Ledger.LoadAcknowledgedServiceRuntimesAtRevision(
		ctx, prior.EnvironmentID, []string{prior.ServiceID}, scope.ReadRevision,
	)
	if err != nil {
		t.Fatal(err)
	}
	scripts, sources := fixture.HookDependencies(t)
	if err := resolver.EnableScriptPlans(scripts); err != nil {
		t.Fatal(err)
	}
	artifacts, err := testtaskplanning.NewScriptArtifactService(scripts, unexpectedHookEntryResolver{t: t})
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
	slices.SortFunc(identities, func(a, b testcomposeidentity.Resource) int {
		return strings.Compare(a.Name, b.Name)
	})
	if recoverMixed {
		definition := project.Services["api"]
		definition.Scale = &changes[0].Record.Desired.Replicas
		project.Services["api"] = definition
	}
	task := fixture.Task(t, 1700)
	if recoverMixed {
		task.CreatedAt = time.Now().UTC()
		task.UpdatedAt = task.CreatedAt
	}
	task.RenderGeneration = 3
	artifactID := ids.New(ids.KindConfig)
	artifact, err := testcomposerender.RenderCompose(
		testcomposerender.ComposeRenderInput{Project: project, ArtifactID: artifactID,
			ProjectOwnerKind: testcomposerender.ComposeProjectOwnerTenant, TenantID: prior.TenantID, ProjectID: prior.ProjectID,
			EnvironmentID: prior.EnvironmentID, PlanID: task.PlanID, RenderGeneration: uint64(task.RenderGeneration),
			AuthorizedVolumeDir: prior.AuthorizedVolumeDir, Identities: testcomposeidentity.Snapshot{Services: identities}},
	)
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
	revision := fixture.PublishNativeBlueprintRevision(t, task, projection, hooks)
	targets := make([]string, 0, len(changes))
	if recoverMixed {
		for _, change := range changes {
			targets = append(targets, change.Record.Desired.ID)
		}
	} else {
		for _, change := range changes {
			if change.Record.Desired.ID != prior.ServiceID {
				targets = append(targets, change.Record.Desired.ID)
			}
		}
	}
	if len(targets) == 0 {
		t.Fatal("native mixed fixture has no selected Service unit")
	}
	preparedUnits := make([]blueprintrelease.Prepared, len(targets))
	imageResolvers := make([]*mixedCandidateImageResolver, len(targets))
	for index, serviceID := range targets {
		imageResolvers[index] = &mixedCandidateImageResolver{
			t: t, seal: prior.CandidateWorkload, withPredecessor: serviceID == prior.ServiceID,
		}
		producer, producerErr := blueprintrelease.NewService(
			fixture.Ledger,
			scripts,
			resolver,
			artifacts,
			sources,
			blueprintImageLookupAgent(t, fixture),
			imageResolvers[index],
			unexpectedBlueprintRollingObserver{t},
		)
		if producerErr != nil {
			t.Fatal(producerErr)
		}
		selectedScripts := hooks
		if serviceID != worker.Desired.ID {
			selectedScripts = nil
		}
		preparedUnits[index] = fixture.PrepareNativeServiceUnit(
			t, producer, revision, serviceID, selectedScripts, nil,
		)
		members := preparedUnits[index].Plan.GetCandidateReleaseProcedure().GetMembers()
		if len(members) != 1 || members[0].GetServiceId() != serviceID {
			t.Fatal("native Service unit did not select exactly its own candidate")
		}
		fixture.PublishNativeServiceUnit(t, revision, serviceID, preparedUnits[index])
	}
	prepared := preparedUnits[0]
	if !recoverMixed {
		for _, unit := range preparedUnits {
			for _, member := range unit.Plan.GetCandidateReleaseProcedure().GetMembers() {
				if member.ServiceId == prior.ServiceID {
					t.Fatal("independent Service unit executes the unchanged serving workload")
				}
			}
		}
		currentScope, readErr := fixture.Ledger.LoadPlanningScope(ctx, prior.EnvironmentID)
		if readErr != nil {
			t.Fatal(readErr)
		}
		current, readErr := fixture.Ledger.LoadAcknowledgedServiceRuntimesAtRevision(
			ctx, prior.EnvironmentID, []string{prior.ServiceID}, currentScope.ReadRevision,
		)
		if readErr != nil || current[0].Revision != acknowledged[0].Revision ||
			!reflect.DeepEqual(current[0].Record, acknowledged[0].Record) {
			t.Fatalf("independent Service publication changed acknowledged serving authority: %v", readErr)
		}
	}
	replayed, err := resolver.ResolveExecutionPlan(ctx, prepared.Task)
	if err != nil || !proto.Equal(prepared.Plan, replayed) {
		t.Fatalf(
			"full producer replay: %v (params=%v timeout=%d)",
			err,
			prepared.Task.Params,
			prepared.Task.TimeoutSeconds,
		)
	}
	for _, imageResolver := range imageResolvers {
		if imageResolver.calls != 1 {
			t.Fatal("producer or replay re-resolved images")
		}
	}
	agentID := ids.New(ids.KindAgent)
	claim, claimed, err := fixture.Tasks.ClaimNextTask(ctx, agentID, 1, prepared.Task.CreatedAt.Add(time.Second), 32)
	if err != nil || !claimed {
		t.Fatalf("full producer claim: %t %v", claimed, err)
	}
	preparedByTask := make(map[string]blueprintrelease.Prepared, len(preparedUnits))
	for _, unit := range preparedUnits {
		preparedByTask[unit.Task.ID] = unit
	}
	prepared, found := preparedByTask[claim.Task.Record.ID]
	if !found {
		t.Fatal("full producer claimed a Task outside its sibling Service units")
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
		for remaining := len(preparedUnits); remaining > 0; remaining-- {
			member := prepared.Plan.GetCandidateReleaseProcedure().GetMembers()[0]
			priorID := ""
			if member.GetServiceId() == prior.ServiceID {
				priorID = prior.ReleaseID
			}
			var worker []func(etcd.TaskAssignment, testtaskjournal.TaskResultRecord) testtaskjournal.TaskResultRecord
			if len(preparedUnits) == 1 {
				worker = append(
					worker,
					func(recovery etcd.TaskAssignment, expected testtaskjournal.TaskResultRecord) testtaskjournal.TaskResultRecord {
						return executeMixedWorkerRecovery(t, fixture, recovery, prepared.Plan, expected)
					},
				)
			}
			fixture.ProveMixedMemberRecovery(
				t, agentID, claim, prepared.Plan, member.GetServiceId(), priorID, worker...,
			)
			delete(preparedByTask, prepared.Task.ID)
			if remaining == 1 {
				break
			}
			claim, claimed, err = fixture.Tasks.ClaimNextTask(
				ctx, agentID, 1, prepared.Task.CreatedAt.Add(time.Minute), 32,
			)
			if err != nil || !claimed {
				t.Fatalf("sibling Service-unit claim: %t %v", claimed, err)
			}
			prepared, found = preparedByTask[claim.Task.Record.ID]
			if !found {
				t.Fatal("claimed sibling is outside the authored revision")
			}
		}
		return
	}
	result := testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose, ExecutionEpoch: 1,
		Diagnostic: testtaskjournal.TaskResultDiagnosticNone}
	completed, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		prepared.Task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		prepared.Task.CreatedAt.Add(time.Minute),
	)
	if err != nil || completed.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("full producer terminal completion: %s %v", completed.Record.Status, err)
	}
	replayedTerminal, err := fixture.Tasks.AcknowledgeTask(
		ctx,
		agentID,
		1,
		prepared.Task.ID,
		claim.Assignment.Record.AssignmentID,
		testtaskjournal.TaskStatusCompleted,
		result,
		prepared.Task.CreatedAt.Add(2*time.Minute),
	)
	if err != nil || replayedTerminal.Revision != completed.Revision ||
		replayedTerminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("full producer terminal replay changed authority: %v", err)
	}
	latest, found, err := fixture.Hierarchy.GetEnvironmentAppliedComposeProjection(ctx, prepared.Task.Target)
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
