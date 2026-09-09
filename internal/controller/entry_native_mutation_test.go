package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"testing"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/core"
	domain "github.com/AlanD20/groundplane/internal/core/release"
	"github.com/AlanD20/groundplane/internal/infra/docker/composehelper"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// Rationale: creating configuration after a native Release must preserve its
// retained proxy/slot authority instead of publishing an incomplete fresh pair.
func TestEntryMutationPreservesRetainedNativeOwnership(t *testing.T) {
	_, _, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	render := input.Members[0].Render
	current := render.Projection
	current.ComposeArtifact = append([]byte(nil), render.PriorRuntime.CurrentArtifact...)
	entry := etcd.EntryRecord{EnvironmentID: current.EnvironmentID,
		Entry: core.EnvEntry{ID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FC1", Kind: core.EntryKindEnv,
			Key: "MODE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
		CurrentValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC2"}
	candidate, _, err := ProjectEnvironmentEntryMutation(current, EnvironmentEntryArtifactMutation{
		RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FC3", ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC4",
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FC5", RenderGeneration: current.RenderGeneration + 1,
		Entries: []etcd.EntryRecord{entry},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, after := &agentpb.ComposeArtifact{}, &agentpb.ComposeArtifact{}
	if err := proto.Unmarshal(current.ComposeArtifact, before); err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(candidate.ComposeArtifact, after); err != nil {
		t.Fatal(err)
	}
	for index, service := range before.Services {
		if !proto.Equal(service, after.Services[index]) {
			t.Fatalf("Entry mutation replaced retained %s ownership", service.ComposeName)
		}
	}
}

// Rationale: publication and assignment must reconstruct the same pinned Entry
// procedure, and the helper must not start its retained proxy or dependencies.
func TestEntryMutationPlanReconstructsAndSelectsRetainedWorkload(t *testing.T) {
	_, _, input := redeployRestorationInput(t, domain.StrategyBlueGreen)
	render := input.Members[0].Render
	current := render.Projection
	current.ComposeArtifact = append([]byte(nil), render.PriorRuntime.CurrentArtifact...)
	entry := etcd.EntryRecord{EnvironmentID: current.EnvironmentID,
		Entry: core.EnvEntry{ID: "ev_01ARZ3NDEKTSV4RRFFQ69G5FC1", Kind: core.EntryKindEnv,
			Key: "MODE", Source: core.EntrySource{Kind: core.SourceLiteral}, Exposure: []string{"api"}},
		CurrentValueGenerationID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC2"}
	candidate, materials, err := ProjectEnvironmentEntryMutation(current, EnvironmentEntryArtifactMutation{
		RevisionID: "task_01ARZ3NDEKTSV4RRFFQ69G5FC3", ArtifactID: "cfg_01ARZ3NDEKTSV4RRFFQ69G5FC4",
		PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FC5", RenderGeneration: current.RenderGeneration + 1,
		Entries: []etcd.EntryRecord{entry},
	})
	if err != nil {
		t.Fatal(err)
	}
	task := etcd.TaskRecord{ID: "task_01ARZ3NDEKTSV4RRFFQ69G5FC3", PlanID: "plan_01ARZ3NDEKTSV4RRFFQ69G5FC5",
		Type: etcd.TaskUpdate, Executor: etcd.TaskExecutorAgent, Target: current.EnvironmentID,
		RenderGeneration: int32(candidate.RenderGeneration), TimeoutSeconds: 120}
	for _, material := range materials {
		digest := sha256.Sum256([]byte("MODE=test\n"))
		task.Materializations = append(task.Materializations, etcd.TaskMaterializationRecord{
			StepID: ids.New(ids.KindStep), MaterializationID: ids.New(ids.KindConfig),
			EnvironmentID: current.EnvironmentID, Destination: material.Destination,
			ServiceID: material.ServiceID, ServiceName: material.ServiceName, OutputKind: material.OutputKind,
			UID: material.UID, GID: material.GID, Mode: uint32(material.Mode),
			Length: 10, SHA256: hex.EncodeToString(digest[:]), Source: material.Source,
		})
	}
	task, err = (EntryMutationRuntime{Projection: current, EpochRevision: 1}).PrepareTask(
		"/var/lib/groundplane/vol",
		task,
		candidate,
		"step_01ARZ3NDEKTSV4RRFFQ69G5FC9",
	)
	if err != nil {
		t.Fatal(err)
	}
	reader := &volumePlanReader{projections: map[string]etcd.EnvironmentComposeProjection{
		current.RevisionID: current, candidate.RevisionID: candidate,
	}}
	resolver, err := NewTaskPlanResolverWithBlueprints("/var/lib/groundplane/vol", reader, nil)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := resolver.ResolveExecutionPlan(t.Context(), task)
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(plan.PlanHash) != task.PlanHash || plan.GetEntryMutationProcedure() == nil {
		t.Fatal("Entry assignment changed the published plan")
	}
	step := plan.Steps[len(plan.Steps)-1]
	request := &agentpb.ComposeHelperRequest{Schema: composehelper.SchemaVersion, Plan: plan,
		StepId: step.StepId, TimeoutSeconds: 120, TaskId: task.ID,
		AssignmentId: "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAV", OperationId: "op_01ARZ3NDEKTSV4RRFFQ69G5FAV"}
	services, err := composehelper.StartupServices(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 1 || services[0].Role != agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_WORKLOAD_SLOT {
		t.Fatalf("Entry startup selected non-workloads: %v", services)
	}
	fake := runner.NewFake()
	if _, err := composehelper.Execute(t.Context(), fake, request); err != nil {
		t.Fatal(err)
	}
	calls := fake.RecordedCalls()
	if len(calls) != 2 {
		t.Fatalf("Entry helper calls=%v", calls)
	}
	args := calls[1].Args
	up := slices.Index(args, "up")
	if up < 0 || !slices.Equal(args[up:], []string{"up", "--detach", "--no-deps", services[0].ComposeName}) {
		t.Fatalf("Entry helper widened its selection: %v", args)
	}
}
