package taskplanning

import (
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

// SVC-04: a removal must select the actual deployed singleton and proxy, not
// merely the unsuffixed desired name that previously left the workload alive.
func assertRemovalSelectsAcknowledgedMembers(
	t *testing.T,
	resolver *TaskPlanResolver,
	task etcd.TaskRecord,
	artifact *agentpb.ComposeArtifact,
) {
	t.Helper()
	raw, err := proto.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	at := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	runtime := executionplan.CandidateRuntime{
		ServiceID: task.Target, ReleaseID: "dep_01ARZ3NDEKTSV4RRFFQ69G5FAV", Target: "singleton",
		CurrentArtifact: raw, ProxyGeneration: 4,
	}
	for _, member := range artifact.Services {
		if member.Role == agentpb.ComposeServiceRole_COMPOSE_SERVICE_ROLE_STABLE_PROXY {
			runtime.ProxyConfigSHA256 = member.ProxyConfigSha256
		}
	}
	record := serviceruntimerecord.Record{EnvironmentID: artifact.OwnerId, Runtime: runtime,
		Source: serviceruntimerecord.Acknowledgement{
			TaskID: ids.NewAt(ids.KindTask, at, 1), PlanID: task.PlanID, StepID: task.Steps[0].ID,
			AgentID: ids.NewAt(ids.KindAgent, at, 2), AssignmentID: ids.NewAt(ids.KindAssignment, at, 3),
			ExecutionEpoch: 1, RenderGeneration: uint64(task.RenderGeneration),
			PlanHash: strings.Repeat("a", 64), EffectDigest: strings.Repeat("b", 64), AcknowledgedAt: at,
		}}
	task.Type, task.CreatedAt = taskjournal.TaskRemove, at
	task.Steps = []taskjournal.TaskStepRecord{
		{ID: ids.NewAt(ids.KindStep, at, 4), Kind: taskjournal.TaskStepOperation},
		{ID: ids.NewAt(ids.KindStep, at, 5), Kind: taskjournal.TaskStepOperation},
	}
	plan, err := resolver.buildAcknowledgedServiceRemovalPlan(task, environmentchanges.ServiceRemovalIntent{
		ServiceID: task.Target, AcknowledgedRuntime: &record,
	})
	if err != nil {
		t.Fatalf("build removal from acknowledged runtime: %v", err)
	}
	if len(plan.Steps) != 2 {
		t.Fatalf("removal steps = %d, want proxy and workload", len(plan.Steps))
	}
	names := make(map[string]uint32)
	for index, step := range plan.Steps {
		selected := plan.Artifacts[index]
		if step.GetComposeRemove() == nil || step.GetComposeRemove().WholeProject ||
			step.GetComposeRemove().ArtifactId != selected.ArtifactId {
			t.Fatal("removal did not select the captured physical member")
		}
		names[selected.Services[0].ComposeName] = selected.Services[0].ExpectedReplicas
	}
	if names["api"] != 1 || names["api--singleton"] != 2 {
		t.Fatalf("removal selected %v, want proxy and both deployed replicas", names)
	}
}
