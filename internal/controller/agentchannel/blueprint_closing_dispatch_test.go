package agentchannel

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type closingBlueprintTaskStore struct{ fakeTaskStore }

func (store *closingBlueprintTaskStore) ReconnectAgentAssignment(
	_ context.Context, assignment etcd.TaskAssignment,
) (etcd.TaskAssignment, error) {
	assignment.Task.Record.Status = etcd.TaskStatusCompleted
	return assignment, nil
}

// QA: TASK-10, BP-10; injected reconnect completion, not durable Blueprint completion.
// Rationale: Controller-only completion must neither resolve closed Script
// artifacts nor consume the Agent slot needed by the next pending Task.
func TestDispatchReadySkipsControllerCompletedReconnect(t *testing.T) {
	at := testTime()
	agentID := ids.NewAt(ids.KindAgent, at, 921)
	closing := assignmentQuarantineFixture(at, agentID, 922)
	closing.Task.Record.Type = etcd.TaskUpdate
	closing.Task.Record.Params = map[string]string{etcd.TaskReleasePublicationParam: "published"}
	next := assignmentQuarantineFixture(at, agentID, 923)
	plan := testExecutionPlan(t, next.Task.Record)
	next.Task.Record.PlanHash = hex.EncodeToString(plan.PlanHash)
	store := &closingBlueprintTaskStore{fakeTaskStore{
		assignments: []etcd.TaskAssignment{closing}, claims: []etcd.TaskAssignment{next},
	}}
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), agentID, 7)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	server := New(nil, registry, store, &assignmentQuarantinePlanResolver{
		rejectedTaskID: closing.Task.Record.ID,
		plans:          map[string]*agentpb.ExecutionPlan{next.Task.Record.ID: plan},
	})
	server.now = func() time.Time { return at }
	stream := &scriptedStream{ctx: context.Background()}
	delivered, quarantined := make(map[string]string), make(map[string]string)
	err = server.dispatchReady(stream, session, agentID,
		Authorization{Generation: 7, Config: &agentpb.AgentConfig{MaxConcurrentTasks: 1}},
		1, delivered, quarantined)
	if err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) != 1 || stream.sent[0].GetTaskAssignment().GetTaskId() != next.Task.Record.ID ||
		len(quarantined) != 0 || len(delivered) != 1 ||
		delivered[next.Task.Record.ID] != next.Assignment.Record.AssignmentID {
		t.Fatal("Controller completion was dispatched, quarantined, or consumed capacity")
	}
	assignment := stream.sent[0].GetTaskAssignment()
	if assignment.GetAssignmentId() != next.Assignment.Record.AssignmentID ||
		hex.EncodeToString(assignment.GetPlan().GetPlanHash()) != next.Task.Record.PlanHash {
		t.Fatalf("next assignment identity = %v", assignment)
	}
}
