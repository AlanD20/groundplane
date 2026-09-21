package agentchannel

import (
	"context"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type assignmentQuarantinePlanResolver struct {
	rejectedTaskID string
	plans          map[string]*agentpb.ExecutionPlan
}

func (resolver *assignmentQuarantinePlanResolver) ResolveExecutionPlan(
	_ context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if task.ID == resolver.rejectedTaskID {
		return nil, errs.New(errs.KindInternal, "sealed plan cannot be reconstructed")
	}
	return resolver.plans[task.ID], nil
}

// QA: TASK-09/10, HOST-05; one fake-store dispatch, not timeout or reconnect recovery.
// Rationale: an unrenderable recovered assignment must be quarantined without
// preventing a valid unrelated assignment from being sent in the same dispatch.
func TestDispatchReadyQuarantinesUnrenderableRecoveredAssignment(t *testing.T) {
	t.Parallel()

	at := testTime()
	agentID := ids.NewAt(ids.KindAgent, at, 901)
	first := assignmentQuarantineFixture(at, agentID, 902)
	second := assignmentQuarantineFixture(at, agentID, 903)
	secondPlan := testExecutionPlan(t, second.Task.Record)
	second.Task.Record.PlanHash = hex.EncodeToString(secondPlan.PlanHash)

	store := &fakeTaskStore{assignments: []etcd.TaskAssignment{first, second}}
	registry := NewRegistry()
	session, err := registry.Open(context.Background(), agentID, 7)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer session.Close()
	server := New(nil, registry, store, &assignmentQuarantinePlanResolver{
		rejectedTaskID: first.Task.Record.ID,
		plans:          map[string]*agentpb.ExecutionPlan{second.Task.Record.ID: secondPlan},
	})
	stream := &scriptedStream{ctx: context.Background()}
	server.now = func() time.Time { return at }
	delivered := make(map[string]string)
	quarantined := make(map[string]string)

	err = server.dispatchReady(
		stream,
		session,
		agentID,
		Authorization{Generation: 7, Config: &agentpb.AgentConfig{MaxConcurrentTasks: 2}},
		2,
		delivered,
		quarantined,
	)
	if err != nil {
		t.Fatalf("dispatchReady() error = %v", err)
	}
	if len(stream.sent) != 1 || stream.sent[0].GetTaskAssignment().GetTaskId() != second.Task.Record.ID {
		t.Fatalf("sent messages = %#v, want only the valid recovered assignment", stream.sent)
	}
	if len(quarantined) != 1 || len(delivered) != 1 ||
		quarantined[first.Task.Record.ID] != first.Assignment.Record.AssignmentID ||
		delivered[second.Task.Record.ID] != second.Assignment.Record.AssignmentID {
		t.Fatalf("delivered/quarantined = %#v/%#v", delivered, quarantined)
	}
	assignment := stream.sent[0].GetTaskAssignment()
	if assignment.GetAssignmentId() != second.Assignment.Record.AssignmentID ||
		hex.EncodeToString(assignment.GetPlan().GetPlanHash()) != second.Task.Record.PlanHash {
		t.Fatalf("delivered assignment identity = %v", assignment)
	}
}

func assignmentQuarantineFixture(at time.Time, agentID string, entropy int64) etcd.TaskAssignment {
	taskID := ids.NewAt(ids.KindTask, at, entropy)
	assignmentID := ids.NewAt(ids.KindAssignment, at, entropy)
	task := etcd.TaskRecord{
		ID: taskID, OperationID: ids.NewAt(ids.KindOperation, at, entropy),
		PlanID: ids.NewAt(ids.KindPlan, at, entropy), PlanHash: strings.Repeat("0", 64),
		Type: testtaskjournal.TaskDeploy, Target: ids.NewAt(ids.KindService, at, entropy),
		Steps: []testtaskjournal.TaskStepRecord{
			{Kind: testtaskjournal.TaskStepOperation, ID: ids.NewAt(ids.KindStep, at, entropy)},
		},
		TimeoutSeconds: 60, RenderGeneration: 7, Status: testtaskjournal.TaskStatusRunning,
		CreatedAt: at, UpdatedAt: at, StartedAt: &at,
	}
	return etcd.TaskAssignment{
		Task: testkeyvalue.Versioned[etcd.TaskRecord]{Record: task},
		Assignment: testkeyvalue.Versioned[testtaskassignments.TaskAssignmentRecord]{
			Record: testtaskassignments.TaskAssignmentRecord{
				AssignmentID: assignmentID, TaskID: taskID, Executor: testtaskjournal.TaskExecutorAgent,
				AgentID: agentID, AgentGeneration: 7, ClaimedTaskRevision: 1,
				AssignedAt: at, Deadline: at.Add(time.Minute), RecoveryDeadline: at.Add(2 * time.Minute),
				ExecutionEpoch: 1, ExecutionMode: testtaskassignments.TaskExecutionModeForward,
			},
		},
	}
}
