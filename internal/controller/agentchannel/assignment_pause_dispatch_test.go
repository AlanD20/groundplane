package agentchannel

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// QA: HOST-07, TASK-10; fake-store dispatch across a controlled pause, not durable execution.
// Rationale: a valid assignment delayed by update admission is not a corrupt
// plan. Resuming must deliver it, rather than quarantine it until Task timeout.
func TestDispatchReadyPauseDuringPreparationRemainsDispatchable(t *testing.T) {
	registry := NewRegistry()
	session := openAdmissionSession(t, registry, 7)
	at := testTime()
	claim := assignmentQuarantineFixture(at, testAgentID, 904)
	plan := testExecutionPlan(t, claim.Task.Record)
	claim.Task.Record.PlanHash = hex.EncodeToString(plan.PlanHash)
	store := &fakeTaskStore{assignments: []etcd.TaskAssignment{claim}}
	resolver := &blockingPlanResolver{
		plan: plan, entered: make(chan struct{}), release: make(chan struct{}),
	}
	server := New(nil, registry, store, resolver)
	server.now = func() time.Time { return at }
	stream := &scriptedStream{ctx: context.Background()}
	delivered := make(map[string]string)
	quarantined := make(map[string]string)
	authorization := Authorization{Generation: 7, Config: &agentpb.AgentConfig{MaxConcurrentTasks: 1}}
	dispatchResult := make(chan error, 1)
	go func() {
		dispatchResult <- server.dispatchReady(stream, session, testAgentID, authorization, 1, delivered, quarantined)
	}()
	<-resolver.entered
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pauseResult := make(chan admissionPauseResult, 1)
	go func() {
		resume, err := registry.PauseAssignments(ctx, testAgentID, 7)
		pauseResult <- admissionPauseResult{resume: resume, err: err}
	}()
	waitForPausedAdmission(t, session)
	close(resolver.release)
	if err := <-dispatchResult; err != nil {
		t.Fatal(err)
	}
	paused := <-pauseResult
	if paused.err != nil {
		t.Fatal(paused.err)
	}
	defer paused.resume()
	if len(stream.sent) != 0 || len(quarantined) != 0 || len(delivered) != 0 {
		t.Fatalf("paused assignment: sent=%d, quarantined=%v, delivered=%v", len(stream.sent), quarantined, delivered)
	}
	paused.resume()
	server.plans = &fakePlanResolver{plan: plan}
	if err := server.dispatchReady(stream, session, testAgentID, authorization, 1, delivered, quarantined); err != nil {
		t.Fatal(err)
	}
	if len(stream.sent) != 1 || len(delivered) != 1 || len(quarantined) != 0 ||
		delivered[claim.Task.Record.ID] != claim.Assignment.Record.AssignmentID {
		t.Fatalf("resumed assignment was not delivered: sent=%d, delivered=%v", len(stream.sent), delivered)
	}
	assignment := stream.sent[0].GetTaskAssignment()
	if assignment.GetTaskId() != claim.Task.Record.ID ||
		assignment.GetAssignmentId() != claim.Assignment.Record.AssignmentID ||
		hex.EncodeToString(assignment.GetPlan().GetPlanHash()) != claim.Task.Record.PlanHash {
		t.Fatalf("resumed assignment identity = %v", assignment)
	}
}
