package etcd

import (
	"context"
	"testing"
	"time"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// SVC05: a Controller bookkeeping failure after receiving completion must not
// turn reconnect into host rollback. Late events and epoch changes lose the CAS.
func TestReleaseCompletionReportPrecedesTerminalWrites(t *testing.T) {
	f := newOrdinaryReleaseClaimFixtureForTarget(t, true)
	ctx := context.Background()
	result := testtaskjournal.TaskResultRecord{Kind: testtaskjournal.TaskResultCompose,
		Diagnostic: testtaskjournal.TaskResultDiagnosticNone, ExecutionEpoch: f.claim.Assignment.Record.ExecutionEpoch}
	_, err := f.repository.AcknowledgeTask(ctx, f.agentID, 1, f.claim.Task.Record.ID,
		f.claim.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, f.now.Add(time.Minute))
	// This fixture intentionally has no operation head: failure is after report acceptance.
	if err == nil {
		t.Fatal("missing operation head did not interrupt terminal publication")
	}
	current, err := f.repository.GetTaskAssignment(ctx, f.claim.Task.Record.ID)
	if err != nil {
		t.Fatal(err)
	}
	report, value, err := f.repository.readTaskClosingReport(ctx, current)
	if err != nil || value == nil || !report.matches(testtaskjournal.TaskStatusCompleted, result) {
		t.Fatalf("received completion was lost: %v", err)
	}
	if err := f.repository.incrementAssignmentEpoch(ctx, f.claim, nil); err == nil {
		t.Fatal("reconnect raced past accepted completion")
	}
	if _, err := f.repository.ReconnectAgentAssignment(ctx, current); err == nil {
		t.Fatal("missing operation head unexpectedly settled")
	}
	read, err := f.repository.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		testtaskassignments.ReleaseRecoveryKey(f.claim.Task.Record.ID), releaseClosingReportKey(f.claim.Task.Record.ID),
	}})
	if err != nil || read.Values[0] != nil || read.Values[1] == nil {
		t.Fatalf("Controller-only resumption switched to host recovery: %v", err)
	}
	_, err = f.repository.AppendTaskEvent(ctx, testtaskjournal.TaskEventInput{
		Identity: testtaskjournal.TaskEventIdentity{AssignmentID: current.Assignment.Record.AssignmentID,
			AgentID: f.agentID, AgentGeneration: 1, TaskID: current.Task.Record.ID,
			StepID: f.forwardStepID, Attempt: current.Assignment.Record.ExecutionEpoch, Ordinal: 1},
		State: testtaskjournal.TaskEventStateRunning, Payload: []byte(`{"message":"late"}`),
	}, f.now.Add(2*time.Minute))
	if err == nil {
		t.Fatal("late effect was accepted after completion report")
	}
}
