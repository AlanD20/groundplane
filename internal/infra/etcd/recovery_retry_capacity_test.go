package etcd

import (
	"bytes"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/localagents"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func TestStoppedRecoveryKeepsOwnershipWithoutConsumingUnrelatedCapacity(t *testing.T) {
	// TASK-16: exhausted recovery may retain its exact source/assignment but
	// must not prevent another operation from using the sole execution slot.
	fixture, assignment, _, _, _ := recoveryProofFixture(t, true)
	encoded, err := taskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		t.Fatal(err)
	}
	control := taskassignments.RecoveryRetryControl{TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID,
		ExecutionEpoch: assignment.ExecutionEpoch, Failures: 1, Stopped: true, StopReason: "non_transient_failure",
		Failure: taskprogress.Failure{
			Code: "internal_execution_error",
		}, UpdatedAt: fixture.now, NextAttemptAt: fixture.now}
	stopped, err := taskassignments.EncodeRecoveryRetryControl(control)
	if err != nil {
		t.Fatal(err)
	}
	claimKey := taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID)
	_, err = fixture.repository.store.Transact(t.Context(), nil, []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: claimKey, Value: encoded},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskAssignmentIndexKey(assignment.TaskID), Value: encoded},
		{Type: keyvalue.MutationPut, Key: taskassignments.RecoveryRetryControlKey(assignment.TaskID), Value: stopped},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := fixture.repository.store.Get(t.Context(), claimKey)
	if err != nil {
		t.Fatal(err)
	}
	other := validTaskRecord(fixture.now.Add(time.Minute))
	createLifecycleTask(t, fixture.repository, other)
	claimed, found, err := fixture.repository.ClaimNextTask(
		t.Context(),
		assignment.AgentID,
		assignment.AgentGeneration,
		other.CreatedAt.Add(time.Second),
		1,
	)
	if err != nil || !found || claimed.Task.Record.ID != other.ID {
		t.Fatalf("unrelated work blocked: %v %v", found, err)
	}
	after, err := fixture.repository.store.Get(t.Context(), claimKey)
	if err != nil || after.Entry == nil || before.Entry.ModRevision != after.Entry.ModRevision ||
		!bytes.Equal(before.Entry.Value, after.Entry.Value) {
		t.Fatal("freeing capacity released unresolved ownership")
	}
}

func TestExplicitRecoveryRetryPreservesCapturedInputsAndReplaysAfterRestart(t *testing.T) {
	// TASK-16: operator Retry must grant exactly one captured restoration
	// attempt. A lost/replayed request cannot revive forward work or grant two.
	fixture, assignment, _, _, _ := recoveryProofFixture(t, true)
	encoded, err := taskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		t.Fatal(err)
	}
	control, err := taskassignments.EncodeRecoveryRetryControl(taskassignments.RecoveryRetryControl{
		TaskID: assignment.TaskID, AssignmentID: assignment.AssignmentID, ExecutionEpoch: assignment.ExecutionEpoch,
		Failures: 1, Stopped: true, StopReason: "non_transient_failure",
		Failure: taskprogress.Failure{
			Code: "internal_execution_error",
		}, UpdatedAt: fixture.now, NextAttemptAt: fixture.now,
	})
	if err != nil {
		t.Fatal(err)
	}
	configuration, err := localagents.EncodeLocalAgentConfig(localagents.LocalAgentRecord{
		ID: assignment.AgentID, Generation: assignment.AgentGeneration,
		Config: localagents.LocalAgentConfig{PullIntervalSeconds: 2, MaxConcurrentTasks: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = fixture.repository.store.Transact(t.Context(), nil, []keyvalue.Mutation{
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID),
			Value: encoded,
		},
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskAssignmentIndexKey(assignment.TaskID), Value: encoded},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskTimeoutIndexKey(assignment.TaskID, assignment.RecoveryDeadline),
			Value: encoded,
		},
		{Type: keyvalue.MutationPut, Key: taskassignments.RecoveryRetryControlKey(assignment.TaskID), Value: control},
		{Type: keyvalue.MutationPut, Key: localagents.LocalAgentConfigKey(assignment.AgentID), Value: configuration},
	})
	if err != nil {
		t.Fatal(err)
	}
	before, err := fixture.repository.store.Get(t.Context(), taskjournal.TaskStorageKey(assignment.TaskID))
	if err != nil {
		t.Fatal(err)
	}
	marker := testDirectMarker()
	marker.Locator.Method, marker.Locator.Route = http.MethodPost, "/tasks/{id}/retry"
	marker.Response = idempotency.IdempotencyResponse{Status: http.StatusAccepted, ContentKind: "application/json",
		Body: []byte(`{"task_id":"` + assignment.TaskID + `"}`)}
	marker.CreatedAt, marker.UpdatedAt, marker.TerminalAt = fixture.now, fixture.now, fixture.now
	marker.RetainUntil = fixture.now.Add(idempotency.MarkerRetention)
	result, err := fixture.repository.RetryStoppedRecovery(t.Context(), assignment.TaskID, marker)
	if err != nil {
		t.Fatal(err)
	}
	if outcome, _, conflict, err := result.Classify(); err != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("Retry did not apply atomically: %v %v %v", outcome, conflict, err)
	}
	current, err := fixture.repository.GetTaskAssignment(t.Context(), assignment.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Assignment.Record.ExecutionEpoch != assignment.ExecutionEpoch+1 ||
		current.Assignment.Record.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
		current.Assignment.Record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 {
		t.Fatal("Retry changed captured restoration authority")
	}
	after, err := fixture.repository.store.Get(t.Context(), taskjournal.TaskStorageKey(assignment.TaskID))
	if err != nil || !bytes.Equal(before.Entry.Value, after.Entry.Value) {
		t.Fatal("Retry changed captured Task")
	}
	restarted, err := newTaskRepository(fixture.repository.store)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := restarted.RetryStoppedRecovery(t.Context(), assignment.TaskID, marker); err != nil {
		t.Fatal(err)
	}
	replayed, err := restarted.GetTaskAssignment(t.Context(), assignment.TaskID)
	if err != nil || replayed.Assignment.Revision != current.Assignment.Revision {
		t.Fatal("request replay authorized a second execution")
	}
}
