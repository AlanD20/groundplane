package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProveRecoveryHookTerminalReconnect interrupts an actual Worker's final
// recovery report after source closure and resumes through a new repository.
func (fixture *ExecutedArtifactFixture) ProveRecoveryHookTerminalReconnect(
	t *testing.T, agentID string, recovery TaskAssignment, result TaskResultRecord, at time.Time,
) Versioned[TaskRecord] {
	t.Helper()
	ctx := context.Background()
	taskID := recovery.Task.Record.ID
	before, err := fixture.Tasks.GetTaskAssignment(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	failedStore := &scriptSourceReferenceFailureStore{
		memoryHierarchyStore: fixture.store.memoryHierarchyStore,
		failAt:               2,
	}
	interrupted, err := newTaskRepository(failedStore)
	if err != nil {
		t.Fatal(err)
	}
	interrupted.blueprintTerminalStore = failedStore
	if _, err := interrupted.AcknowledgeTask(ctx, agentID, 1, taskID, recovery.Assignment.Record.AssignmentID,
		TaskStatusCompleted, result, at); !isKind(err, errs.KindInternal) {
		t.Fatalf("recovery source release was not interrupted: %v", err)
	}
	after, err := fixture.Tasks.GetTaskAssignment(ctx, taskID)
	if err != nil || after.Task.Revision != before.Task.Revision ||
		after.Assignment.Revision != before.Assignment.Revision {
		t.Fatalf("interrupted recovery changed execution authority: %v", err)
	}
	report, value, err := fixture.Tasks.readBlueprintClosingReport(ctx, after)
	if err != nil || value == nil || report.Status != TaskStatusCompleted ||
		!report.matches(TaskStatusCompleted, result) ||
		report.Result.ReleaseRecoveryRecordSHA256 == "" ||
		report.Result.ExecutionEpoch != recovery.Assignment.Record.ExecutionEpoch {
		t.Fatalf("original recovery ACK was replaced by its normalized primary failure: %v", err)
	}
	restarted, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	restarted.blueprintTerminalStore = fixture.store
	completed, err := restarted.ReconnectAgentAssignment(ctx, recovery)
	if err != nil || completed.Task.Record.Status != TaskStatusFailed ||
		completed.Assignment.Revision != before.Assignment.Revision || completed.Task.Record.FinishedAt == nil ||
		!completed.Task.Record.FinishedAt.Equal(report.ObservedAt) {
		t.Fatalf("recovery closing report did not resume its original failure: %v", err)
	}
	read, err := fixture.store.GetMany(ctx, GetManyRequest{Keys: []string{
		blueprintClosingReportKey(taskID), scriptSourceRootKey(recovery.Task.Record.OperationID),
	}})
	if err != nil || read.Values[0] != nil || read.Values[1] != nil {
		t.Fatalf("recovery continuation keys survived terminal completion: %v", err)
	}
	return completed.Task
}
