package app

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ProveRecoveryHookTerminalReconnect interrupts an actual Worker's final
// recovery report after source closure and resumes through a new repository.
func (fixture *ExecutedArtifactFixture) ProveRecoveryHookTerminalReconnect(
	t *testing.T, agentID string, recovery etcd.TaskAssignment, result testtaskjournal.TaskResultRecord, at time.Time,
) testkeyvalue.Versioned[etcd.TaskRecord] {
	t.Helper()
	ctx := context.Background()
	taskID := recovery.Task.Record.ID
	before, err := fixture.Tasks.GetTaskAssignment(ctx, taskID)
	if err != nil {
		t.Fatal(err)
	}
	failedStore := &scriptSourceReferenceFailureStore{
		releasePlanningTestStore: fixture.store,
		failAt:                   2,
	}
	interrupted, err := etcd.NewTaskRepository(failedStore)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := interrupted.AcknowledgeTask(ctx, agentID, 1, taskID, recovery.Assignment.Record.AssignmentID, testtaskjournal.TaskStatusCompleted, result, at); !isKind(
		err,
		errs.KindInternal,
	) {
		t.Fatalf("recovery source release was not interrupted: %v", err)
	}
	after, err := fixture.Tasks.GetTaskAssignment(ctx, taskID)
	if err != nil || after.Task.Revision != before.Task.Revision ||
		after.Assignment.Revision != before.Assignment.Revision {
		t.Fatalf("interrupted recovery changed execution authority: %v", err)
	}
	report, value := readIntegrationClosingReport(t, fixture.store, taskID)
	if value == nil || report.Status != testtaskjournal.TaskStatusCompleted ||
		!testtaskjournal.TaskResultsEqual(report.Result, result) ||
		report.Result.ReleaseRecoveryRecordSHA256 == "" ||
		report.Result.ExecutionEpoch != recovery.Assignment.Record.ExecutionEpoch {
		t.Fatalf("original recovery ACK was replaced by its normalized primary failure: %v", err)
	}
	restarted, err := etcd.NewTaskRepository(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	completed, err := restarted.ReconnectAgentAssignment(ctx, recovery)
	if err != nil || completed.Task.Record.Status != testtaskjournal.TaskStatusFailed ||
		completed.Assignment.Revision != before.Assignment.Revision || completed.Task.Record.FinishedAt == nil ||
		!completed.Task.Record.FinishedAt.Equal(report.ObservedAt) {
		t.Fatalf("recovery closing report did not resume its original failure: %v", err)
	}
	read, err := fixture.store.GetMany(ctx, testkeyvalue.GetManyRequest{Keys: []string{
		integrationBlueprintClosingReportKey(
			taskID,
		), testscriptsourceevidence.ScriptSourceRootKey(recovery.Task.Record.OperationID),
	}})
	if err != nil || read.Values[0] != nil || read.Values[1] != nil {
		t.Fatalf("recovery continuation keys survived terminal completion: %v", err)
	}
	return completed.Task
}
