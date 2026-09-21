package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testscriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	testscriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	testscriptsourcereference "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
)

// Rationale: both deadline collectors must fence an unstarted execution and
// retain its exact sources for Retry, rather than leaving it assigned forever
// because the generic timeout result claims uncertain Compose effects.
func TestManualScriptBeforeStartTimeoutRetainsRetryAuthority(t *testing.T) {
	for _, collector := range []string{"deadline", "agent_generation"} {
		t.Run(collector, func(t *testing.T) {
			ctx := context.Background()
			store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
			deadline := assignment.Assignment.Record.Deadline
			collect := func(at time.Time) (int, error) {
				if collector == "deadline" {
					return tasks.ExpireTimedOutTasks(ctx, at)
				}
				return tasks.TimeoutAgentAssignments(ctx, assignment.Assignment.Record.AgentID, 1, 24, at)
			}
			if count, err := collect(deadline.Add(-time.Nanosecond)); count != 0 || err != nil {
				t.Fatalf("early timeout = %d, %v", count, err)
			}
			if count, err := collect(deadline); count != 1 || err != nil {
				t.Fatalf("before-start timeout = %d, %v", count, err)
			}
			terminal, err := tasks.GetTask(ctx, assignment.Task.Record.ID)
			if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusTimedOut ||
				terminal.Record.RetainUntil == nil {
				t.Fatalf("timeout terminal = %s, %v", terminal.Record.Status, err)
			}
			rootValue := store.valueAt(
				testscriptsourceevidence.ScriptSourceRootKey(execution.OperationID),
				store.revision,
			)
			root, err := testscriptsourceevidence.DecodeScriptOperationSourceRoot(rootValue.Value)
			if err != nil || root.Phase != testscriptsourceevidence.ScriptOperationSourceActive ||
				root.RetryDisposition != testscriptsourcereference.RetryDispositionAvailable || root.RetryExpiresAt == nil ||
				!root.RetryExpiresAt.Equal(*terminal.Record.RetainUntil) || rootValue.ModRevision != terminal.Revision {
				t.Fatalf("timeout lost atomic retry authority: %v", err)
			}
			retained, err := scripts.GetScriptExecution(ctx, execution.ID)
			if err != nil || retained.Record.State != testscriptexecutions.ScriptExecutionNotStarted ||
				!retained.Record.ActiveReference || retained.Record.AssignmentID != "" {
				t.Fatalf("timeout changed sealed execution: %v", err)
			}
			revision := store.revision
			if count, err := collect(deadline.Add(time.Second)); count != 0 || err != nil ||
				store.revision != revision {
				t.Fatalf("timeout replay wrote state: %d, %v", count, err)
			}
			retryAt := deadline.Add(2 * time.Second)
			retryID := ids.NewAt(ids.KindTask, retryAt, 120)
			result, err := tasks.RetryTask(
				ctx,
				terminal.Record.ID,
				retryID,
				testtaskjournal.TaskActorOperator,
				pendingRetryMarker(terminal.Record, retryID, retryAt, "manual-timeout-retry"),
			)
			if err != nil || result.kind != idempotencyTransactionApplied {
				t.Fatalf("timeout retry = %v", err)
			}
		})
	}
}

// Rationale: a timeout classification is only a snapshot. Start authorization
// winning before terminal commit must preserve assignment and references until
// real cleanup, both in that pass and in subsequent deadline scans.
func TestManualScriptTimeoutCannotRacePastStart(t *testing.T) {
	for _, collector := range []string{"deadline", "agent_generation"} {
		t.Run(collector, func(t *testing.T) {
			ctx := context.Background()
			store, scripts, _, assignment, execution := claimedManualScriptFixture(t)
			input := scriptCheckpointTestInput(execution, assignment.Task.Record.CreatedAt.Add(2*time.Second))
			input.AgentID = assignment.Assignment.Record.AgentID
			input.AssignmentID = assignment.Assignment.Record.AssignmentID
			input.ExpectedState, input.State = testscriptexecutions.ScriptExecutionNotStarted, testscriptexecutions.ScriptExecutionStartAuthorized
			input.Evidence = testscriptexecutions.ScriptCheckpointEvidence{
				Kind: testscriptexecutions.ScriptCheckpointEvidenceStartAuthorized, StartAuthorized: &testscriptexecutions.ScriptStartAuthorizedEvidence{},
			}
			input.PayloadSHA256 = scriptSourceReferenceDigest(
				string(testscriptexecutions.ScriptExecutionStartAuthorized),
			)
			racing := &manualAssignedAbortRaceStore{memoryHierarchyStore: store, scripts: scripts, input: input,
				rootKey: testscriptsourceevidence.ScriptSourceRootKey(execution.OperationID)}
			tasks, err := newTaskRepository(racing)
			if err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				at := assignment.Assignment.Record.Deadline.Add(time.Duration(pass) * time.Second)
				var count int
				if collector == "deadline" {
					count, err = tasks.ExpireTimedOutTasks(ctx, at)
				} else {
					count, err = tasks.TimeoutAgentAssignments(ctx, assignment.Assignment.Record.AgentID, 1, 24, at)
				}
				if err != nil || count != 0 || !racing.raced {
					t.Fatalf("timeout/start race pass %d = %d, raced %t, %v", pass, count, racing.raced, err)
				}
			}
			current, err := tasks.GetTaskAssignment(ctx, assignment.Task.Record.ID)
			if err != nil || current.Task.Record.Status != testtaskjournal.TaskStatusRunning ||
				current.Assignment.Record.AssignmentID != assignment.Assignment.Record.AssignmentID {
				t.Fatalf("timeout discarded cleanup authority: %v", err)
			}
			retained, err := scripts.GetScriptExecution(ctx, execution.ID)
			if err != nil || retained.Record.State != testscriptexecutions.ScriptExecutionStartAuthorized ||
				!retained.Record.ActiveReference {
				t.Fatalf("timeout changed winning start checkpoint: %v", err)
			}
		})
	}
}
