package etcd

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: a failure before start authorization is retryable without
// discarding any sealed input or creating a second reference reservation.
func TestManualScriptBeforeStartFailureRetainsRetrySources(t *testing.T) {
	ctx := context.Background()
	store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
	terminalAt := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
	result := manualScriptTerminalResult(assignment, TaskStatusFailed)
	terminal, err := tasks.AcknowledgeTask(ctx, assignment.Assignment.Record.AgentID, 1,
		assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, TaskStatusFailed, result, terminalAt)
	if err != nil || terminal.Record.Status != TaskStatusFailed || terminal.Record.RetainUntil == nil {
		t.Fatalf("before-start failure did not terminalize with retention: %v", err)
	}
	rootValue := store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision)
	if rootValue == nil || rootValue.ModRevision != terminal.Revision {
		t.Fatal("retry availability did not share Task terminal publication")
	}
	root, err := decodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil || root.Phase != ScriptOperationSourceActive ||
		root.RetryDisposition != ScriptRetryDispositionAvailable {
		t.Fatalf("before-start failure closed its retry sources: %v", err)
	}
	if root.RetryExpiresAt == nil || !root.RetryExpiresAt.Equal(*terminal.Record.RetainUntil) {
		t.Fatal("retry expiry differs from the terminal Task retention deadline")
	}
	retained, err := scripts.GetScriptExecution(ctx, execution.ID)
	if err != nil || retained.Record.State != ScriptExecutionNotStarted || !retained.Record.ActiveReference ||
		retained.Record.StartAuthorized || retained.Record.AssignmentID != "" {
		t.Fatalf("before-start failure changed execution evidence: %v", err)
	}
	script, err := scripts.GetScript(ctx, execution.ScriptID)
	if err != nil || script.Record.ActiveReferences != 1 {
		t.Fatalf("retry-available execution lost its Script fence: %v", err)
	}
	revision := store.revision
	_, err = tasks.AcknowledgeTask(
		ctx,
		assignment.Assignment.Record.AgentID,
		1,
		assignment.Task.Record.ID,
		assignment.Assignment.Record.AssignmentID,
		TaskStatusFailed,
		result,
		terminalAt.Add(time.Second),
	)
	if err != nil || store.revision != revision {
		t.Fatalf("before-start terminal replay wrote state: %v", err)
	}
}

// Rationale: Retry transfers one operation's existing authority; it must not
// resnapshot inputs, reserve them twice, or leave execution owned by the old Task.
func TestManualScriptRetryTransfersSealedSources(t *testing.T) {
	ctx := context.Background()
	store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
	at := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
	terminal, err := tasks.AcknowledgeTask(ctx, assignment.Assignment.Record.AgentID, 1,
		assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, TaskStatusFailed,
		manualScriptTerminalResult(assignment, TaskStatusFailed), at)
	if err != nil {
		t.Fatal(err)
	}
	retryID := ids.NewAt(ids.KindTask, at.Add(time.Second), 100)
	marker := pendingRetryMarker(terminal.Record, retryID, at.Add(time.Second), "manual-script-retry")
	result, err := tasks.RetryTask(ctx, terminal.Record.ID, retryID, TaskActorOperator, marker)
	if err != nil || result.kind != idempotencyTransactionApplied {
		t.Fatalf("manual retry publication = %v, %v", result.kind, err)
	}
	retry, err := tasks.GetTask(ctx, retryID)
	if err != nil || retry.Record.OperationID != execution.OperationID || retry.Record.PlanHash != execution.PlanHash {
		t.Fatalf("retry changed sealed plan lineage: %v", err)
	}
	transferred, err := scripts.GetScriptExecution(ctx, execution.ID)
	if err != nil || transferred.Record.CurrentTaskID != retryID || transferred.Revision != retry.Revision ||
		transferred.Record.State != ScriptExecutionNotStarted || !transferred.Record.ActiveReference ||
		!bytes.Equal(
			transferred.Record.Plan,
			execution.Plan,
		) || !bytes.Equal(transferred.Record.Snapshot, execution.Snapshot) {
		t.Fatalf("retry did not atomically transfer the sealed execution: %v", err)
	}
	rootValue := store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision)
	if rootValue == nil || rootValue.ModRevision != retry.Revision {
		t.Fatal("retry did not transfer its source root atomically")
	}
	root, err := decodeScriptOperationSourceRoot(rootValue.Value)
	if err != nil || root.RetryDisposition != ScriptRetryDispositionTransferred || root.RetryExpiresAt != nil ||
		!manualScriptRootMatches(transferred.Record, root) {
		t.Fatalf("retry changed its immutable source set: %v", err)
	}
	script, err := scripts.GetScript(ctx, execution.ScriptID)
	if err != nil || script.Record.ActiveReferences != 1 {
		t.Fatalf("retry reserved references again: %v", err)
	}
	claimed, found, err := tasks.ClaimNextTask(ctx, assignment.Assignment.Record.AgentID, 1, at.Add(2*time.Second))
	if err != nil || !found || claimed.Task.Record.ID != retryID ||
		claimed.Assignment.Record.AssignmentID == assignment.Assignment.Record.AssignmentID {
		t.Fatalf("retry did not acquire fresh assignment authority: %t, %v", found, err)
	}
	activeValue := store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision)
	active, err := decodeScriptOperationSourceRoot(activeValue.Value)
	if err != nil || active.RetryDisposition != ScriptRetryDispositionUndecided ||
		activeValue.ModRevision != claimed.Task.Revision {
		t.Fatalf("retry claim did not activate its new Task authority: %v", err)
	}
}

// Rationale: expiry and Retry compete for the same root; the deadline belongs
// to the original terminal Task and cannot be extended by a late retry request.
func TestManualScriptRetryRejectsAtRetentionDeadline(t *testing.T) {
	ctx := context.Background()
	store, _, tasks, assignment, _ := claimedManualScriptFixture(t)
	terminal, err := tasks.AcknowledgeTask(ctx, assignment.Assignment.Record.AgentID, 1,
		assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, TaskStatusFailed,
		manualScriptTerminalResult(assignment, TaskStatusFailed), assignment.Task.Record.CreatedAt.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	revision := store.revision
	retryID := ids.NewAt(ids.KindTask, *terminal.Record.RetainUntil, 102)
	_, err = tasks.RetryTask(ctx, terminal.Record.ID, retryID, TaskActorOperator,
		pendingRetryMarker(terminal.Record, retryID, *terminal.Record.RetainUntil, "expired-manual-retry"))
	if !isKind(err, errs.KindScriptRetryUnsafe) || store.revision != revision {
		t.Fatalf("expired Script retry created authority: %v", err)
	}
}

// Rationale: cleanup after an arbitrary Script effect does not make rerunning
// that effect safe; even a failed Task cannot acquire a new retry assignment.
func TestManualScriptRetryRejectsAfterStart(t *testing.T) {
	ctx := context.Background()
	store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
	manualScriptCleanupCheckpoints(t, scripts, assignment, execution, TaskStatusFailed)
	at := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
	terminal, err := tasks.AcknowledgeTask(ctx, assignment.Assignment.Record.AgentID, 1,
		assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, TaskStatusFailed,
		manualScriptTerminalResult(assignment, TaskStatusFailed), at)
	if err != nil {
		t.Fatal(err)
	}
	revision := store.revision
	retryID := ids.NewAt(ids.KindTask, at.Add(time.Second), 101)
	_, err = tasks.RetryTask(ctx, terminal.Record.ID, retryID, TaskActorOperator,
		pendingRetryMarker(terminal.Record, retryID, at.Add(time.Second), "unsafe-manual-retry"))
	if !isKind(err, errs.KindScriptRetryUnsafe) || store.revision != revision {
		t.Fatalf("started Script was retryable: %v", err)
	}
}
