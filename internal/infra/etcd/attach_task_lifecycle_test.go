package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: an Agent must never observe a running Attach Task while the durable Attach remains pending,
// and the matching terminal acknowledgement must commit Task and Attach success in the same revision.
func TestAttachTaskClaimAndAcknowledgementAdvanceProvisioningAtomically(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	attaches, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 81, "lifecycle-db", nil)
	createTestAttach(t, ctx, attaches, scope, record, &facts)
	agentID := ids.NewAt(ids.KindAgent, record.CreatedAt, 801)

	claim, found, err := tasks.ClaimNextTask(ctx, agentID, 1, record.CreatedAt.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() = %#v, %v, %v", claim, found, err)
	}
	provisioning, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || provisioning.Record.Status != core.AttachProvisioning ||
		provisioning.Revision != claim.Task.Revision {
		t.Fatalf("provisioning Attach = %#v, %v", provisioning, err)
	}

	terminalAt := record.CreatedAt.Add(2 * time.Second)
	terminal, err := tasks.AcknowledgeTask(
		ctx, agentID, 1, record.TaskID, TaskStatusCompleted, completedComposeTaskResult(), terminalAt,
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || ready.Record.Status != core.AttachReady || ready.Revision != terminal.Revision {
		t.Fatalf("ready Attach = %#v, %v", ready, err)
	}
	replay, err := tasks.AcknowledgeTask(
		ctx, agentID, 1, record.TaskID, TaskStatusCompleted, completedComposeTaskResult(), terminalAt.Add(time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(replay) = %#v, %v", replay, err)
	}
}

// Rationale: retry publication must replace the failed provisioning Task ownership atomically so the
// sealed retry plan can reveal the same private Attach identity without an orphaned intermediate state.
func TestAttachTaskRetryReplacesProvisioningTaskAtomically(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	attaches, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 82, "retry-db", nil)
	createTestAttach(t, ctx, attaches, scope, record, &facts)
	agentID := ids.NewAt(ids.KindAgent, record.CreatedAt, 802)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 2, record.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	terminalAt := record.CreatedAt.Add(2 * time.Second)
	result := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone, ReconciliationRequired: true,
	}
	terminal, err := tasks.AcknowledgeTask(
		ctx, agentID, 2, record.TaskID, TaskStatusTimedOut, result, terminalAt,
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(timeout) error = %v", err)
	}
	failed, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || failed.Record.Status != core.AttachFailed || failed.Revision != terminal.Revision {
		t.Fatalf("failed Attach = %#v, %v", failed, err)
	}

	retryAt := terminalAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 803)
	marker := pendingRetryMarker(terminal.Record, retryID, retryAt, "attach-retry-key-0001")
	if _, err := tasks.RetryTask(ctx, record.TaskID, retryID, marker); err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	retryTask, err := tasks.GetTask(ctx, retryID)
	if err != nil {
		t.Fatalf("GetTask(retry) error = %v", err)
	}
	pending, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || pending.Record.Status != core.AttachPending || pending.Record.TaskID != retryID ||
		pending.Revision != retryTask.Revision {
		t.Fatalf("retrying Attach = %#v, %v", pending, err)
	}
	claim, found, err := tasks.ClaimNextTask(ctx, agentID, 2, retryAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextTask(retry) = %#v, %v, %v", claim, found, err)
	}
	provisioning, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || provisioning.Record.Status != core.AttachProvisioning ||
		provisioning.Revision != claim.Task.Revision {
		t.Fatalf("retry provisioning Attach = %#v, %v", provisioning, err)
	}
}

// Rationale: detach retries must preserve the detach operation and bind the replacement Task before a
// terminal success atomically removes the Attach desired state.
func TestDetachTaskFailureRetryAndSuccessAdvanceAttachAtomically(t *testing.T) {
	ctx := context.Background()
	store := newAttachTestStore()
	scope := seedAttachScope(t, ctx, store)
	attaches, err := NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	record, facts := testPendingAttach(t, scope, 83, "detach-db", nil)
	createTestAttach(t, ctx, attaches, scope, record, &facts)
	agentID := ids.NewAt(ids.KindAgent, record.CreatedAt, 804)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 3, record.CreatedAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask(provision) found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		3,
		record.TaskID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		record.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(provision) error = %v", err)
	}
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(ready) error = %v", err)
	}

	detachAt := record.CreatedAt.Add(3 * time.Second)
	detachTask := publishTestDetach(t, ctx, attaches, scope, ready, detachAt)
	detachID := detachTask.ID
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 3, detachAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask(detach) found/error = %v/%v", found, err)
	}
	detachFailureAt := detachAt.Add(2 * time.Second)
	failedResult := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone, ReconciliationRequired: true,
	}
	failedTask, err := tasks.AcknowledgeTask(
		ctx, agentID, 3, detachID, TaskStatusTimedOut, failedResult, detachFailureAt,
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(detach timeout) error = %v", err)
	}
	failed, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || failed.Record.Status != core.AttachFailed || failed.Record.Operation != AttachOperationDetach ||
		failed.Revision != failedTask.Revision {
		t.Fatalf("failed detach Attach = %#v, %v", failed, err)
	}

	retryAt := detachFailureAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 806)
	marker := pendingRetryMarker(failedTask.Record, retryID, retryAt, "detach-retry-key-0001")
	if _, err := tasks.RetryTask(ctx, detachID, retryID, marker); err != nil {
		t.Fatalf("RetryTask(detach) error = %v", err)
	}
	retrying, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || retrying.Record.Status != core.AttachDetaching || retrying.Record.TaskID != retryID {
		t.Fatalf("retrying detach Attach = %#v, %v", retrying, err)
	}
	claim, found, err := tasks.ClaimNextTask(ctx, agentID, 3, retryAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextTask(detach retry) = %#v, %v, %v", claim, found, err)
	}
	terminal, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		3,
		retryID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		retryAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(detach retry) error = %v", err)
	}
	_, err = attaches.GetAttach(ctx, record.ID)
	kind, _ := errs.KindOf(err)
	if kind != errs.KindAttachNotFound {
		t.Fatalf("GetAttach(detached) error = %v, want Attach not found", err)
	}
	for _, key := range []string{
		attachNameKey(record.EnvironmentID, record.Name),
		attachOwnerKey(record.EnvironmentID, record.ID),
		attachServiceKey(record.ServiceIDs[0], record.ID),
		attachBackingServiceKey(record.BackingServiceID, record.ID),
		attachBackingProjectKey(record.BackingProjectID, record.ID),
		attachFactsKey(record.ID),
	} {
		result, getErr := store.Get(ctx, key)
		if getErr != nil || result == nil || result.Entry != nil || result.ReadRevision != terminal.Revision {
			t.Fatalf("detached Attach companion %s = %#v, %v", key, result, getErr)
		}
	}
	replay, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		3,
		retryID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		retryAt.Add(3*time.Second),
	)
	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(detach replay) = %#v, %v", replay, err)
	}
}
