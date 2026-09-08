package etcd

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: the retention collector must close an unused retry opportunity
// and release its inputs before deleting the Task that authorizes that cleanup.
func TestManualScriptRetentionPruneReleasesRetrySources(t *testing.T) {
	ctx := context.Background()
	store, scripts, tasks, assignment, execution := claimedManualScriptFixture(t)
	terminal, err := tasks.AcknowledgeTask(ctx, assignment.Assignment.Record.AgentID, 1,
		assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, TaskStatusFailed,
		manualScriptTerminalResult(assignment, TaskStatusFailed), assignment.Task.Record.CreatedAt.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	if count, err := idempotency.PruneExpired(ctx, *terminal.Record.RetainUntil); err != nil || count != 1 {
		t.Fatalf("expire Task marker = %d, %v", count, err)
	}
	count, err := tasks.PruneExpiredTasks(ctx, *terminal.Record.RetainUntil)
	if err != nil || count != 1 {
		t.Fatalf("prune expired manual Task = %d, %v", count, err)
	}
	if store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision) != nil {
		t.Fatal("retention pruned Task authority before draining retry sources")
	}
	script, err := scripts.GetScript(ctx, execution.ScriptID)
	if err != nil || script.Record.ActiveReferences != 0 {
		t.Fatalf("expired retry retained Script references: %v", err)
	}
	closed, err := scripts.GetScriptExecution(ctx, execution.ID)
	if err != nil || closed.Record.State != ScriptExecutionCleanupProven || closed.Record.ActiveReference ||
		closed.Record.Outcome == nil || closed.Record.Outcome.Reason != ScriptOutcomeExpiryBeforeStart {
		t.Fatalf("expired retry has no durable absence proof: %v", err)
	}
}

// Rationale: retry expiry is not normal completion. Interruption must retain
// the original terminal Task and retention bytes throughout bounded release.
func TestManualScriptExpiryInterruptionPreservesTerminalAuthority(t *testing.T) {
	for _, failAt := range []int{2, 3} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			ctx := context.Background()
			store, _, tasks, assignment, execution := claimedManualScriptFixture(t)
			terminal, err := tasks.AcknowledgeTask(
				ctx,
				assignment.Assignment.Record.AgentID,
				1,
				assignment.Task.Record.ID,
				assignment.Assignment.Record.AssignmentID,
				TaskStatusFailed,
				manualScriptTerminalResult(
					assignment,
					TaskStatusFailed,
				),
				assignment.Task.Record.CreatedAt.Add(20*time.Second),
			)
			if err != nil {
				t.Fatal(err)
			}
			idempotency, err := newIdempotencyRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := idempotency.PruneExpired(ctx, *terminal.Record.RetainUntil); err != nil {
				t.Fatal(err)
			}
			taskBefore := store.valueAt(taskKey(terminal.Record.ID), store.revision)
			indexBefore := store.valueAt(
				taskRetentionIndexKey(terminal.Record.ID, *terminal.Record.RetainUntil),
				store.revision,
			)
			interrupted, err := newTaskRepository(
				&scriptSourceReferenceFailureStore{memoryHierarchyStore: store, failAt: failAt},
			)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := interrupted.PruneExpiredTasks(ctx, *terminal.Record.RetainUntil); !isKind(
				err,
				errs.KindInternal,
			) {
				t.Fatalf("expiry interruption = %v", err)
			}
			for _, before := range []*KeyValue{taskBefore, indexBefore} {
				after := store.valueAt(before.Key, store.revision)
				if after == nil || after.ModRevision != before.ModRevision || !bytes.Equal(after.Value, before.Value) {
					t.Fatal("expiry rewrote original terminal Task or retention authority")
				}
			}
			rootValue := store.valueAt(scriptSourceRootKey(execution.OperationID), store.revision)
			root, err := decodeScriptOperationSourceRoot(rootValue.Value)
			if err != nil || root.ReleasePath != ScriptSourceReleaseRetryExpiry ||
				root.RetryDisposition != ScriptRetryDispositionExpired || root.RetryExpiresAt == nil ||
				!root.RetryExpiresAt.Equal(*terminal.Record.RetainUntil) {
				t.Fatalf("expiry lost its original release path/deadline: %v", err)
			}
			authority, err := newScriptSourceReferenceAuthority(store)
			if err != nil {
				t.Fatal(err)
			}
			beforeWrongPath := store.revision
			if _, _, err := authority.ReleaseNext(ctx, execution.OperationID,
				[]Condition{{Key: taskBefore.Key, ModRevision: taskBefore.ModRevision}}); err == nil || store.revision != beforeWrongPath {
				t.Fatalf("normal release accepted retry-expiry authority: %v", err)
			}
			if failAt == 3 {
				wrongFinal, err := authority.PrepareReleaseFinalization(ctx, execution.OperationID)
				wrongFinal.Clear()
				if err == nil {
					t.Fatal("normal finalization accepted a drained retry-expiry root")
				}
			}
			restarted, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			if count, err := restarted.PruneExpiredTasks(ctx, terminal.Record.RetainUntil.Add(time.Minute)); err != nil ||
				count != 1 {
				t.Fatalf("expiry restart = %d, %v", count, err)
			}
		})
	}
}

// Rationale: a live retry still owns its sources, but retaining its older
// attempt must not starve unrelated expired Task history behind it.
func TestManualScriptRetentionSkipsLiveRetryWithoutDroppingIndex(t *testing.T) {
	ctx := context.Background()
	store, _, tasks, assignment, _ := claimedManualScriptFixture(t)
	at := assignment.Task.Record.CreatedAt.Add(20 * time.Second)
	terminal, err := tasks.AcknowledgeTask(ctx, assignment.Assignment.Record.AgentID, 1,
		assignment.Task.Record.ID, assignment.Assignment.Record.AssignmentID, TaskStatusFailed,
		manualScriptTerminalResult(assignment, TaskStatusFailed), at)
	if err != nil {
		t.Fatal(err)
	}
	retryID := ids.NewAt(ids.KindTask, at.Add(time.Second), 120)
	if _, err := tasks.RetryTask(ctx, terminal.Record.ID, retryID, TaskActorOperator,
		pendingRetryMarker(terminal.Record, retryID, at.Add(time.Second), "live-manual-retry")); err != nil {
		t.Fatal(err)
	}
	unrelated := validTaskRecord(at.Add(time.Minute))
	unrelated.Type = TaskUpdate
	createLifecycleTask(t, tasks, unrelated)
	unrelatedTerminal, err := tasks.AbortPendingTask(ctx, unrelated.ID, unrelated.CreatedAt.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatal(err)
	}
	for {
		count, err := idempotency.PruneExpired(ctx, *unrelatedTerminal.Record.RetainUntil)
		if err != nil {
			t.Fatal(err)
		}
		if count == 0 {
			break
		}
	}
	if count, err := tasks.PruneExpiredTasks(ctx, *unrelatedTerminal.Record.RetainUntil); err != nil || count != 1 {
		t.Fatalf("live retry starved unrelated Task pruning: %d, %v", count, err)
	}
	for _, key := range []string{taskKey(terminal.Record.ID), taskRetentionIndexKey(terminal.Record.ID, *terminal.Record.RetainUntil)} {
		if store.valueAt(key, store.revision) == nil {
			t.Fatal("live retry source Task or retention index was discarded")
		}
	}
}
