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
		ctx, agentID, 1, record.TaskID, taskAssignmentIDForTest(t, tasks,
			record.TaskID),
		TaskStatusCompleted, completedComposeTaskResult(), terminalAt)

	if err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, terminal.Revision)
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || ready.Record.Status != core.AttachReady || ready.Revision != terminal.Revision {
		t.Fatalf("ready Attach = %#v, %v", ready, err)
	}
	replay, err := tasks.AcknowledgeTask(
		ctx, agentID, 1, record.TaskID, taskAssignmentIDForTest(t, tasks,
			record.TaskID),
		TaskStatusCompleted, completedComposeTaskResult(), terminalAt.Add(time.Second))

	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(replay) = %#v, %v", replay, err)
	}
}

// Rationale: an Attach Task aborted before assignment must not leave its
// owned Attach pending behind a terminal Task journal.
func TestAttachTaskPendingAbortAtomicallyFailsProvisioning(t *testing.T) {
	t.Parallel()

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
	record, facts := testPendingAttach(t, scope, 87, "abort-db", nil)
	createTestAttach(t, ctx, attaches, scope, record, &facts)

	aborted, err := tasks.AbortPendingTask(ctx, record.TaskID, record.CreatedAt.Add(time.Second))
	if err != nil || aborted.Record.Status != TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v, %v", aborted.Record, err)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, aborted.Revision)
	failed, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || failed.Record.Status != core.AttachFailed || failed.Record.Operation != AttachOperationProvision ||
		failed.Record.TaskID != record.TaskID || failed.Revision != aborted.Revision {
		t.Fatalf("failed Attach/aborted Task = %#v/%#v, %v", failed, aborted, err)
	}
	replay, err := tasks.AbortPendingTask(ctx, record.TaskID, record.CreatedAt.Add(2*time.Second))
	if err != nil || replay.Revision != aborted.Revision {
		t.Fatalf("AbortPendingTask(replay) = %#v, %v", replay, err)
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
		ctx, agentID, 2, record.TaskID, taskAssignmentIDForTest(t, tasks,
			record.TaskID),
		TaskStatusTimedOut, result, terminalAt)

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
	retryResult, err := tasks.RetryTask(ctx, record.TaskID, retryID, TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	retryOutcome, _, retryConflict, classifyErr := retryResult.Classify()
	if classifyErr != nil || retryConflict != nil || retryOutcome != IdempotencyKnownApplied {
		t.Fatalf(
			"RetryTask() outcome/revision/conflict/error = %v/%d/%v/%v",
			retryOutcome,
			retryResult.revision,
			retryConflict,
			classifyErr,
		)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, retryResult.revision)
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
		record.TaskID, taskAssignmentIDForTest(t, tasks,

			record.TaskID),

		TaskStatusCompleted,
		completedComposeTaskResult(),
		record.CreatedAt.Add(2*time.Second)); err != nil {
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
		ctx, agentID, 3, detachID, taskAssignmentIDForTest(t, tasks,
			detachID),
		TaskStatusTimedOut, failedResult, detachFailureAt)

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
	retryResult, err := tasks.RetryTask(ctx, detachID, retryID, TaskActorOperator, marker)
	if err != nil {
		t.Fatalf("RetryTask(detach) error = %v", err)
	}
	retryOutcome, _, retryConflict, classifyErr := retryResult.Classify()
	if classifyErr != nil || retryConflict != nil || retryOutcome != IdempotencyKnownApplied {
		t.Fatalf(
			"RetryTask(detach) outcome/revision/conflict/error = %v/%d/%v/%v",
			retryOutcome,
			retryResult.revision,
			retryConflict,
			classifyErr,
		)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, retryResult.revision)
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
		retryID, taskAssignmentIDForTest(t, tasks,

			retryID),

		TaskStatusCompleted,
		completedComposeTaskResult(),
		retryAt.Add(2*time.Second))

	if err != nil {
		t.Fatalf("AcknowledgeTask(detach retry) error = %v", err)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, terminal.Revision)
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
		retryID, taskAssignmentIDForTest(t, tasks,

			retryID),

		TaskStatusCompleted,
		completedComposeTaskResult(),
		retryAt.Add(3*time.Second))

	if err != nil || replay.Revision != terminal.Revision {
		t.Fatalf("AcknowledgeTask(detach replay) = %#v, %v", replay, err)
	}
}

// Rationale: every Task-backed detach mutation must retain the exact active
// Backup exclusion compare through its final transaction, not only its read.
func TestAttachTaskBackedDetachRacesBackupSourceExclusion(t *testing.T) {
	t.Run("publication", func(t *testing.T) {
		ctx := context.Background()
		store := newAttachTestStore()
		scope := seedAttachScope(t, ctx, store)
		attaches, err := NewAttachRepository(store)
		if err != nil {
			t.Fatalf("NewAttachRepository() error = %v", err)
		}
		record, facts := testPendingAttach(t, scope, 91, "detach-publication-race", nil)
		ready := createTestAttach(t, ctx, attaches, scope, record, &facts)
		ready, err = advanceAttachReady(ctx, attaches, ready)
		if err != nil {
			t.Fatalf("advanceAttachReady() error = %v", err)
		}
		exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		raceStore := &attachBackupExclusionRaceStore{
			attachTestStore: store,
			exclusionKey:    exclusionKey,
			exclusionValue: testAttachBackupExclusionValue(
				t, scope.Environment.Record.ID, record.ID, 920,
			),
		}
		raceAttaches, err := NewAttachRepository(raceStore)
		if err != nil {
			t.Fatalf("NewAttachRepository(race) error = %v", err)
		}
		scope, renderInput, task, marker := attachDetachRaceEnvelope(
			t, ctx, raceAttaches, scope, ready, record.CreatedAt.Add(5*time.Minute),
		)
		result, err := raceAttaches.BeginAttachDetachWithTask(
			ctx, scope, ready, renderInput, task, marker,
		)
		if err != nil {
			t.Fatalf("BeginAttachDetachWithTask() error = %v", err)
		}
		outcome, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || conflict == nil || outcome == IdempotencyKnownApplied {
			t.Fatalf(
				"BeginAttachDetachWithTask().Classify() = %v/%v/%v, want conflict",
				outcome,
				conflict,
				classifyErr,
			)
		}
		stored, err := attaches.GetAttach(ctx, record.ID)
		if err != nil || stored.Record.Status != core.AttachReady {
			t.Fatalf("GetAttach(after publication race) = %#v/%v", stored, err)
		}
		taskEntry, err := store.Get(ctx, taskKey(task.ID))
		if err != nil || taskEntry.Entry != nil {
			t.Fatalf("detach race Task = %#v/%v", taskEntry, err)
		}
	})

	t.Run("retry", func(t *testing.T) {
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
		record, facts := testPendingAttach(t, scope, 92, "detach-retry-race", nil)
		createTestAttach(t, ctx, attaches, scope, record, &facts)
		agentID := ids.NewAt(ids.KindAgent, record.CreatedAt, 920)
		if _, found, err := tasks.ClaimNextTask(ctx, agentID, 12, record.CreatedAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextTask(provision) found/error = %v/%v", found, err)
		}
		if _, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			12,
			record.TaskID,
			taskAssignmentIDForTest(t, tasks, record.TaskID),
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
		if _, found, err := tasks.ClaimNextTask(ctx, agentID, 12, detachAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextTask(detach) found/error = %v/%v", found, err)
		}
		failedTask, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			12,
			detachTask.ID,
			taskAssignmentIDForTest(t, tasks, detachTask.ID),
			TaskStatusTimedOut,
			TaskResultRecord{
				Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticNone, ReconciliationRequired: true,
			},
			detachAt.Add(2*time.Second),
		)
		if err != nil {
			t.Fatalf("AcknowledgeTask(detach timeout) error = %v", err)
		}
		exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		for index, test := range []struct {
			name     string
			value    []byte
			wantKind errs.Kind
			key      string
		}{
			{
				name:     "valid",
				value:    testAttachBackupExclusionValue(t, scope.Environment.Record.ID, record.ID, 921),
				wantKind: errs.KindResourceInUse,
				key:      "detach-exclusion-retry-key-valid",
			},
			{
				name:     "malformed",
				value:    []byte("not-an-exclusion-record"),
				wantKind: errs.KindInternal,
				key:      "detach-exclusion-retry-key-malformed",
			},
			{
				name: "misbucketed",
				value: testAttachBackupExclusionValue(
					t,
					scope.Environment.Record.ID,
					ids.NewAt(ids.KindAttach, detachAt.Add(4*time.Second), 923),
					924,
				),
				wantKind: errs.KindInternal,
				key:      "detach-exclusion-retry-key-misbucketed",
			},
		} {
			t.Run(test.name, func(t *testing.T) {
				raceStore := &attachBackupExclusionRaceStore{
					attachTestStore: store,
					exclusionKey:    exclusionKey,
					exclusionValue:  test.value,
				}
				raceTasks, err := newTaskRepository(raceStore)
				if err != nil {
					t.Fatalf("newTaskRepository(race) error = %v", err)
				}
				retryAt := detachAt.Add(time.Duration(index+3) * time.Second)
				retryID := ids.NewAt(ids.KindTask, retryAt, int64(921+index))
				retryResult, err := raceTasks.RetryTask(
					ctx,
					detachTask.ID,
					retryID,
					TaskActorOperator,
					pendingRetryMarker(failedTask.Record, retryID, retryAt, test.key),
				)
				if err != nil {
					t.Fatalf("RetryTask(detach race) error = %v", err)
				}
				outcome, _, conflict, classifyErr := retryResult.Classify()
				if classifyErr != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, test.wantKind) {
					t.Fatalf(
						"RetryTask(detach race).Classify() = %v/%v/%v, want %v conflict",
						outcome,
						conflict,
						classifyErr,
						test.wantKind,
					)
				}
				if _, err := store.Delete(ctx, exclusionKey); err != nil {
					t.Fatalf("Delete(%s exclusion) error = %v", test.name, err)
				}
				stored, err := attaches.GetAttach(ctx, record.ID)
				if err != nil || stored.Record.Status != core.AttachFailed ||
					stored.Record.Operation != AttachOperationDetach {
					t.Fatalf("GetAttach(after retry race) = %#v/%v", stored, err)
				}
			})
		}
	})

	t.Run("acknowledgement finalization", func(t *testing.T) {
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
		record, facts := testPendingAttach(t, scope, 93, "detach-finalization-race", nil)
		createTestAttach(t, ctx, attaches, scope, record, &facts)
		agentID := ids.NewAt(ids.KindAgent, record.CreatedAt, 930)
		if _, found, err := tasks.ClaimNextTask(ctx, agentID, 13, record.CreatedAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextTask(provision) found/error = %v/%v", found, err)
		}
		if _, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			13,
			record.TaskID,
			taskAssignmentIDForTest(t, tasks, record.TaskID),
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
		if _, found, err := tasks.ClaimNextTask(ctx, agentID, 13, detachAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextTask(detach) found/error = %v/%v", found, err)
		}
		exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
		if err != nil {
			t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
		}
		raceStore := &attachBackupExclusionRaceStore{
			attachTestStore: store,
			exclusionKey:    exclusionKey,
			exclusionValue: testAttachBackupExclusionValue(
				t, scope.Environment.Record.ID, record.ID, 930,
			),
		}
		raceTasks, err := newTaskRepository(raceStore)
		if err != nil {
			t.Fatalf("newTaskRepository(race) error = %v", err)
		}
		_, err = raceTasks.AcknowledgeTask(
			ctx,
			agentID,
			13,
			detachTask.ID,
			taskAssignmentIDForTest(t, raceTasks, detachTask.ID),
			TaskStatusCompleted,
			completedComposeTaskResult(),
			detachAt.Add(2*time.Second),
		)
		if err == nil {
			t.Fatal("AcknowledgeTask(detach race) succeeded across active Backup exclusion")
		}
		stored, getErr := attaches.GetAttach(ctx, record.ID)
		if getErr != nil || stored.Record.Status != core.AttachDetaching {
			t.Fatalf("GetAttach(after finalization race) = %#v/%v", stored, getErr)
		}
	})
}

// Rationale: a concurrent exclusion is ResourceInUse only when its bytes are
// a valid Attach exclusion in the exact bucket; corrupt evidence is internal.
func TestAttachTaskBackedDetachRaceRejectsInvalidBackupSourceExclusion(t *testing.T) {
	for _, test := range []struct {
		name  string
		value func(*testing.T, AttachCreateScope, string) []byte
	}{
		{
			name: "malformed",
			value: func(*testing.T, AttachCreateScope, string) []byte {
				return []byte("not-an-exclusion-record")
			},
		},
		{
			name: "misbucketed",
			value: func(t *testing.T, scope AttachCreateScope, attachID string) []byte {
				return testAttachBackupExclusionValue(
					t,
					scope.Environment.Record.ID,
					ids.NewAt(ids.KindAttach, testAttachTime.Add(970*time.Second), 971),
					972,
				)
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := newAttachTestStore()
			scope := seedAttachScope(t, ctx, store)
			attaches, err := NewAttachRepository(store)
			if err != nil {
				t.Fatalf("NewAttachRepository() error = %v", err)
			}
			record, facts := testPendingAttach(t, scope, 97, "detach-invalid-exclusion-race", nil)
			ready := createTestAttach(t, ctx, attaches, scope, record, &facts)
			ready, err = advanceAttachReady(ctx, attaches, ready)
			if err != nil {
				t.Fatalf("advanceAttachReady() error = %v", err)
			}
			exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
			if err != nil {
				t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
			}
			raceStore := &attachBackupExclusionRaceStore{
				attachTestStore: store,
				exclusionKey:    exclusionKey,
				exclusionValue:  test.value(t, scope, record.ID),
			}
			raceAttaches, err := NewAttachRepository(raceStore)
			if err != nil {
				t.Fatalf("NewAttachRepository(race) error = %v", err)
			}
			scope, renderInput, task, marker := attachDetachRaceEnvelope(
				t, ctx, raceAttaches, scope, ready, record.CreatedAt.Add(6*time.Minute),
			)
			result, err := raceAttaches.BeginAttachDetachWithTask(
				ctx, scope, ready, renderInput, task, marker,
			)
			if err != nil {
				t.Fatalf("BeginAttachDetachWithTask() error = %v", err)
			}
			outcome, _, conflict, classifyErr := result.Classify()
			if classifyErr != nil || outcome != IdempotencyKnownConflict || !isKind(conflict, errs.KindInternal) {
				t.Fatalf(
					"BeginAttachDetachWithTask(%s).Classify() = %v/%v/%v, want internal conflict",
					test.name, outcome, conflict, classifyErr,
				)
			}
		})
	}
}

// Rationale: completed detach replay must prove removed companions stay absent,
// a reused name has an exact successor primary, and corrupt removal evidence
// cannot drive an unbounded companion read.
func TestCompletedAttachDetachReplayRejectsDanglingCompanions(t *testing.T) {
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
	agentID := ids.NewAt(ids.KindAgent, testAttachTime, 950)

	grantRecord, grantFacts := testPendingAttach(t, scope, 94, "replay-grant", nil)
	createTestAttach(t, ctx, attaches, scope, grantRecord, &grantFacts)
	if _, found, err := tasks.ClaimNextTask(
		ctx, agentID, 14, grantRecord.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask(grant) found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		14,
		grantRecord.TaskID,
		taskAssignmentIDForTest(t, tasks, grantRecord.TaskID),
		TaskStatusCompleted,
		completedComposeTaskResult(),
		grantRecord.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(grant) error = %v", err)
	}
	grant, err := attaches.GetAttach(ctx, grantRecord.ID)
	if err != nil {
		t.Fatalf("GetAttach(grant) error = %v", err)
	}

	sourceScope := scope
	sourceScope.Grants = []Versioned[AttachRecord]{grant}
	record, facts := testPendingAttach(t, sourceScope, 95, "replay-source", sourceScope.Grants)
	createTestAttach(t, ctx, attaches, sourceScope, record, &facts)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 15, record.CreatedAt.Add(3*time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask(source) found/error = %v/%v", found, err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		15,
		record.TaskID,
		taskAssignmentIDForTest(t, tasks, record.TaskID),
		TaskStatusCompleted,
		completedComposeTaskResult(),
		record.CreatedAt.Add(4*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(source) error = %v", err)
	}
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil {
		t.Fatalf("GetAttach(source ready) error = %v", err)
	}
	grant, err = attaches.GetAttach(ctx, grantRecord.ID)
	if err != nil {
		t.Fatalf("GetAttach(grant refreshed) error = %v", err)
	}
	sourceScope.Grants = []Versioned[AttachRecord]{grant}

	detachAt := record.CreatedAt.Add(5 * time.Second)
	detachTask := publishTestDetach(t, ctx, attaches, sourceScope, ready, detachAt)
	if _, found, err := tasks.ClaimNextTask(ctx, agentID, 16, detachAt.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask(detach) found/error = %v/%v", found, err)
	}
	assignmentID := taskAssignmentIDForTest(t, tasks, detachTask.ID)
	terminalAt := detachAt.Add(2 * time.Second)
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt,
	); err != nil {
		t.Fatalf("AcknowledgeTask(detach) error = %v", err)
	}

	dependentValue, err := encodeAttachDependentGrantIndex(record.ID, []string{grantRecord.ID})
	if err != nil {
		t.Fatalf("encodeAttachDependentGrantIndex() error = %v", err)
	}
	defer clear(dependentValue)
	unlistedGrantID := ids.NewAt(ids.KindAttach, terminalAt, 961)
	unlistedValue, err := encodeAttachDependentGrantIndex(record.ID, []string{unlistedGrantID})
	if err != nil {
		t.Fatalf("encodeAttachDependentGrantIndex(unlisted) error = %v", err)
	}
	defer clear(unlistedValue)
	companions := map[string]struct {
		key   string
		value []byte
	}{
		"name index":            {key: attachNameKey(record.EnvironmentID, record.Name), value: []byte(record.ID)},
		"owner index":           {key: attachOwnerKey(record.EnvironmentID, record.ID), value: []byte(record.ID)},
		"backing service index": {key: attachBackingServiceKey(record.BackingServiceID, record.ID), value: []byte(record.ID)},
		"backing project index": {key: attachBackingProjectKey(record.BackingProjectID, record.ID), value: []byte(record.ID)},
		"service index":         {key: attachServiceKey(record.ServiceIDs[0], record.ID), value: []byte(record.ID)},
		"encrypted facts":       {key: attachFactsKey(record.ID), value: []byte(record.ID)},
		"grant reverse index":   {key: attachGrantedByKey(grantRecord.ID, record.ID), value: []byte(record.ID)},
		"grant dependent index": {key: attachDependentGrantKey(record.ID), value: dependentValue},
		"unlisted grant dependent index": {key: attachDependentGrantKey(record.ID), value: unlistedValue},
	}
	for name, companion := range companions {
		if _, err := store.Put(ctx, companion.key, companion.value); err != nil {
			t.Fatalf("Put(%s) error = %v", name, err)
		}
		if _, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			16,
			detachTask.ID,
			assignmentID,
			TaskStatusCompleted,
			completedComposeTaskResult(),
			terminalAt.Add(time.Second),
		); err == nil {
			t.Fatalf("completed detach replay accepted dangling %s", name)
		}
		if _, err := store.Delete(ctx, companion.key); err != nil {
			t.Fatalf("Delete(%s) error = %v", name, err)
		}
	}
	reusedNameOwnerID := ids.NewAt(ids.KindAttach, terminalAt, 960)
	if _, err := store.Put(
		ctx,
		attachNameKey(record.EnvironmentID, record.Name),
		[]byte(reusedNameOwnerID),
	); err != nil {
		t.Fatalf("Put(reused name owner) error = %v", err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt.Add(2*time.Second),
	); !isKind(err, errs.KindInternal) {
		t.Fatalf("completed detach replay with dangling reused name error = %v, want internal", err)
	}
	if _, err := store.Delete(ctx, attachNameKey(record.EnvironmentID, record.Name)); err != nil {
		t.Fatalf("Delete(reused name owner) error = %v", err)
	}
	successorRecord, successorFacts := testPendingAttach(t, scope, 96, record.Name, nil)
	createTestAttach(t, ctx, attaches, scope, successorRecord, &successorFacts)
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("completed detach replay with exact successor name owner error = %v", err)
	}
	successorKey := attachKey(successorRecord.ID)
	successorValue, err := encodeAttachRecord(successorRecord)
	if err != nil {
		t.Fatalf("encodeAttachRecord(successor) error = %v", err)
	}
	defer clear(successorValue)
	for _, test := range []struct {
		name  string
		value []byte
	}{
		{name: "malformed successor primary", value: []byte("not-an-attach-record")},
		{
			name: "mismatched successor primary",
			value: func() []byte {
				mismatched := successorRecord
				mismatched.Name = "different-name"
				value, encodeErr := encodeAttachRecord(mismatched)
				if encodeErr != nil {
					t.Fatalf("encodeAttachRecord(mismatched successor) error = %v", encodeErr)
				}
				return value
			}(),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			defer clear(test.value)
			if _, err := store.Put(ctx, successorKey, test.value); err != nil {
				t.Fatalf("Put(%s) error = %v", test.name, err)
			}
			if _, err := tasks.AcknowledgeTask(
				ctx,
				agentID,
				16,
				detachTask.ID,
				assignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				terminalAt.Add(2*time.Second),
			); !isKind(err, errs.KindInternal) {
				t.Fatalf("completed detach replay with %s error = %v, want internal", test.name, err)
			}
			if _, err := store.Put(ctx, successorKey, successorValue); err != nil {
				t.Fatalf("restore successor primary error = %v", err)
			}
		})
	}
	storedRenderInput, err := attaches.GetAttachTaskRenderInput(ctx, detachTask.PlanID)
	if err != nil {
		t.Fatalf("GetAttachTaskRenderInput() error = %v", err)
	}
	originalRenderInputValue, err := encodeAttachTaskRenderInput(storedRenderInput.Record)
	if err != nil {
		t.Fatalf("encodeAttachTaskRenderInput(original) error = %v", err)
	}
	defer clear(originalRenderInputValue)
	for _, test := range []struct {
		name   string
		mutate func(AttachTaskRenderInput) AttachTaskRenderInput
	}{
		{
			name: "two consumers",
			mutate: func(input AttachTaskRenderInput) AttachTaskRenderInput {
				input.Services = append(input.Services, EnvironmentComposeIdentity{
					ID: ids.NewAt(ids.KindService, terminalAt.Add(100*time.Second), 999),
					Name: input.Services[0].Name + "-second",
				})
				left, right := input.Services[0].ID, input.Services[1].ID
				if right < left {
					left, right = right, left
				}
				input.ConsumerServiceIDs = []string{left, right}
				return input
			},
		},
		{
			name: "nine grants",
			mutate: func(input AttachTaskRenderInput) AttachTaskRenderInput {
				input.GrantAttachIDs = make([]string, MaximumAttachGrants+1)
				for index := range input.GrantAttachIDs {
					input.GrantAttachIDs[index] = ids.NewAt(
						ids.KindAttach,
						terminalAt.Add(time.Duration(index+1)*time.Second),
						int64(970+index),
					)
				}
				return input
			},
		},
	} {
		t.Run("render evidence "+test.name, func(t *testing.T) {
			corruptValue, encodeErr := encodeAttachTaskRenderInput(test.mutate(storedRenderInput.Record))
			if encodeErr != nil {
				t.Fatalf("encodeAttachTaskRenderInput(%s) error = %v", test.name, encodeErr)
			}
			defer clear(corruptValue)
			renderKey := attachTaskRenderInputKey(detachTask.PlanID)
			if _, err := store.Put(ctx, renderKey, corruptValue); err != nil {
				t.Fatalf("Put(%s render evidence) error = %v", test.name, err)
			}
			guardStore := &attachReplayReadGuardStore{attachTestStore: store, renderKey: renderKey}
			guardTasks, repositoryErr := newTaskRepository(guardStore)
			if repositoryErr != nil {
				t.Fatalf("newTaskRepository(replay guard) error = %v", repositoryErr)
			}
			if _, err := guardTasks.AcknowledgeTask(
				ctx,
				agentID,
				16,
				detachTask.ID,
				assignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				terminalAt.Add(2*time.Second),
			); !isKind(err, errs.KindInternal) {
				t.Fatalf("completed detach replay with %s error = %v, want internal", test.name, err)
			}
			if !guardStore.sawRenderInput || guardStore.readCompanions {
				t.Fatalf(
					"completed detach replay with %s saw render/companions = %v/%v",
					test.name,
					guardStore.sawRenderInput,
					guardStore.readCompanions,
				)
			}
			if _, err := store.Put(ctx, renderKey, originalRenderInputValue); err != nil {
				t.Fatalf("restore Attach render evidence error = %v", err)
			}
		})
	}
	exclusionKey, err := backupSourceTargetExclusionKey(BackupSourceTargetAttach, record.ID)
	if err != nil {
		t.Fatalf("backupSourceTargetExclusionKey() error = %v", err)
	}
	exclusionCases := []struct {
		name     string
		value    []byte
		wantKind errs.Kind
	}{
		{
			name:     "valid",
			value:    testAttachBackupExclusionValue(t, record.EnvironmentID, record.ID, 962),
			wantKind: errs.KindStateConflict,
		},
		{
			name:     "malformed",
			value:    []byte("not-an-exclusion-record"),
			wantKind: errs.KindInternal,
		},
		{
			name: "misbucketed",
			value: testAttachBackupExclusionValue(
				t,
				record.EnvironmentID,
				ids.NewAt(ids.KindAttach, terminalAt, 963),
				964,
			),
			wantKind: errs.KindInternal,
		},
	}
	for _, test := range exclusionCases {
		t.Run("exclusion "+test.name, func(t *testing.T) {
			if _, err := store.Put(ctx, exclusionKey, test.value); err != nil {
				t.Fatalf("Put(%s exclusion) error = %v", test.name, err)
			}
			if _, err := tasks.AcknowledgeTask(
				ctx,
				agentID,
				16,
				detachTask.ID,
				assignmentID,
				TaskStatusCompleted,
				completedComposeTaskResult(),
				terminalAt.Add(2*time.Second),
			); !isKind(err, test.wantKind) {
				t.Fatalf("completed detach replay with %s exclusion error = %v, want %v", test.name, err, test.wantKind)
			}
			if _, err := store.Delete(ctx, exclusionKey); err != nil {
				t.Fatalf("Delete(%s exclusion) error = %v", test.name, err)
			}
		})
	}
	replayTargetKey, err := idempotencyReplayTargetKey(
		IdempotencyReplayTarget{Kind: IdempotencyReplayTargetAttach, ID: record.ID},
		"DELETE",
		"/attaches/{id}",
		detachTask.IdempotencyKey,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	if mustOptionalKey(t, store.memoryHierarchyStore, replayTargetKey) == nil {
		t.Fatal("Attach deletion replay target is missing before corruption test")
	}
	if _, err := store.Delete(ctx, replayTargetKey); err != nil {
		t.Fatalf("Delete(replay target) error = %v", err)
	}
	if _, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		16,
		detachTask.ID,
		assignmentID,
		TaskStatusCompleted,
		completedComposeTaskResult(),
		terminalAt.Add(2*time.Second),
	); err == nil {
		t.Fatal("completed detach replay accepted a missing stable replay target")
	}
}

func attachDetachRaceEnvelope(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	current Versioned[AttachRecord],
	createdAt time.Time,
) (AttachCreateScope, AttachTaskRenderInput, TaskRecord, IdempotencyMarker) {
	t.Helper()
	hierarchy, err := NewHierarchyRepository(repository.store)
	if err != nil {
		t.Fatalf("NewHierarchyRepository() error = %v", err)
	}
	scope.Environment, err = hierarchy.GetEnvironment(ctx, scope.Environment.Record.ID)
	if err != nil {
		t.Fatalf("GetEnvironment() error = %v", err)
	}
	task := validTaskRecord(createdAt)
	owner, err := EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task.Owner = owner
	task.Actor = TaskActorOperator
	task.ID = ids.NewAt(ids.KindTask, createdAt, 940)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 941)
	task.IdempotencyKey = "attach-detach-exclusion-race-key-0001"
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 942)
	task.RenderGeneration = int32(scope.ComposeProjection.Record.RenderGeneration)
	task.Type = TaskDetach
	task.Target = current.Record.ID
	task.Params = map[string]string{TaskMutationEnvironmentParam: current.Record.EnvironmentID}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = current.Record.EnvironmentID
	marker.Locator.Method = "DELETE"
	marker.Locator.Route = "/attaches/{id}"
	marker.ReplayTarget = &IdempotencyReplayTarget{Kind: IdempotencyReplayTargetAttach, ID: current.Record.ID}
	renderInput := AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: current.Record.ID, AttachName: current.Record.Name,
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: current.Record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir: scope.Environment.Record.VolumeDir,
		BackingServiceID:    scope.BackingService.Record.Desired.ID,
		BackingProjectID:    current.Record.BackingProjectID,
		AdapterKey:          scope.BackingService.Record.Desired.Adapter,
		BlueprintRevisionID: scope.BlueprintRevision.Record.RevisionID,
		ArtifactID:          ids.NewAt(ids.KindConfig, createdAt, 943),
		RenderGeneration:    scope.ComposeProjection.Record.RenderGeneration,
		Services:            append([]EnvironmentComposeIdentity(nil), scope.ComposeProjection.Record.Services...),
		Networks:            append([]EnvironmentComposeIdentity(nil), scope.ComposeProjection.Record.Networks...),
		Volumes:             append([]EnvironmentComposeIdentity(nil), scope.ComposeProjection.Record.Volumes...),
		ConsumerServiceIDs:  append([]string(nil), current.Record.ServiceIDs...),
		GrantAttachIDs:      append([]string(nil), current.Record.GrantAttachIDs...),
	}
	return scope, renderInput, task, marker
}

type attachReplayReadGuardStore struct {
	*attachTestStore
	renderKey      string
	sawRenderInput bool
	readCompanions bool
}

func (store *attachReplayReadGuardStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	if store.sawRenderInput && len(request.Keys) > 1 {
		store.readCompanions = true
	}
	if len(request.Keys) == 1 && request.Keys[0] == store.renderKey {
		store.sawRenderInput = true
	}
	return store.attachTestStore.GetMany(ctx, request)
}

func assertAttachTaskEnvironmentEpoch(
	t *testing.T,
	ctx context.Context,
	store *attachTestStore,
	environmentID string,
	wantRevision int64,
) {
	t.Helper()
	result, err := store.Get(ctx, environmentMutationEpochKey(environmentID))
	if err != nil || result.Entry == nil || result.Entry.ModRevision != wantRevision {
		t.Fatalf("Attach Task Environment epoch = %#v, %v; want revision %d", result, err, wantRevision)
	}
}
