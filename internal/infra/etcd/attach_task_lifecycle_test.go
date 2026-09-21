package etcd

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	testattachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	testattachrender "github.com/AlanD20/groundplane/internal/infra/etcd/attachrender"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testenvironmentprojection "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
			record.TaskID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(), terminalAt)

	if err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, terminal.Revision)
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || ready.Record.Status != core.AttachReady || ready.Revision != terminal.Revision {
		t.Fatalf("ready Attach = %#v, %v", ready, err)
	}
	replay, err := tasks.AcknowledgeTask(
		ctx, agentID, 1, record.TaskID, taskAssignmentIDForTest(
			t,
			tasks,
			record.TaskID,
		), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(), terminalAt.Add(time.Second))

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
	if err != nil || aborted.Record.Status != testtaskjournal.TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v, %v", aborted.Record, err)
	}
	assertAttachTaskEnvironmentEpoch(t, ctx, store, scope.Environment.Record.ID, aborted.Revision)
	failed, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || failed.Record.Status != core.AttachFailed ||
		failed.Record.Operation != testattachments.AttachOperationProvision ||
		failed.Record.TaskID != record.TaskID ||
		failed.Revision != aborted.Revision {
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
	result := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone, ReconciliationRequired: true,
	}
	terminal, err := tasks.AcknowledgeTask(
		ctx, agentID, 2, record.TaskID, taskAssignmentIDForTest(t, tasks,
			record.TaskID), testtaskjournal.TaskStatusTimedOut, result, terminalAt)

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
	retryResult, err := tasks.RetryTask(ctx, record.TaskID, retryID, testtaskjournal.TaskActorOperator, marker)
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

// Rationale: an injected terminal failure may be followed by Controller
// restart; reconstruction must retain one Attach identity, one encrypted fact
// bundle, and one reverse grant edge while retrying the same durable operation.
func TestAttachGrantFailureSurvivesRepositoryRestartWithoutDuplicateIdentity(t *testing.T) {
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
	agentID := ids.NewAt(ids.KindAgent, testAttachTime, 820)

	grantRecord, grantFacts := testPendingAttach(t, scope, 84, "grant-target", nil)
	createTestAttach(t, ctx, attaches, scope, grantRecord, &grantFacts)
	if _, found, claimErr := tasks.ClaimNextTask(
		ctx, agentID, 20, grantRecord.CreatedAt.Add(time.Second),
	); claimErr != nil || !found {
		t.Fatalf("ClaimNextTask(grant) found/error = %v/%v", found, claimErr)
	}
	if _, err = tasks.AcknowledgeTask(
		ctx,
		agentID,
		20,
		grantRecord.TaskID,
		taskAssignmentIDForTest(t, tasks, grantRecord.TaskID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		grantRecord.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(grant) error = %v", err)
	}
	grant, err := attaches.GetAttach(ctx, grantRecord.ID)
	if err != nil {
		t.Fatalf("GetAttach(grant) error = %v", err)
	}

	sourceScope := scope
	sourceScope.Grants = []testkeyvalue.Versioned[testattachments.Record]{grant}
	record, facts := testPendingAttach(t, sourceScope, 85, "granted-source", sourceScope.Grants)
	expectedCiphertext := append([]byte(nil), facts.Ciphertext...)
	defer clear(expectedCiphertext)
	createTestAttach(t, ctx, attaches, sourceScope, record, &facts)
	if _, found, claimErr := tasks.ClaimNextTask(
		ctx, agentID, 20, record.CreatedAt.Add(3*time.Second),
	); claimErr != nil || !found {
		t.Fatalf("ClaimNextTask(source) found/error = %v/%v", found, claimErr)
	}
	failureAt := record.CreatedAt.Add(4 * time.Second)
	failedTask, err := tasks.AcknowledgeTask(
		ctx,
		agentID,
		20,
		record.TaskID,
		taskAssignmentIDForTest(
			t,
			tasks,
			record.TaskID,
		),
		testtaskjournal.TaskStatusTimedOut,
		testtaskjournal.TaskResultRecord{
			Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone,
			ReconciliationRequired: true,
		},
		failureAt,
	)
	if err != nil {
		t.Fatalf("AcknowledgeTask(injected timeout) error = %v", err)
	}

	attaches, err = NewAttachRepository(store)
	if err != nil {
		t.Fatalf("NewAttachRepository(restart) error = %v", err)
	}
	tasks, err = newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository(restart) error = %v", err)
	}
	failed, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || failed.Record.Status != core.AttachFailed ||
		failed.Record.ID != record.ID || failed.Record.Name != record.Name ||
		failed.Record.BackingServiceID != record.BackingServiceID ||
		!reflect.DeepEqual(failed.Record.GrantAttachIDs, record.GrantAttachIDs) ||
		!reflect.DeepEqual(failed.Record.FactSets, record.FactSets) {
		t.Fatalf("restarted failed Attach = %#v, %v", failed, err)
	}
	storedFacts, foundFacts, err := attaches.GetAttachFacts(ctx, failed)
	if err != nil || !foundFacts || string(storedFacts.Ciphertext) != "encrypted-facts" {
		t.Fatalf("restarted encrypted facts = %#v, %v, %v", storedFacts, foundFacts, err)
	}
	clear(storedFacts.Ciphertext)

	retryAt := failureAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 821)
	retryResult, err := tasks.RetryTask(
		ctx,
		record.TaskID,
		retryID,
		testtaskjournal.TaskActorOperator,
		pendingRetryMarker(failedTask.Record, retryID, retryAt, "grant-restart-retry-0001"),
	)
	if err != nil {
		t.Fatalf("RetryTask(restart) error = %v", err)
	}
	outcome, _, conflict, classifyErr := retryResult.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask(restart) outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	claim, found, err := tasks.ClaimNextTask(ctx, agentID, 20, retryAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextTask(retry) = %#v, %v, %v", claim, found, err)
	}
	if _, err = tasks.AcknowledgeTask(
		ctx,
		agentID,
		20,
		retryID,
		taskAssignmentIDForTest(t, tasks, retryID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
		retryAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeTask(retry) error = %v", err)
	}
	ready, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || ready.Record.Status != core.AttachReady || ready.Record.ID != record.ID ||
		ready.Record.Name != record.Name ||
		ready.Record.BackingServiceID != record.BackingServiceID ||
		ready.Record.ServiceID != record.ServiceID ||
		!reflect.DeepEqual(ready.Record.GrantAttachIDs, record.GrantAttachIDs) ||
		!reflect.DeepEqual(ready.Record.FactSets, record.FactSets) {
		t.Fatalf("ready Attach after restart = %#v, %v", ready, err)
	}
	retriedFacts, foundFacts, err := attaches.GetAttachFacts(ctx, ready)
	if err != nil || !foundFacts || retriedFacts.AttachID != facts.AttachID ||
		retriedFacts.EnvelopeVersion != facts.EnvelopeVersion || retriedFacts.Cipher != facts.Cipher ||
		retriedFacts.DigestAlgorithm != facts.DigestAlgorithm ||
		retriedFacts.CiphertextSHA256 != facts.CiphertextSHA256 ||
		!bytes.Equal(retriedFacts.Ciphertext, expectedCiphertext) {
		t.Fatalf("ready encrypted facts after restart = %#v, %v, %v", retriedFacts, foundFacts, err)
	}
	clear(retriedFacts.Ciphertext)
	reverse, err := store.Get(ctx, testattachments.AttachGrantedByKey(grant.Record.ID, record.ID))
	if err != nil || reverse.Entry == nil || string(reverse.Entry.Value) != record.ID {
		t.Fatalf("reverse grant edge after restart = %#v, %v", reverse, err)
	}
	page, err := attaches.ListAttaches(ctx, record.EnvironmentID, testkeyvalue.PageRequest{Limit: 10})
	if err != nil || len(page.Items) != 2 {
		t.Fatalf("Attach page after restart = %#v, %v", page, err)
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

			record.TaskID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
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
	failedResult := testtaskjournal.TaskResultRecord{
		Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone, ReconciliationRequired: true,
	}
	failedTask, err := tasks.AcknowledgeTask(
		ctx, agentID, 3, detachID, taskAssignmentIDForTest(t, tasks,
			detachID), testtaskjournal.TaskStatusTimedOut, failedResult, detachFailureAt)

	if err != nil {
		t.Fatalf("AcknowledgeTask(detach timeout) error = %v", err)
	}
	failed, err := attaches.GetAttach(ctx, record.ID)
	if err != nil || failed.Record.Status != core.AttachFailed ||
		failed.Record.Operation != testattachments.AttachOperationDetach ||
		failed.Revision != failedTask.Revision {
		t.Fatalf("failed detach Attach = %#v, %v", failed, err)
	}

	retryAt := detachFailureAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 806)
	marker := pendingRetryMarker(failedTask.Record, retryID, retryAt, "detach-retry-key-0001")
	retryResult, err := tasks.RetryTask(ctx, detachID, retryID, testtaskjournal.TaskActorOperator, marker)
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

			retryID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
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
	for _, key := range []string{testattachments.AttachNameKey(record.EnvironmentID, record.Name), testattachments.AttachOwnerKey(record.EnvironmentID, record.ID), testattachments.AttachServiceKey(record.ServiceID, record.ID), testattachments.AttachBackingServiceKey(record.BackingServiceID, record.ID), testattachments.AttachBackingProjectKey(record.BackingProjectID, record.ID), testattachments.AttachFactsKey(record.ID)} {
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

			retryID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
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
		exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
			testbackupruntime.BackupSourceTargetAttach,
			record.ID,
		)
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
		taskEntry, err := store.Get(ctx, testtaskjournal.TaskStorageKey(task.ID))
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
		if _, found, err := tasks.ClaimNextTask(
			ctx,
			agentID,
			12,
			record.CreatedAt.Add(time.Second),
		); err != nil ||
			!found {
			t.Fatalf("ClaimNextTask(provision) found/error = %v/%v", found, err)
		}
		if _, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			12,
			record.TaskID,
			taskAssignmentIDForTest(t, tasks, record.TaskID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
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
			taskAssignmentIDForTest(
				t,
				tasks,
				detachTask.ID,
			),
			testtaskjournal.TaskStatusTimedOut,
			testtaskjournal.TaskResultRecord{
				Kind: testtaskjournal.TaskResultCompose, Diagnostic: testtaskjournal.TaskResultDiagnosticNone, ReconciliationRequired: true,
			},
			detachAt.Add(2*time.Second),
		)
		if err != nil {
			t.Fatalf("AcknowledgeTask(detach timeout) error = %v", err)
		}
		exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
			testbackupruntime.BackupSourceTargetAttach,
			record.ID,
		)
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
					testtaskjournal.TaskActorOperator,
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
					stored.Record.Operation != testattachments.AttachOperationDetach {
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
		if _, found, err := tasks.ClaimNextTask(
			ctx,
			agentID,
			13,
			record.CreatedAt.Add(time.Second),
		); err != nil ||
			!found {
			t.Fatalf("ClaimNextTask(provision) found/error = %v/%v", found, err)
		}
		if _, err := tasks.AcknowledgeTask(
			ctx,
			agentID,
			13,
			record.TaskID,
			taskAssignmentIDForTest(t, tasks, record.TaskID), testtaskjournal.TaskStatusCompleted, completedComposeTaskResult(),
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
		exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
			testbackupruntime.BackupSourceTargetAttach,
			record.ID,
		)
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
			taskAssignmentIDForTest(
				t,
				raceTasks,
				detachTask.ID,
			),
			testtaskjournal.TaskStatusCompleted,
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
			exclusionKey, err := testbackupruntime.BackupSourceTargetExclusionKey(
				testbackupruntime.BackupSourceTargetAttach,
				record.ID,
			)
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

func attachDetachRaceEnvelope(
	t *testing.T,
	ctx context.Context,
	repository *AttachRepository,
	scope AttachCreateScope,
	current testkeyvalue.Versioned[testattachments.Record],
	createdAt time.Time,
) (AttachCreateScope, testattachrender.AttachTaskRenderInput, TaskRecord, testidempotency.IdempotencyMarker) {
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
	owner, err := testtaskjournal.EnvironmentTaskOwner(scope.Project.Record, scope.Environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task.Owner = owner
	task.Actor = testtaskjournal.TaskActorOperator
	task.ID = ids.NewAt(ids.KindTask, createdAt, 940)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, 941)
	task.IdempotencyKey = "attach-detach-exclusion-race-key-0001"
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, 942)
	task.RenderGeneration = int32(scope.ComposeProjection.Record.RenderGeneration)
	task.Type = testtaskjournal.TaskDetach
	task.Target = current.Record.ID
	task.Params = map[string]string{testtaskjournal.TaskMutationEnvironmentParam: current.Record.EnvironmentID}
	marker := pendingTaskMarker(task)
	marker.Locator.ScopeID = current.Record.EnvironmentID
	marker.Locator.Method = "DELETE"
	marker.Locator.Route = "/attaches/{id}"
	marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetAttach,
		ID:   current.Record.ID,
	}
	renderInput := testattachrender.AttachTaskRenderInput{
		PlanID: task.PlanID, AttachID: current.Record.ID, AttachName: current.Record.Name,
		TenantID: scope.Tenant.Record.ID, TenantSlug: scope.Tenant.Record.Slug,
		ProjectID: scope.Project.Record.ID, ProjectSlug: scope.Project.Record.Slug,
		EnvironmentID: current.Record.EnvironmentID, EnvironmentName: scope.Environment.Record.Name,
		AuthorizedVolumeDir:      scope.Environment.Record.VolumeDir,
		BackingServiceID:         scope.BackingService.Record.Desired.ID,
		BackingProjectID:         current.Record.BackingProjectID,
		AdapterKey:               scope.BackingService.Record.Desired.Adapter,
		DesiredRevisionID:        scope.DesiredHead.Record.RevisionID,
		ArtifactID:               ids.NewAt(ids.KindConfig, createdAt, 943),
		RenderGeneration:         scope.ComposeProjection.Record.RenderGeneration,
		EnvironmentEpochRevision: attachTestEpochRevision(t, ctx, repository.store, current.Record.EnvironmentID),
		RuntimeProjection:        scope.ComposeProjection.Record,
		RuntimePreparation:       configuredAttachRuntimePreparation(task, current.Record.EnvironmentID),
		Services: testattachrender.AttachTaskServiceSnapshots(
			scope.ComposeProjection.Record.DesiredServices,
		),
		Networks: testattachrender.AttachTaskOwnedNetworkSnapshots(
			scope.ComposeProjection.Record.DesiredZones,
		),
		Volumes: append(
			[]testenvironmentprojection.EnvironmentVolumeIdentity(nil),
			scope.ComposeProjection.Record.Volumes...),
		VolumeMounts: append(
			[]testenvironmentprojection.EnvironmentServiceVolumeMount(nil),
			scope.ComposeProjection.Record.VolumeMounts...),
		ConsumerServiceIDs: []string{current.Record.ServiceID},
		GrantAttachIDs:     append([]string(nil), current.Record.GrantAttachIDs...),
	}
	return scope, renderInput, task, marker
}

func assertAttachTaskEnvironmentEpoch(
	t *testing.T,
	ctx context.Context,
	store *attachTestStore,
	environmentID string,
	wantRevision int64,
) {
	t.Helper()
	result, err := store.Get(ctx, testhierarchy.EnvironmentMutationEpochKey(environmentID))
	if err != nil || result.Entry == nil || result.Entry.ModRevision != wantRevision {
		t.Fatalf("Attach Task Environment epoch = %#v, %v; want revision %d", result, err, wantRevision)
	}
}
