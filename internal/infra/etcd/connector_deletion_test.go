package etcd

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: successful Connector removal must keep the fenced record visible
// and atomically erase every index and credential only at finalization.
func TestConnectorDeletionTaskFencesAndFinalizesCompleteConnector(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newConnectorDeletionFixture(t)
	connectors, err := newConnectorRepository(fixture.store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task, marker, tombstone, intent := connectorDeletionTestTask(t,
		fixture.connector, fixture.project, fixture.environment, fixture.now.Add(time.Minute), 2500,
	)
	result, err := connectors.BeginConnectorDeletionWithTask(
		ctx,
		fixture.environment,
		fixture.project,
		fixture.connector,
		tombstone,
		intent,
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("deletion outcome/conflict/error = %v/%v/%v", outcome, conflict, classifyErr)
	}
	epochAfterBegin := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	)
	replayPointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2501)
	replayReferenceKey, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(task.Target, replayPointID)
	if err != nil {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
	}
	connectorDeletionPutKey(t, fixture.store, replayReferenceKey, []byte(replayPointID))
	storedIntent, found, err := connectors.GetConnectorRemovalIntent(ctx, task.ID)
	if err != nil || !found || storedIntent.Record != intent {
		t.Fatalf("GetConnectorRemovalIntent() = %#v/%v/%v", storedIntent, found, err)
	}
	replayed, err := connectors.BeginConnectorDeletionWithTask(
		ctx,
		fixture.environment,
		fixture.project,
		fixture.connector,
		tombstone,
		intent,
		task,
		marker,
	)
	if err != nil {
		t.Fatalf("BeginConnectorDeletionWithTask(replay) error = %v", err)
	}
	outcome, _, conflict, classifyErr = replayed.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting {
		t.Fatalf(
			"deletion replay outcome/conflict/error = %v/%v/%v",
			outcome,
			conflict,
			classifyErr,
		)
	}
	if epochAfterReplay := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epochAfterReplay != epochAfterBegin {
		t.Fatalf("deletion replay epoch = %d, want %d", epochAfterReplay, epochAfterBegin)
	}
	connectorDeletionDeleteKey(t, fixture.store, replayReferenceKey)
	if mustOptionalKey(t, fixture.store, replayReferenceKey) != nil {
		t.Fatal("replay-only Recovery Point reference remained before completed acknowledgement")
	}
	assertConnectorDeletionVisible(t, connectors, fixture.connector.Record.Connector.ID)
	page, err := connectors.ListConnectors(
		ctx, fixture.environment.Record.ID, testkeyvalue.PageRequest{Limit: 10},
	)
	if err != nil || len(page.Items) != 1 || page.Items[0].Record.Connector.ID != task.Target {
		t.Fatalf("ListConnectors(fenced) = %#v, %v", page, err)
	}
	assertConnectorDeletionKeyState(t, fixture, task, true, true)
	assertConnectorCredentialPresence(t, fixture.store, fixture.connector.Record.Connector.ID, true)
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx,
		task.CreatedAt.Add(time.Second),
	); err != nil ||
		!found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	terminal, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v/%v", terminal, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epoch != terminal.Revision {
		t.Fatalf("completed Connector deletion epoch = %d, want %d", epoch, terminal.Revision)
	}
	for _, key := range []string{testconnectors.RecordKey(task.Target), testconnectors.ConnectorEnvironmentKey(fixture.environment.Record.ID, task.Target), testconnectors.ConnectorNameKey(fixture.environment.Record.ID, fixture.connector.Record.Connector.Name), testconnectors.CredentialValueKey(task.Target), testdeletions.TombstoneKey(string(testdeletions.DeletionTargetConnector), task.Target), testconnectors.RemovalIntentKey(task.ID)} {
		stored, getErr := fixture.store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
	assertConnectorDeletionKeyState(t, fixture, task, false, false)
	replayOrphanID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2502)
	replayOrphanKey, err := testbackupruntime.BackupOrphanConnectorIndexKey(task.Target, replayOrphanID)
	if err != nil {
		t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
	}
	for name, reference := range map[string]struct {
		key   string
		value []byte
	}{
		"enabled policy": {
			key:   testbackuppolicy.BackupPolicyConnectorReferenceKey(task.Target, fixture.environment.Record.ID),
			value: []byte(fixture.environment.Record.ID),
		},
		"Recovery Point": {key: replayReferenceKey, value: []byte(replayPointID)},
		"orphan":         {key: replayOrphanKey, value: []byte(replayOrphanID)},
	} {
		connectorDeletionPutKey(t, fixture.store, reference.key, reference.value)
		if _, err := tasks.AcknowledgeControllerTask(
			ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
		); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("AcknowledgeControllerTask(replay with %s) error = %v, want resource in use", name, err)
		}
		connectorDeletionDeleteKey(t, fixture.store, reference.key)
	}
	reusedNameOwnerID := ids.NewAt(ids.KindConnector, fixture.now, 2503)
	connectorDeletionPutKey(
		t,
		fixture.store,
		testconnectors.ConnectorNameKey(fixture.environment.Record.ID, fixture.connector.Record.Connector.Name),
		[]byte(reusedNameOwnerID),
	)
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay with reused name) error = %v", err)
	}
}

// Rationale: failed, timed-out, and aborted cleanup must restore visibility
// without losing credentials, while retry must reacquire a fresh typed fence.
func TestConnectorDeletionFailureTimeoutAbortAndRetryRestoreVisibility(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newConnectorDeletionFixture(t)
	connectors, err := newConnectorRepository(fixture.store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	tasks, err := newTaskRepository(fixture.store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	task, marker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(2*time.Minute),
		2510,
	)
	if _, err := connectors.BeginConnectorDeletionWithTask(
		ctx,
		fixture.environment,
		fixture.project,
		fixture.connector,
		tombstone,
		intent,
		task,
		marker,
	); err != nil {
		t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx,
		task.CreatedAt.Add(time.Second),
	); err != nil ||
		!found {
		t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, testtaskjournal.TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epoch != failed.Revision {
		t.Fatalf("failed Connector deletion epoch = %d, want %d", epoch, failed.Revision)
	}
	assertConnectorDeletionVisible(t, connectors, task.Target)
	assertConnectorCredentialPresence(t, fixture.store, task.Target, true)
	assertConnectorIntentPresence(t, fixture.store, task.ID, false)
	assertConnectorDeletionKeyState(t, fixture, task, true, false)

	retryID := ids.NewAt(ids.KindTask, task.CreatedAt.Add(3*time.Second), 2520)
	retryMarker := pendingRetryMarker(
		task,
		retryID,
		task.CreatedAt.Add(3*time.Second),
		"connector-retry-key-0001",
	)
	retryResult, err := tasks.RetryTask(
		ctx,
		task.ID,
		retryID, testtaskjournal.TaskActorOperator, retryMarker,
	)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epoch != retryResult.revision {
		t.Fatalf("retried Connector deletion epoch = %d, want %d", epoch, retryResult.revision)
	}
	assertConnectorDeletionVisible(t, connectors, task.Target)
	claim, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(4*time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextControllerTask(retry) = %#v/%v/%v", claim, found, err)
	}
	expired, err := tasks.ExpireTimedOutTasks(
		ctx,
		claim.Assignment.Record.Deadline.Add(time.Second),
	)
	if err != nil || expired != 1 {
		t.Fatalf("ExpireTimedOutTasks() = %d/%v", expired, err)
	}
	assertConnectorDeletionVisible(t, connectors, task.Target)
	assertConnectorCredentialPresence(t, fixture.store, task.Target, true)
	assertConnectorIntentPresence(t, fixture.store, retryID, false)

	timedOut, err := tasks.GetTask(ctx, retryID)
	if err != nil || timedOut.Record.Status != testtaskjournal.TaskStatusTimedOut {
		t.Fatalf("GetTask(timed out) = %#v/%v", timedOut, err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epoch != timedOut.Revision {
		t.Fatalf("timed-out Connector deletion epoch = %d, want %d", epoch, timedOut.Revision)
	}
	abortID := ids.NewAt(ids.KindTask, task.CreatedAt.Add(5*time.Second), 2530)
	abortMarker := pendingRetryMarker(
		timedOut.Record, abortID, task.CreatedAt.Add(5*time.Second), "connector-retry-key-0002",
	)
	abortRetryResult, err := tasks.RetryTask(
		ctx,
		retryID,
		abortID, testtaskjournal.TaskActorOperator, abortMarker,
	)
	if err != nil {
		t.Fatalf("RetryTask(after timeout) error = %v", err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epoch != abortRetryResult.revision {
		t.Fatalf("second Connector retry epoch = %d, want %d", epoch, abortRetryResult.revision)
	}
	assertConnectorDeletionVisible(t, connectors, task.Target)
	aborted, err := tasks.AbortPendingTask(
		ctx,
		abortID,
		task.CreatedAt.Add(6*time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	if epoch := mustEnvironmentMutationEpochRevision(
		t,
		fixture.store,
		fixture.environment.Record.ID,
	); epoch != aborted.Revision {
		t.Fatalf("aborted Connector deletion epoch = %d, want %d", epoch, aborted.Revision)
	}
	assertConnectorDeletionVisible(t, connectors, task.Target)
	assertConnectorCredentialPresence(t, fixture.store, task.Target, true)
	assertConnectorIntentPresence(t, fixture.store, abortID, false)
}

// Rationale: Connector delete and Backup Policy enable share the Connector
// tombstone/reference fences, so a concurrent race must permit exactly one.
func TestConnectorDeletionRacesBackupPolicyEnableWithExactlyOneWinner(t *testing.T) {
	ctx := context.Background()
	fixture := newConnectorDeletionFixture(t)
	lockedStore := &connectorDeletionLockedStore{store: fixture.store}
	connectors, err := newConnectorRepository(lockedStore)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	policies, err := newBackupPolicyRepository(lockedStore)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	task, deleteMarker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(3*time.Minute),
		2540,
	)
	candidate := connectorDeletionPolicyCandidate(t, fixture)
	defer candidate.Destroy()
	policyMarker := backupPolicyReplacementMarker(
		fixture.environment.Record.ID, "connector-race-policy-key-0001",
	)

	type attempt struct {
		result IdempotencyTransactionResult
		err    error
	}
	start := make(chan struct{})
	deleteAttempt := make(chan attempt, 1)
	policyAttempt := make(chan attempt, 1)
	var ready sync.WaitGroup
	ready.Add(2)
	go func() {
		ready.Done()
		<-start
		result, runErr := connectors.BeginConnectorDeletionWithTask(
			ctx, fixture.environment, fixture.project, fixture.connector,
			tombstone, intent, task, deleteMarker,
		)
		deleteAttempt <- attempt{result: result, err: runErr}
	}()
	go func() {
		ready.Done()
		<-start
		result, runErr := policies.ReplaceBackupPolicyProtected(ctx, candidate, policyMarker)
		policyAttempt <- attempt{result: result, err: runErr}
	}()
	ready.Wait()
	close(start)
	deleteResult := <-deleteAttempt
	policyResult := <-policyAttempt

	winners := 0
	for name, value := range map[string]attempt{"delete": deleteResult, "policy": policyResult} {
		if value.err != nil {
			if !errors.Is(value.err, errs.New(errs.KindResourceInUse, "")) {
				t.Fatalf("%s attempt error = %v", name, value.err)
			}
			continue
		}
		outcome, _, conflict, classifyErr := value.result.Classify()
		if classifyErr != nil {
			t.Fatalf("%s classify error = %v", name, classifyErr)
		}
		if outcome == IdempotencyKnownApplied && conflict == nil {
			winners++
		}
	}
	if winners != 1 {
		t.Fatalf("race winners = %d, want 1", winners)
	}
	reference := mustOptionalKey(t, fixture.store, testbackuppolicy.BackupPolicyConnectorReferenceKey(
		fixture.connector.Record.Connector.ID, fixture.environment.Record.ID,
	))
	tombstoneEntry := mustOptionalKey(t, fixture.store, testdeletions.TombstoneKey(
		string(testdeletions.DeletionTargetConnector), fixture.connector.Record.Connector.ID,
	))
	if (reference != nil) == (tombstoneEntry != nil) {
		t.Fatalf(
			"reference/tombstone outcome mismatch: reference=%#v tombstone=%#v",
			reference,
			tombstoneEntry,
		)
	}
	assertConnectorDeletionVisible(t, connectors, fixture.connector.Record.Connector.ID)
}

// Rationale: a Recovery Point published after the fixed read must fail the
// deletion prefix compare rather than allowing both durable states to commit.
func TestConnectorDeletionRacesRecoveryPointMembership(t *testing.T) {
	ctx := context.Background()
	fixture := newConnectorDeletionFixture(t)
	pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2690)
	referenceKey, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(
		fixture.connector.Record.Connector.ID,
		pointID,
	)
	if err != nil {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
	}
	raceStore := &connectorReferenceRaceStore{
		memoryHierarchyStore: fixture.store,
		referenceKey:         referenceKey,
		referenceValue:       []byte(pointID),
	}
	connectors, err := newConnectorRepository(raceStore)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	task, marker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(4*time.Minute),
		2691,
	)
	result, err := connectors.BeginConnectorDeletionWithTask(
		ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
	}
	_, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || !errors.Is(conflict, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("Connector deletion race conflict/error = %v/%v", conflict, classifyErr)
	}
	if mustOptionalKey(t, fixture.store, referenceKey) == nil {
		t.Fatal("Recovery Point Connector membership was not injected")
	}
	if mustOptionalKey(t, fixture.store, testdeletions.TombstoneKey(
		string(testdeletions.DeletionTargetConnector), task.Target,
	)) != nil {
		t.Fatal("Connector deletion tombstone committed across Recovery Point race")
	}
}

// Rationale: a post-read artifact index is authoritative only when its raw
// stable id reconstructs the exact canonical raw Connector reverse-reference key.
func TestConnectorDeletionRaceRejectsMalformedRecoveryPointMembership(t *testing.T) {
	ctx := context.Background()
	fixture := newConnectorDeletionFixture(t)
	indexedPointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2703)
	rawPointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2704)
	referenceKey, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(
		fixture.connector.Record.Connector.ID,
		indexedPointID,
	)
	if err != nil {
		t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
	}
	raceStore := &connectorReferenceRaceStore{
		memoryHierarchyStore: fixture.store,
		referenceKey:         referenceKey,
		referenceValue:       []byte(rawPointID),
	}
	connectors, err := newConnectorRepository(raceStore)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	task, marker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(4*time.Minute),
		2705,
	)
	result, err := connectors.BeginConnectorDeletionWithTask(
		ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
	)
	if err != nil {
		t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
	}
	_, _, conflict, classifyErr := result.Classify()
	classified := classifyErr
	if classified == nil {
		classified = conflict
	}
	if !errors.Is(classified, errs.New(errs.KindInternal, "")) {
		t.Fatalf("Connector malformed reference race error = %v, want internal", classified)
	}
	if mustOptionalKey(t, fixture.store, testdeletions.TombstoneKey(
		string(testdeletions.DeletionTargetConnector), task.Target,
	)) != nil {
		t.Fatal("Connector deletion committed across malformed Recovery Point reference race")
	}
}

// Rationale: retry publication and successful finalization are independent
// destructive commits and must re-prove artifact reverse-reference absence.
func TestConnectorDeletionRetryAndFinalizationRejectArtifactMemberships(t *testing.T) {
	t.Run("finalization Recovery Point", func(t *testing.T) {
		ctx := context.Background()
		fixture := newConnectorDeletionFixture(t)
		connectors, err := newConnectorRepository(fixture.store)
		if err != nil {
			t.Fatalf("newConnectorRepository() error = %v", err)
		}
		tasks, err := newTaskRepository(fixture.store)
		if err != nil {
			t.Fatalf("newTaskRepository() error = %v", err)
		}
		task, marker, tombstone, intent := connectorDeletionTestTask(
			t, fixture.connector, fixture.project, fixture.environment, fixture.now.Add(5*time.Minute), 2692,
		)
		if _, err := connectors.BeginConnectorDeletionWithTask(
			ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
		); err != nil {
			t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
		}
		if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
		}
		pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2693)
		referenceKey, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(task.Target, pointID)
		if err != nil {
			t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
		}
		connectorDeletionPutKey(t, fixture.store, referenceKey, []byte(pointID))
		_, err = tasks.AcknowledgeControllerTask(
			ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
		)
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("AcknowledgeControllerTask() error = %v, want resource in use", err)
		}
		assertConnectorDeletionKeyState(t, fixture, task, true, true)
	})

	t.Run("retry orphan", func(t *testing.T) {
		ctx := context.Background()
		fixture := newConnectorDeletionFixture(t)
		connectors, err := newConnectorRepository(fixture.store)
		if err != nil {
			t.Fatalf("newConnectorRepository() error = %v", err)
		}
		tasks, err := newTaskRepository(fixture.store)
		if err != nil {
			t.Fatalf("newTaskRepository() error = %v", err)
		}
		task, marker, tombstone, intent := connectorDeletionTestTask(
			t, fixture.connector, fixture.project, fixture.environment, fixture.now.Add(6*time.Minute), 2694,
		)
		if _, err := connectors.BeginConnectorDeletionWithTask(
			ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
		); err != nil {
			t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
		}
		if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
		}
		if _, err := tasks.AcknowledgeControllerTask(
			ctx, task.ID, testtaskjournal.TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
		); err != nil {
			t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
		}
		pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2695)
		referenceKey, err := testbackupruntime.BackupOrphanConnectorIndexKey(task.Target, pointID)
		if err != nil {
			t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
		}
		connectorDeletionPutKey(t, fixture.store, referenceKey, []byte(pointID))
		retryAt := task.CreatedAt.Add(3 * time.Second)
		retryID := ids.NewAt(ids.KindTask, retryAt, 2696)
		_, err = tasks.RetryTask(
			ctx,
			task.ID,
			retryID,
			testtaskjournal.TaskActorOperator,
			pendingRetryMarker(task, retryID, retryAt, "connector-artifact-retry-key-0001"),
		)
		if !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("RetryTask() error = %v, want resource in use", err)
		}
		assertConnectorDeletionVisible(t, connectors, task.Target)
	})
}

// Rationale: retry and terminal cleanup each read reference namespaces before
// their transaction, so an artifact membership inserted in that gap must win.
func TestConnectorDeletionRetryAndFinalizationRaceArtifactMemberships(t *testing.T) {
	t.Run("retry", func(t *testing.T) {
		ctx := context.Background()
		fixture := newConnectorDeletionFixture(t)
		connectors, err := newConnectorRepository(fixture.store)
		if err != nil {
			t.Fatalf("newConnectorRepository() error = %v", err)
		}
		tasks, err := newTaskRepository(fixture.store)
		if err != nil {
			t.Fatalf("newTaskRepository() error = %v", err)
		}
		task, marker, tombstone, intent := connectorDeletionTestTask(
			t, fixture.connector, fixture.project, fixture.environment, fixture.now.Add(7*time.Minute), 2697,
		)
		if _, err := connectors.BeginConnectorDeletionWithTask(
			ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
		); err != nil {
			t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
		}
		if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
		}
		if _, err := tasks.AcknowledgeControllerTask(
			ctx, task.ID, testtaskjournal.TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
		); err != nil {
			t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
		}
		pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2698)
		referenceKey, err := testbackupruntime.BackupOrphanConnectorIndexKey(task.Target, pointID)
		if err != nil {
			t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
		}
		raceStore := &connectorReferenceRaceStore{
			memoryHierarchyStore: fixture.store,
			referenceKey:         referenceKey,
			referenceValue:       []byte(pointID),
		}
		raceTasks, err := newTaskRepository(raceStore)
		if err != nil {
			t.Fatalf("newTaskRepository(race) error = %v", err)
		}
		retryAt := task.CreatedAt.Add(3 * time.Second)
		retryID := ids.NewAt(ids.KindTask, retryAt, 2699)
		result, err := raceTasks.RetryTask(
			ctx,
			task.ID,
			retryID,
			testtaskjournal.TaskActorOperator,
			pendingRetryMarker(task, retryID, retryAt, "connector-reference-race-retry-key-0001"),
		)
		if err != nil {
			t.Fatalf("RetryTask() error = %v", err)
		}
		outcome, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || conflict == nil || outcome == IdempotencyKnownApplied {
			t.Fatalf(
				"RetryTask().Classify() = %v/%v/%v, want conflict",
				outcome,
				conflict,
				classifyErr,
			)
		}
		if mustOptionalKey(t, fixture.store, testtaskjournal.TaskStorageKey(retryID)) != nil {
			t.Fatal("Connector retry Task committed across orphan membership race")
		}
		if mustOptionalKey(t, fixture.store, testdeletions.TombstoneKey(
			string(testdeletions.DeletionTargetConnector), task.Target,
		)) != nil {
			t.Fatal("Connector retry tombstone committed across orphan membership race")
		}
		assertConnectorDeletionVisible(t, connectors, task.Target)
	})

	t.Run("acknowledgement finalization", func(t *testing.T) {
		ctx := context.Background()
		fixture := newConnectorDeletionFixture(t)
		connectors, err := newConnectorRepository(fixture.store)
		if err != nil {
			t.Fatalf("newConnectorRepository() error = %v", err)
		}
		tasks, err := newTaskRepository(fixture.store)
		if err != nil {
			t.Fatalf("newTaskRepository() error = %v", err)
		}
		task, marker, tombstone, intent := connectorDeletionTestTask(
			t, fixture.connector, fixture.project, fixture.environment, fixture.now.Add(8*time.Minute), 2701,
		)
		if _, err := connectors.BeginConnectorDeletionWithTask(
			ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
		); err != nil {
			t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
		}
		if _, found, err := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second)); err != nil || !found {
			t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
		}
		pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2702)
		referenceKey, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(task.Target, pointID)
		if err != nil {
			t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
		}
		raceStore := &connectorReferenceRaceStore{
			memoryHierarchyStore: fixture.store,
			referenceKey:         referenceKey,
			referenceValue:       []byte(pointID),
		}
		raceTasks, err := newTaskRepository(raceStore)
		if err != nil {
			t.Fatalf("newTaskRepository(race) error = %v", err)
		}
		_, err = raceTasks.AcknowledgeControllerTask(
			ctx, task.ID, testtaskjournal.TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
		)
		if err == nil {
			t.Fatal("AcknowledgeControllerTask() succeeded across Recovery Point membership race")
		}
		if mustOptionalKey(t, fixture.store, referenceKey) == nil {
			t.Fatal("Recovery Point Connector membership was not injected")
		}
		assertConnectorDeletionKeyState(t, fixture, task, true, true)
		assertConnectorDeletionVisible(t, connectors, task.Target)
	})
}
