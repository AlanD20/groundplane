package etcd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"filippo.io/age"
	"github.com/AlanD20/groundplane/internal/common/ids"
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
	assertConnectorDeletionVisible(t, connectors, fixture.connector.Record.Connector.ID)
	page, err := connectors.ListConnectors(
		ctx, fixture.environment.Record.ID, PageRequest{Limit: 10},
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
		ctx, task.ID, TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask() = %#v/%v", terminal, err)
	}
	for _, key := range []string{
		connectorRecordKey(task.Target),
		connectorEnvironmentKey(fixture.environment.Record.ID, task.Target),
		connectorNameKey(fixture.environment.Record.ID, fixture.connector.Record.Connector.Name),
		connectorCredentialValueKey(task.Target),
		deletionTombstoneKey(string(DeletionTargetConnector), task.Target),
		connectorRemovalIntentKey(task.ID),
	} {
		stored, getErr := fixture.store.Get(ctx, key)
		if getErr != nil || stored.Entry != nil {
			t.Fatalf("finalized key %s = %#v/%v", key, stored, getErr)
		}
	}
	assertConnectorDeletionKeyState(t, fixture, task, false, false)
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, TaskStatusCompleted, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
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
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, task.ID, TaskStatusFailed, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
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
	if _, err := tasks.RetryTask(
		ctx,
		task.ID,
		retryID,
		TaskActorOperator,
		retryMarker,
	); err != nil {
		t.Fatalf("RetryTask() error = %v", err)
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
	if err != nil || timedOut.Record.Status != TaskStatusTimedOut {
		t.Fatalf("GetTask(timed out) = %#v/%v", timedOut, err)
	}
	abortID := ids.NewAt(ids.KindTask, task.CreatedAt.Add(5*time.Second), 2530)
	abortMarker := pendingRetryMarker(
		timedOut.Record, abortID, task.CreatedAt.Add(5*time.Second), "connector-retry-key-0002",
	)
	if _, err := tasks.RetryTask(
		ctx,
		retryID,
		abortID,
		TaskActorOperator,
		abortMarker,
	); err != nil {
		t.Fatalf("RetryTask(after timeout) error = %v", err)
	}
	assertConnectorDeletionVisible(t, connectors, task.Target)
	if _, err := tasks.AbortPendingTask(
		ctx,
		abortID,
		task.CreatedAt.Add(6*time.Second),
	); err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
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
		result, runErr := policies.replaceBackupPolicyProtected(ctx, candidate, policyMarker)
		policyAttempt <- attempt{result: result, err: runErr}
	}()
	ready.Wait()
	close(start)
	deleteResult := <-deleteAttempt
	policyResult := <-policyAttempt

	winners := 0
	for name, value := range map[string]attempt{"delete": deleteResult, "policy": policyResult} {
		if value.err != nil {
			t.Fatalf("%s attempt error = %v", name, value.err)
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
	reference := mustOptionalKey(t, fixture.store, backupPolicyConnectorReferenceKey(
		fixture.connector.Record.Connector.ID, fixture.environment.Record.ID,
	))
	tombstoneEntry := mustOptionalKey(t, fixture.store, deletionTombstoneKey(
		string(DeletionTargetConnector), fixture.connector.Record.Connector.ID,
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

// Rationale: protected Connector deletion must reject any durable Task or
// marker shape that would make retry or post-delete replay ambiguous.
func TestConnectorDeletionRejectsMalformedTaskAndMarker(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*TaskRecord, *IdempotencyMarker){
		"missing task key": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.IdempotencyKey = ""
		},
		"mismatched task key": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.IdempotencyKey = "connector-remove-key-0002"
		},
		"wrong timeout": func(task *TaskRecord, _ *IdempotencyMarker) {
			task.TimeoutSeconds++
		},
		"wrong method": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Locator.Method = http.MethodPost
		},
		"wrong route": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Locator.Route = "/connectors"
		},
		"wrong response status": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Response.Status = http.StatusOK
		},
		"wrong response content kind": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Response.ContentKind = "text/plain"
		},
		"wrong response body": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.Response.Body = []byte("{}")
		},
		"missing replay target": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.ReplayTarget = nil
		},
		"wrong replay target": func(_ *TaskRecord, marker *IdempotencyMarker) {
			marker.ReplayTarget = &IdempotencyReplayTarget{
				Kind: IdempotencyReplayTargetConnector,
				ID:   ids.NewAt(ids.KindConnector, marker.CreatedAt, 2790),
			}
		},
		"missing environment pin": func(task *TaskRecord, _ *IdempotencyMarker) {
			delete(task.Params, TaskConnectorEnvironmentParam)
		},
		"missing name pin": func(task *TaskRecord, _ *IdempotencyMarker) {
			delete(task.Params, TaskConnectorNameParam)
		},
	}
	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			fixture := newConnectorDeletionFixture(t)
			connectors, err := newConnectorRepository(fixture.store)
			if err != nil {
				t.Fatalf("newConnectorRepository() error = %v", err)
			}
			task, marker, tombstone, intent := connectorDeletionTestTask(
				t,
				fixture.connector,
				fixture.project,
				fixture.environment,
				fixture.now.Add(7*time.Minute),
				2700,
			)
			task.Params = cloneStringMap(task.Params)
			mutate(&task, &marker)
			_, err = connectors.BeginConnectorDeletionWithTask(
				context.Background(), fixture.environment, fixture.project, fixture.connector,
				tombstone, intent, task, marker,
			)
			if !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
				t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
			}
		})
	}
}

// Rationale: a successful transaction followed by a transport failure has an
// unknown outcome, and an exact retry must recover the stored opaque response.
func TestConnectorDeletionPreservesUnknownOutcomeForExactReplay(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	fixture := newConnectorDeletionFixture(t)
	unknown := errs.New(errs.KindStorageUnavailable, "unknown connector deletion outcome")
	store := &backupPolicyReplacementUnknownStore{
		memoryHierarchyStore: fixture.store,
		failNext:             unknown,
	}
	connectors, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	task, marker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(8*time.Minute),
		2710,
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
	); !errors.Is(err, unknown) {
		t.Fatalf("BeginConnectorDeletionWithTask(unknown) error = %v", err)
	}
	replay, err := connectors.BeginConnectorDeletionWithTask(
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
	outcome, response, conflict, classifyErr := replay.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
		response.Response.Status != marker.Response.Status ||
		response.Response.ContentKind != marker.Response.ContentKind ||
		!bytes.Equal(response.Response.Body, marker.Response.Body) {
		t.Fatalf(
			"replay outcome/response/conflict/error = %v/%#v/%v/%v",
			outcome,
			response,
			conflict,
			classifyErr,
		)
	}
	assertConnectorDeletionKeyState(t, fixture, task, true, true)
}

// Rationale: absent or malformed durable ownership evidence is corruption,
// while a valid enabled-policy reference is the only legitimate use conflict.
func TestConnectorDeletionClassifiesCorruptionAndReferences(t *testing.T) {
	t.Parallel()
	tests := map[string]struct {
		mutate func(*testing.T, *connectorDeletionFixture)
		kind   errs.Kind
	}{
		"missing environment index": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionDeleteKey(t, fixture.store, connectorEnvironmentKey(
					fixture.environment.Record.ID, fixture.connector.Record.Connector.ID,
				))
			},
			kind: errs.KindInternal,
		},
		"corrupt name index": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionPutKey(t, fixture.store, connectorNameKey(
					fixture.environment.Record.ID, fixture.connector.Record.Connector.Name,
				), []byte("not-the-connector"))
			},
			kind: errs.KindInternal,
		},
		"missing credentials": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionDeleteKey(t, fixture.store, connectorCredentialValueKey(
					fixture.connector.Record.Connector.ID,
				))
			},
			kind: errs.KindInternal,
		},
		"missing environment owner": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionDeleteKey(
					t,
					fixture.store,
					environmentKey(fixture.environment.Record.ID),
				)
			},
			kind: errs.KindEnvironmentNotFound,
		},
		"enabled policy reference": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionPutKey(t, fixture.store, backupPolicyConnectorReferenceKey(
					fixture.connector.Record.Connector.ID, fixture.environment.Record.ID,
				), []byte(fixture.environment.Record.ID))
			},
			kind: errs.KindResourceInUse,
		},
		"foreign reference prefix": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				foreignEnvironmentID := ids.NewAt(ids.KindEnvironment, fixture.now, 2780)
				connectorDeletionPutKey(t, fixture.store, backupPolicyConnectorReferenceKey(
					fixture.connector.Record.Connector.ID, foreignEnvironmentID,
				), []byte(foreignEnvironmentID))
			},
			kind: errs.KindInternal,
		},
	}
	for name, test := range tests {
		name, test := name, test
		t.Run(name, func(t *testing.T) {
			fixture := newConnectorDeletionFixture(t)
			test.mutate(t, fixture)
			connectors, err := newConnectorRepository(fixture.store)
			if err != nil {
				t.Fatalf("newConnectorRepository() error = %v", err)
			}
			fixture.connector, err = connectors.GetConnector(
				context.Background(), fixture.connector.Record.Connector.ID,
			)
			if err != nil {
				t.Fatalf("GetConnector() error = %v", err)
			}
			task, marker, tombstone, intent := connectorDeletionTestTask(
				t,
				fixture.connector,
				fixture.project,
				fixture.environment,
				fixture.now.Add(9*time.Minute),
				2720,
			)
			result, err := connectors.BeginConnectorDeletionWithTask(
				context.Background(), fixture.environment, fixture.project, fixture.connector,
				tombstone, intent, task, marker,
			)
			if err == nil {
				_, _, conflict, classifyErr := result.Classify()
				if classifyErr != nil {
					err = classifyErr
				} else {
					err = conflict
				}
			}
			if !errors.Is(err, errs.New(test.kind, "")) {
				t.Fatalf("Connector deletion classified error = %v, want kind %v", err, test.kind)
			}
		})
	}
}

// Rationale: terminal replay must verify every deleted index and the retained
// stable replay target rather than trusting only the terminal Task status.
func TestConnectorDeletionTerminalReplayRejectsCorruptEvidence(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*testing.T, *connectorDeletionFixture, TaskRecord){
		"orphan environment index": func(t *testing.T, fixture *connectorDeletionFixture, task TaskRecord) {
			connectorDeletionPutKey(t, fixture.store, connectorEnvironmentKey(
				fixture.environment.Record.ID, task.Target,
			), []byte(task.Target))
		},
		"missing replay target": func(t *testing.T, fixture *connectorDeletionFixture, task TaskRecord) {
			connectorDeletionDeleteKey(t, fixture.store, connectorDeletionReplayTargetKey(t, task))
		},
	}
	for name, corrupt := range tests {
		name, corrupt := name, corrupt
		t.Run(name, func(t *testing.T) {
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
				fixture.now.Add(10*time.Minute),
				2730,
			)
			if _, err := connectors.BeginConnectorDeletionWithTask(
				ctx, fixture.environment, fixture.project, fixture.connector,
				tombstone, intent, task, marker,
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
			terminalAt := task.CreatedAt.Add(2 * time.Second)
			if _, err := tasks.AcknowledgeControllerTask(
				ctx,
				task.ID,
				TaskStatusCompleted,
				terminalAt,
			); err != nil {
				t.Fatalf("AcknowledgeControllerTask() error = %v", err)
			}
			corrupt(t, fixture, task)
			if _, err := tasks.AcknowledgeControllerTask(
				ctx, task.ID, TaskStatusCompleted, terminalAt,
			); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
				t.Fatalf("AcknowledgeControllerTask(replay) error = %v", err)
			}
		})
	}
}

// Rationale: a late retry owns a later Task marker retention epoch and must
// replay after the original DELETE replay-target index has been pruned.
func TestConnectorDeletionLateRetryReplaysAfterOriginalTargetPruned(t *testing.T) {
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
	original, marker, tombstone, intent := connectorDeletionTestTask(
		t,
		fixture.connector,
		fixture.project,
		fixture.environment,
		fixture.now.Add(11*time.Minute),
		2740,
	)
	if _, err := connectors.BeginConnectorDeletionWithTask(
		ctx, fixture.environment, fixture.project, fixture.connector,
		tombstone, intent, original, marker,
	); err != nil {
		t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx, original.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(original) found/error = %v/%v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(
		ctx, original.ID, TaskStatusFailed, original.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask(original) error = %v", err)
	}

	retryAt := original.CreatedAt.Add(89 * 24 * time.Hour)
	retryID := ids.NewAt(ids.KindTask, retryAt, 2743)
	retryMarker := pendingRetryMarker(
		failed.Record,
		retryID,
		retryAt,
		"connector-late-retry-key-0001",
	)
	if _, err := tasks.RetryTask(
		ctx,
		original.ID,
		retryID,
		TaskActorOperator,
		retryMarker,
	); err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, retryAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextControllerTask(retry) = %#v/%v/%v", claim, found, err)
	}
	terminalAt := retryAt.Add(2 * time.Second)
	terminal, err := tasks.AcknowledgeControllerTask(
		ctx, retryID, TaskStatusCompleted, terminalAt,
	)
	if err != nil || terminal.Record.RetryOf != original.ID {
		t.Fatalf("AcknowledgeControllerTask(retry) = %#v/%v", terminal, err)
	}
	retryMarkerKey, err := idempotencyMarkerKey(retryMarker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(retry) error = %v", err)
	}
	if mustOptionalKey(t, fixture.store, retryMarkerKey) == nil {
		t.Fatal("retry attempt marker is missing")
	}
	connectorDeletionDeleteKey(t, fixture.store, connectorDeletionReplayTargetKey(t, original))
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, retryID, TaskStatusCompleted, terminalAt,
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(retry replay) error = %v", err)
	}
	if mustOptionalKey(t, fixture.store, retryMarkerKey) == nil {
		t.Fatal("retry attempt marker was lost during replay")
	}
}

func connectorDeletionTestTask(
	t *testing.T,
	current Versioned[ConnectorRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	createdAt time.Time,
	entropy int64,
) (TaskRecord, IdempotencyMarker, DeletionTombstoneRecord, ConnectorRemovalIntent) {
	task := validTaskRecord(createdAt)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, entropy)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, entropy+1)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, entropy+2)
	task.Executor = TaskExecutorController
	task.Type = TaskRemove
	task.Target = current.Record.Connector.ID
	task.Params = map[string]string{
		TaskResourceKindParam:         TaskResourceConnector,
		TaskConnectorEnvironmentParam: environment.Record.ID,
		TaskConnectorNameParam:        current.Record.Connector.Name,
	}
	task.TimeoutSeconds = connectorDeletionTimeoutSeconds
	task.IdempotencyKey = "connector-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: connectorDeletionRoute, Key: task.IdempotencyKey,
	}
	target := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetConnector, ID: task.Target}
	marker.ReplayTarget = &target
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetConnector, TargetID: task.Target,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: DeletionPhaseFinalizing,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	intent, err := NewConnectorRemovalIntent(
		task.ID, environment.Record.ID, task.Target, current.Revision, createdAt,
	)
	if err != nil {
		panic(err)
	}
	return task, marker, tombstone, intent
}

type connectorDeletionLockedStore struct {
	store *memoryHierarchyStore
	mutex sync.Mutex
}

func (store *connectorDeletionLockedStore) Get(
	ctx context.Context,
	key string,
) (*GetResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.Get(ctx, key)
}

func (store *connectorDeletionLockedStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.GetMany(ctx, request)
}

func (store *connectorDeletionLockedStore) Range(
	ctx context.Context,
	request RangeRequest,
) (*RangeResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.Range(ctx, request)
}

func (store *connectorDeletionLockedStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.Transact(ctx, conditions, mutations)
}

type connectorDeletionFixture struct {
	repository  *BackupPolicyRepository
	store       *memoryHierarchyStore
	environment Versioned[EnvironmentRecord]
	project     Versioned[ProjectRecord]
	source      Versioned[BackupSourceRecord]
	connector   Versioned[ConnectorRecord]
	now         time.Time
}

func newConnectorDeletionFixture(t *testing.T) *connectorDeletionFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 23, 18, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 2600)
	if _, err := hierarchy.CreateTenant(ctx, TenantRecord{
		ID: tenantID, Slug: "sample-tenant", Name: "Sample Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 2601), TenantID: tenantID,
		Slug: "sample-project", Name: "Sample Project", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 2602)
	environment, err := hierarchy.CreateEnvironment(ctx, EnvironmentRecord{
		ID:                environmentID,
		ProjectID:         project.Record.ID,
		Name:              "production",
		NetworkPool:       "10.242.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 2603),
		CreatedAt:         now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	repository, err := newBackupPolicyRepository(store)
	if err != nil {
		t.Fatalf("newBackupPolicyRepository() error = %v", err)
	}
	repository.now = func() time.Time { return now }
	configSource, err := repository.EnsureBackupSource(
		ctx, environment, project, "config", environment.Record.ID,
	)
	if err != nil {
		t.Fatalf("EnsureBackupSource(config) error = %v", err)
	}
	fixture := &connectorDeletionFixture{
		repository:  repository,
		store:       store,
		environment: environment,
		project:     project,
		source:      configSource,
		now:         now,
	}
	record := testConnectorRecord(t, environment.Record.ID, now, 2610, "primary-backups")
	credentials, err := NewConnectorEncryptedCredentials(
		record.Connector.ID, []byte("sealed-connector-credentials"),
	)
	if err != nil {
		t.Fatalf("NewConnectorEncryptedCredentials() error = %v", err)
	}
	connectors, err := newConnectorRepository(store)
	if err != nil {
		t.Fatalf("newConnectorRepository() error = %v", err)
	}
	fixture.connector, err = connectors.CreateConnector(
		ctx, environment, project, record, credentials,
	)
	if err != nil {
		t.Fatalf("CreateConnector() error = %v", err)
	}
	return fixture
}

func connectorDeletionPolicyCandidate(
	t *testing.T,
	fixture *connectorDeletionFixture,
) backupPolicyReplacementCandidate {
	t.Helper()
	identity, err := age.GenerateX25519Identity()
	if err != nil {
		t.Fatalf("age.GenerateX25519Identity() error = %v", err)
	}
	return backupPolicyReplacementCandidate{
		Environment: fixture.environment,
		Project:     fixture.project,
		MutationEpoch: mustBackupPolicyMutationEpoch(
			t,
			fixture.store,
			fixture.environment.Record.ID,
		),
		Replacement: BackupPolicyRecord{
			EnvironmentID: fixture.environment.Record.ID,
			Enabled:       true,
			Frequency:     "*-*-* 03:00:00",
			Keep:          7,
			Encryption:    "age",
			ConnectorID:   fixture.connector.Record.Connector.ID,
			SourceIDs:     []string{fixture.source.Record.ID},
			UpdatedAt:     fixture.now,
		},
		Sources: []backupPolicySourceEvidence{{
			Source: fixture.source,
			EnvironmentIndex: connectorDeletionRequiredKey(
				t,
				fixture.store,
				backupSourceEnvironmentKey(fixture.environment.Record.ID, fixture.source.Record.ID),
			),
			IdentityIndex: connectorDeletionRequiredKey(
				t,
				fixture.store,
				backupSourceIdentityKey(
					fixture.environment.Record.ID,
					fixture.source.Record.Kind,
					fixture.source.Record.TargetID,
				),
			),
		}},
		Connector: &fixture.connector,
		ConnectorOwnerIndex: connectorDeletionRequiredKey(
			t,
			fixture.store,
			connectorEnvironmentKey(
				fixture.environment.Record.ID, fixture.connector.Record.Connector.ID,
			),
		),
		ConnectorReferences: []backupPolicyConnectorReferenceEvidence{{
			ConnectorID: fixture.connector.Record.Connector.ID,
		}},
		InitialKey: &backupPolicyInitialKey{
			Record: BackupKeyRecord{
				EnvironmentID: fixture.environment.Record.ID,
				Recipient:     identity.Recipient().String(), KeyEra: 1,
				CreatedAt: fixture.now, RotatedAt: fixture.now,
			},
			Encrypted: BackupKeyEncryptedValue{
				EnvironmentID: fixture.environment.Record.ID,
				KeyEra:        1, Ciphertext: []byte("controller-sealed-age-identity"),
			},
		},
	}
}

func connectorDeletionRequiredKey(
	t *testing.T,
	store *memoryHierarchyStore,
	key string,
) *KeyValue {
	t.Helper()
	entry := mustOptionalKey(t, store, key)
	if entry == nil {
		t.Fatalf("required key %s is missing", key)
	}
	return entry
}

func assertConnectorDeletionKeyState(
	t *testing.T,
	fixture *connectorDeletionFixture,
	task TaskRecord,
	targetPresent bool,
	fencePresent bool,
) {
	t.Helper()
	targetKeys := []string{
		connectorRecordKey(task.Target),
		connectorEnvironmentKey(fixture.environment.Record.ID, task.Target),
		connectorNameKey(fixture.environment.Record.ID, fixture.connector.Record.Connector.Name),
		connectorCredentialValueKey(task.Target),
	}
	for _, key := range targetKeys {
		entry := mustOptionalKey(t, fixture.store, key)
		if (entry != nil) != targetPresent {
			t.Fatalf("target key %s present = %t, want %t", key, entry != nil, targetPresent)
		}
	}
	fenceKeys := []string{
		deletionTombstoneKey(string(DeletionTargetConnector), task.Target),
		connectorRemovalIntentKey(task.ID),
	}
	for _, key := range fenceKeys {
		entry := mustOptionalKey(t, fixture.store, key)
		if (entry != nil) != fencePresent {
			t.Fatalf("fence key %s present = %t, want %t", key, entry != nil, fencePresent)
		}
	}
	replay := mustOptionalKey(t, fixture.store, connectorDeletionReplayTargetKey(t, task))
	if replay == nil {
		t.Fatal("connector deletion replay target is missing")
	}
	markerKey, err := idempotencyMarkerKey(IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment,
		ScopeID:   fixture.environment.Record.ID,
		Method:    http.MethodDelete,
		Route:     connectorDeletionRoute,
		Key:       task.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	if err := decodeReplayTargetReference(replay.Value, markerKey); err != nil {
		t.Fatalf("decodeReplayTargetReference() error = %v", err)
	}
}

func connectorDeletionReplayTargetKey(t *testing.T, task TaskRecord) string {
	t.Helper()
	key, err := idempotencyReplayTargetKey(
		IdempotencyReplayTarget{Kind: IdempotencyReplayTargetConnector, ID: task.Target},
		http.MethodDelete,
		connectorDeletionRoute,
		task.IdempotencyKey,
	)
	if err != nil {
		t.Fatalf("idempotencyReplayTargetKey() error = %v", err)
	}
	return key
}

func connectorDeletionPutKey(t *testing.T, store *memoryHierarchyStore, key string, value []byte) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationPut, Key: key, Value: value,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("put %s result/error = %#v/%v", key, result, err)
	}
}

func connectorDeletionDeleteKey(t *testing.T, store *memoryHierarchyStore, key string) {
	t.Helper()
	result, err := store.Transact(context.Background(), nil, []Mutation{{
		Type: MutationDelete, Key: key,
	}})
	if err != nil || !result.Succeeded {
		t.Fatalf("delete %s result/error = %#v/%v", key, result, err)
	}
}

func assertConnectorDeletionVisible(
	t *testing.T,
	repository *ConnectorRepository,
	connectorID string,
) {
	t.Helper()
	current, err := repository.GetConnector(context.Background(), connectorID)
	if err != nil || current.Record.Connector.ID != connectorID {
		t.Fatalf("GetConnector(visible) = %#v/%v", current, err)
	}
}

func assertConnectorCredentialPresence(
	t *testing.T,
	store *memoryHierarchyStore,
	connectorID string,
	want bool,
) {
	t.Helper()
	entry := mustOptionalKey(t, store, connectorCredentialValueKey(connectorID))
	if (entry != nil) != want {
		t.Fatalf("Connector credential present = %t, want %t", entry != nil, want)
	}
}

func assertConnectorIntentPresence(
	t *testing.T,
	store *memoryHierarchyStore,
	taskID string,
	want bool,
) {
	t.Helper()
	entry := mustOptionalKey(t, store, connectorRemovalIntentKey(taskID))
	if (entry != nil) != want {
		t.Fatalf("Connector removal intent present = %t, want %t", entry != nil, want)
	}
}

func mustOptionalKey(t *testing.T, store *memoryHierarchyStore, key string) *KeyValue {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil || result == nil {
		t.Fatalf("Get(%s) = %#v/%v", key, result, err)
	}
	return result.Entry
}
