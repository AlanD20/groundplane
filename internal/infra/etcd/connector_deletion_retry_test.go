package etcd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	testbackuppolicy "github.com/AlanD20/groundplane/internal/infra/etcd/backuppolicy"
	testbackupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	testconnectors "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: non-success terminal acknowledgements do not delete the
// Connector, so they must release cleanup ownership even when an artifact
// reference appeared after deletion publication.
func TestConnectorDeletionNonDeletingTerminalStatusesReleaseCleanupWithReferences(t *testing.T) {
	for _, terminalStatus := range []testtaskjournal.TaskStatus{testtaskjournal.TaskStatusFailed, testtaskjournal.TaskStatusTimedOut, testtaskjournal.TaskStatusAborted} {
		terminalStatus := terminalStatus
		t.Run(string(terminalStatus), func(t *testing.T) {
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
			seed := int64(2710)
			switch terminalStatus {
			case testtaskjournal.TaskStatusTimedOut:
				seed = 2720
			case testtaskjournal.TaskStatusAborted:
				seed = 2730
			}
			task, marker, tombstone, intent := connectorDeletionTestTask(
				t,
				fixture.connector,
				fixture.project,
				fixture.environment,
				fixture.now.Add(9*time.Minute),
				seed,
			)
			if _, err := connectors.BeginConnectorDeletionWithTask(
				ctx, fixture.environment, fixture.project, fixture.connector, tombstone, intent, task, marker,
			); err != nil {
				t.Fatalf("BeginConnectorDeletionWithTask() error = %v", err)
			}

			deadline := time.Time{}
			if terminalStatus != testtaskjournal.TaskStatusAborted {
				claim, found, claimErr := tasks.ClaimNextControllerTask(ctx, task.CreatedAt.Add(time.Second))
				if claimErr != nil || !found {
					t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, claimErr)
				}
				deadline = claim.Assignment.Record.Deadline
			}

			pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, seed+1)
			referenceKey, keyErr := testbackupruntime.BackupRecoveryPointConnectorIndexKey(task.Target, pointID)
			if keyErr != nil {
				t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", keyErr)
			}
			connectorDeletionPutKey(t, fixture.store, referenceKey, []byte(pointID))

			var terminal testkeyvalue.Versioned[TaskRecord]
			switch terminalStatus {
			case testtaskjournal.TaskStatusFailed:
				terminal, err = tasks.AcknowledgeControllerTask(
					ctx, task.ID, terminalStatus, task.CreatedAt.Add(2*time.Second),
				)
			case testtaskjournal.TaskStatusTimedOut:
				var expired int
				expired, err = tasks.ExpireTimedOutTasks(ctx, deadline.Add(time.Second))
				if err == nil && expired != 1 {
					t.Fatalf("ExpireTimedOutTasks() = %d, want 1", expired)
				}
				if err == nil {
					terminal, err = tasks.GetTask(ctx, task.ID)
				}
			case testtaskjournal.TaskStatusAborted:
				terminal, err = tasks.AbortPendingTask(ctx, task.ID, task.CreatedAt.Add(2*time.Second))
			default:
				t.Fatalf("unexpected terminal status %s", terminalStatus)
			}
			if err != nil || terminal.Record.Status != terminalStatus {
				t.Fatalf("terminal Connector deletion = %#v/%v", terminal, err)
			}
			assertConnectorDeletionVisible(t, connectors, task.Target)
			assertConnectorCredentialPresence(t, fixture.store, task.Target, true)
			assertConnectorDeletionKeyState(t, fixture, task, true, false)
			if mustOptionalKey(t, fixture.store, referenceKey) == nil {
				t.Fatal("Connector artifact reference was lost during non-deleting cleanup")
			}
		})
	}
}

// Rationale: protected Connector deletion must reject any durable Task or
// marker shape that would make retry or post-delete replay ambiguous.
func TestConnectorDeletionRejectsMalformedTaskAndMarker(t *testing.T) {
	t.Parallel()
	tests := map[string]func(*TaskRecord, *testidempotency.IdempotencyMarker){
		"missing task key": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.IdempotencyKey = ""
		},
		"mismatched task key": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.IdempotencyKey = "connector-remove-key-0002"
		},
		"wrong timeout": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			task.TimeoutSeconds++
		},
		"wrong method": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Locator.Method = http.MethodPost
		},
		"wrong route": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Locator.Route = "/connectors"
		},
		"wrong response status": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Response.Status = http.StatusOK
		},
		"wrong response content kind": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Response.ContentKind = "text/plain"
		},
		"wrong response body": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.Response.Body = []byte("{}")
		},
		"missing replay target": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.ReplayTarget = nil
		},
		"wrong replay target": func(_ *TaskRecord, marker *testidempotency.IdempotencyMarker) {
			marker.ReplayTarget = &testidempotency.IdempotencyReplayTarget{
				Kind: testidempotency.IdempotencyReplayTargetConnector,
				ID:   ids.NewAt(ids.KindConnector, marker.CreatedAt, 2790),
			}
		},
		"missing environment pin": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
			delete(task.Params, TaskConnectorEnvironmentParam)
		},
		"missing name pin": func(task *TaskRecord, _ *testidempotency.IdempotencyMarker) {
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
				connectorDeletionDeleteKey(t, fixture.store, testconnectors.ConnectorEnvironmentKey(
					fixture.environment.Record.ID, fixture.connector.Record.Connector.ID,
				))
			},
			kind: errs.KindInternal,
		},
		"corrupt name index": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionPutKey(t, fixture.store, testconnectors.ConnectorNameKey(
					fixture.environment.Record.ID, fixture.connector.Record.Connector.Name,
				), []byte("not-the-connector"))
			},
			kind: errs.KindInternal,
		},
		"missing credentials": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionDeleteKey(t, fixture.store, testconnectors.CredentialValueKey(
					fixture.connector.Record.Connector.ID,
				))
			},
			kind: errs.KindInternal,
		},
		"missing environment owner": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionDeleteKey(
					t,
					fixture.store, testhierarchy.EnvironmentKey(fixture.environment.Record.ID),
				)
			},
			kind: errs.KindEnvironmentNotFound,
		},
		"enabled policy reference": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				connectorDeletionPutKey(t, fixture.store, testbackuppolicy.BackupPolicyConnectorReferenceKey(
					fixture.connector.Record.Connector.ID, fixture.environment.Record.ID,
				), []byte(fixture.environment.Record.ID))
			},
			kind: errs.KindResourceInUse,
		},
		"Recovery Point reference": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2781)
				key, err := testbackupruntime.BackupRecoveryPointConnectorIndexKey(
					fixture.connector.Record.Connector.ID,
					pointID,
				)
				if err != nil {
					t.Fatalf("backupRecoveryPointConnectorIndexKey() error = %v", err)
				}
				connectorDeletionPutKey(t, fixture.store, key, []byte(pointID))
			},
			kind: errs.KindResourceInUse,
		},
		"orphan reference": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				pointID := ids.NewAt(ids.KindRecoveryPoint, fixture.now, 2782)
				key, err := testbackupruntime.BackupOrphanConnectorIndexKey(
					fixture.connector.Record.Connector.ID,
					pointID,
				)
				if err != nil {
					t.Fatalf("backupOrphanConnectorIndexKey() error = %v", err)
				}
				connectorDeletionPutKey(t, fixture.store, key, []byte(pointID))
			},
			kind: errs.KindResourceInUse,
		},
		"foreign reference prefix": {
			mutate: func(t *testing.T, fixture *connectorDeletionFixture) {
				foreignEnvironmentID := ids.NewAt(ids.KindEnvironment, fixture.now, 2780)
				connectorDeletionPutKey(t, fixture.store, testbackuppolicy.BackupPolicyConnectorReferenceKey(
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
			connectorDeletionPutKey(t, fixture.store, testconnectors.ConnectorEnvironmentKey(
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
				task.ID, testtaskjournal.TaskStatusCompleted, terminalAt,
			); err != nil {
				t.Fatalf("AcknowledgeControllerTask() error = %v", err)
			}
			corrupt(t, fixture, task)
			if _, err := tasks.AcknowledgeControllerTask(
				ctx, task.ID, testtaskjournal.TaskStatusCompleted, terminalAt,
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
		ctx, original.ID, testtaskjournal.TaskStatusFailed, original.CreatedAt.Add(2*time.Second),
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
		retryID, testtaskjournal.TaskActorOperator, retryMarker,
	); err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	claim, found, err := tasks.ClaimNextControllerTask(ctx, retryAt.Add(time.Second))
	if err != nil || !found || claim.Task.Record.ID != retryID {
		t.Fatalf("ClaimNextControllerTask(retry) = %#v/%v/%v", claim, found, err)
	}
	terminalAt := retryAt.Add(2 * time.Second)
	terminal, err := tasks.AcknowledgeControllerTask(
		ctx, retryID, testtaskjournal.TaskStatusCompleted, terminalAt,
	)
	if err != nil || terminal.Record.RetryOf != original.ID {
		t.Fatalf("AcknowledgeControllerTask(retry) = %#v/%v", terminal, err)
	}
	retryMarkerKey, err := testidempotency.IdempotencyMarkerKey(retryMarker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(retry) error = %v", err)
	}
	if mustOptionalKey(t, fixture.store, retryMarkerKey) == nil {
		t.Fatal("retry attempt marker is missing")
	}
	connectorDeletionDeleteKey(t, fixture.store, connectorDeletionReplayTargetKey(t, original))
	if _, err := tasks.AcknowledgeControllerTask(
		ctx, retryID, testtaskjournal.TaskStatusCompleted, terminalAt,
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(retry replay) error = %v", err)
	}
	if mustOptionalKey(t, fixture.store, retryMarkerKey) == nil {
		t.Fatal("retry attempt marker was lost during replay")
	}
}

func connectorDeletionTestTask(
	t *testing.T,
	current testkeyvalue.Versioned[testconnectors.Record],
	project testkeyvalue.Versioned[testhierarchy.ProjectRecord],
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord],
	createdAt time.Time,
	entropy int64,
) (TaskRecord, testidempotency.IdempotencyMarker, testdeletions.DeletionTombstoneRecord, testconnectors.RemovalIntent) {
	task := validTaskRecord(createdAt)
	task.Owner = mustEnvironmentTaskOwner(t, project.Record, environment.Record)
	task.ID = ids.NewAt(ids.KindTask, createdAt, entropy)
	task.OperationID = ids.NewAt(ids.KindOperation, createdAt, entropy+1)
	task.PlanID = ids.NewAt(ids.KindPlan, createdAt, entropy+2)
	task.Executor = testtaskjournal.TaskExecutorController
	task.Type = testtaskjournal.TaskRemove
	task.Target = current.Record.Connector.ID
	task.Params = map[string]string{
		testtaskjournal.TaskResourceKindParam: testtaskjournal.TaskResourceConnector,
		TaskConnectorEnvironmentParam:         environment.Record.ID,
		TaskConnectorNameParam:                current.Record.Connector.Name,
	}
	task.TimeoutSeconds = connectorDeletionTimeoutSeconds
	task.IdempotencyKey = "connector-remove-key-0001"
	marker := pendingTaskMarker(task)
	marker.Locator = testidempotency.IdempotencyLocator{
		ScopeKind: testidempotency.IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: connectorDeletionRoute, Key: task.IdempotencyKey,
	}
	target := testidempotency.IdempotencyReplayTarget{
		Kind: testidempotency.IdempotencyReplayTargetConnector,
		ID:   task.Target,
	}
	marker.ReplayTarget = &target
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetConnector, TargetID: task.Target,
		TargetRevision: current.Revision, TaskID: task.ID, Phase: testdeletions.DeletionPhaseFinalizing,
		CreatedAt: createdAt, UpdatedAt: createdAt,
	}
	intent, err := testconnectors.NewRemovalIntent(
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

type connectorReferenceRaceStore struct {
	*memoryHierarchyStore
	referenceKey   string
	referenceValue []byte
	injected       bool
}

func (store *connectorReferenceRaceStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if !store.injected && connectorReferenceConditionContains(conditions, store.referenceKey) {
		store.injected = true
		if _, err := store.memoryHierarchyStore.Transact(ctx, nil, []testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: store.referenceKey, Value: store.referenceValue,
		}}); err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
	}
	for _, condition := range conditions {
		if !condition.Prefix {
			continue
		}
		result, err := store.memoryHierarchyStore.Range(ctx, testkeyvalue.RangeRequest{
			Prefix: condition.Key, Limit: 1,
		})
		if err != nil {
			return testkeyvalue.TransactionResult{}, err
		}
		if len(result.Values) != 0 {
			failureReads, err := connectorReferenceFailureReads(ctx, store.memoryHierarchyStore, conditions)
			if err != nil {
				return testkeyvalue.TransactionResult{}, err
			}
			return testkeyvalue.TransactionResult{
				Succeeded: false, Revision: result.ResponseRevision, FailureReads: failureReads,
			}, nil
		}
	}
	return store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
}

func connectorReferenceConditionContains(conditions []testkeyvalue.Condition, key string) bool {
	for _, condition := range conditions {
		if condition.Prefix && strings.HasPrefix(key, condition.Key) {
			return true
		}
	}
	return false
}

func connectorReferenceFailureReads(
	ctx context.Context,
	store *memoryHierarchyStore,
	conditions []testkeyvalue.Condition,
) ([]*testkeyvalue.KeyValue, error) {
	values := make([]*testkeyvalue.KeyValue, len(conditions))
	for index, condition := range conditions {
		if condition.Prefix {
			result, err := store.Range(ctx, testkeyvalue.RangeRequest{Prefix: condition.Key, Limit: 1})
			if err != nil {
				return nil, err
			}
			if len(result.Values) != 0 {
				value := result.Values[0]
				values[index] = &value
			}
			continue
		}
		result, err := store.Get(ctx, condition.Key)
		if err != nil {
			return nil, err
		}
		values[index] = result.Entry
	}
	return values, nil
}

func (store *connectorDeletionLockedStore) Get(
	ctx context.Context,
	key string,
) (*testkeyvalue.GetResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.Get(ctx, key)
}

func (store *connectorDeletionLockedStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.GetMany(ctx, request)
}

func (store *connectorDeletionLockedStore) Range(
	ctx context.Context,
	request testkeyvalue.RangeRequest,
) (*testkeyvalue.RangeResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.Range(ctx, request)
}

func (store *connectorDeletionLockedStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	store.mutex.Lock()
	defer store.mutex.Unlock()
	return store.store.Transact(ctx, conditions, mutations)
}

type connectorDeletionFixture struct {
	repository  *BackupPolicyRepository
	store       *memoryHierarchyStore
	environment testkeyvalue.Versioned[testhierarchy.EnvironmentRecord]
	project     testkeyvalue.Versioned[testhierarchy.ProjectRecord]
	source      testkeyvalue.Versioned[testbackuppolicy.BackupSourceRecord]
	connector   testkeyvalue.Versioned[testconnectors.Record]
	now         time.Time
}
