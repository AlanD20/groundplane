package etcd

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: Environment deletion must lose before publishing host effects when
// another persistence operation already owns the canonical lock.
func TestEnvironmentDeletionRejectsHeldOperationLock(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	other := BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 8010),
		TaskID:        ids.NewAt(ids.KindTask, fixture.now, 8011),
		Kind:          BackupOperationBackup,
		CreatedAt:     fixture.now,
		UpdatedAt:     fixture.now,
	}
	fixture.putLock(t, other)
	result, err := fixture.begin()
	if err != nil {
		t.Fatalf("BeginEnvironmentDeletionWithTask(held lock) error = %v", err)
	}
	_, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || !isKind(conflict, errs.KindResourceInUse) {
		t.Fatalf("BeginEnvironmentDeletionWithTask(held lock) = %#v, %v", result, err)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(fixture.task.ID), false)
}

// Rationale: stale and corrupt mutation epochs must fail closed instead of
// publishing a deletion Task against persistence evidence from another epoch.
func TestEnvironmentDeletionRejectsStaleAndCorruptMutationEpoch(t *testing.T) {
	t.Parallel()
	t.Run("stale", func(t *testing.T) {
		fixture := newEnvironmentDeletionLockFixture(t)
		fixture.rewriteEpoch(t)
		result, err := fixture.begin()
		if err != nil {
			t.Fatalf("BeginEnvironmentDeletionWithTask(stale) error = %v", err)
		}
		_, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || !isKind(conflict, errs.KindStateConflict) {
			t.Fatalf("stale epoch classification = %#v/%v", result, classifyErr)
		}
	})

	t.Run("corrupt", func(t *testing.T) {
		fixture := newEnvironmentDeletionLockFixture(t)
		fixture.putRaw(t, environmentMutationEpochKey(fixture.environment.Record.ID), []byte("corrupt"))
		current, err := fixture.hierarchy.GetEnvironment(context.Background(), fixture.environment.Record.ID)
		if err != nil {
			t.Fatalf("GetEnvironment() error = %v", err)
		}
		fixture.environment = current
		result, err := fixture.begin()
		if !isKind(err, errs.KindInternal) {
			t.Fatalf("BeginEnvironmentDeletionWithTask(corrupt) = %#v, %v", result, err)
		}
	})
}

// Rationale: every destructive Blueprint batch must retain exact deletion lock
// ownership and advance the Environment mutation epoch in the same transaction.
func TestEnvironmentDeletionBlueprintBatchFencesOwnedLockAndAdvancesEpoch(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	revisionID := ids.NewAt(ids.KindTask, fixture.now, 8020)
	fixture.putRaw(
		t,
		environmentBlueprintManifestKey(fixture.environment.Record.ID, revisionID),
		[]byte("revision"),
	)
	other := BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   ids.NewAt(ids.KindOperation, fixture.now, 8021),
		TaskID:        ids.NewAt(ids.KindTask, fixture.now, 8022),
		Kind:          BackupOperationRestore,
		CreatedAt:     fixture.now,
		UpdatedAt:     fixture.now,
	}
	fixture.putLock(t, other)
	if _, err := fixture.tasks.finalizeEnvironmentBlueprintRevisionBatch(
		context.Background(), fixture.task, fixture.now.Add(time.Second),
	); !isKind(err, errs.KindStateConflict) {
		t.Fatalf("finalizeEnvironmentBlueprintRevisionBatch(other owner) error = %v", err)
	}
	owned := BackupOperationLockRecord{
		EnvironmentID: fixture.environment.Record.ID,
		OperationID:   fixture.task.OperationID,
		TaskID:        fixture.task.ID,
		Kind:          BackupOperationDeletion,
		CreatedAt:     fixture.task.CreatedAt,
		UpdatedAt:     fixture.task.CreatedAt,
	}
	fixture.putLock(t, owned)
	before := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
	changed, err := fixture.tasks.finalizeEnvironmentBlueprintRevisionBatch(
		context.Background(), fixture.task, fixture.now.Add(2*time.Second),
	)
	if err != nil || !changed {
		t.Fatalf("finalizeEnvironmentBlueprintRevisionBatch() = %t, %v", changed, err)
	}
	after := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
	if after.ModRevision <= before.ModRevision {
		t.Fatalf("mutation epoch revision = %d, want > %d", after.ModRevision, before.ModRevision)
	}
	assertEnvironmentDeletionCompanion(
		t, fixture.store, environmentBlueprintManifestKey(fixture.environment.Record.ID, revisionID), false,
	)
}

// Rationale: every terminal outcome must release the exact deletion lock;
// success removes the Environment epoch while non-success advances and retains it.
func TestEnvironmentDeletionTerminalOutcomesCleanCompanionsAndReplay(t *testing.T) {
	t.Parallel()
	for _, terminalStatus := range []TaskStatus{TaskStatusCompleted, TaskStatusFailed, TaskStatusAborted} {
		t.Run(string(terminalStatus), func(t *testing.T) {
			fixture := newEnvironmentDeletionLockFixture(t)
			fixture.mustBegin(t)
			agentID := ids.NewAt(ids.KindAgent, fixture.now, 8030)
			var terminal Versioned[TaskRecord]
			var err error
			if terminalStatus == TaskStatusAborted {
				terminal, err = fixture.tasks.AbortPendingTask(
					context.Background(), fixture.task.ID, fixture.now.Add(time.Second),
				)
			} else {
				if _, found, claimErr := fixture.tasks.ClaimNextTask(
					context.Background(), agentID, 1, fixture.now.Add(time.Second),
				); claimErr != nil || !found {
					t.Fatalf("ClaimNextTask() found/error = %t/%v", found, claimErr)
				}
				result := TaskResultRecord{
					Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone,
				}
				if terminalStatus == TaskStatusFailed {
					result.ExitCode = 1
				}
				terminal, err = fixture.tasks.AcknowledgeTask(
					context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, fixture.tasks,
						fixture.task.ID),
					terminalStatus, result,
					fixture.now.Add(2*time.Second))

			}
			if err != nil {
				t.Fatalf("terminalize Environment deletion error = %v", err)
			}
			assertEnvironmentDeletionCompanion(
				t, fixture.store, environmentOperationLockKey(fixture.environment.Record.ID), false,
			)
			assertEnvironmentDeletionCompanion(
				t, fixture.store,
				deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID), false,
			)
			if terminalStatus == TaskStatusCompleted {
				assertEnvironmentDeletionCompanion(
					t, fixture.store, environmentKey(fixture.environment.Record.ID), false,
				)
				assertEnvironmentDeletionCompanion(
					t, fixture.store, environmentMutationEpochKey(fixture.environment.Record.ID), false,
				)
			} else {
				assertEnvironmentDeletionCompanion(
					t, fixture.store, environmentKey(fixture.environment.Record.ID), true,
				)
				epoch := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
				if epoch.ModRevision != terminal.Revision {
					t.Fatalf("terminal epoch revision = %d, want %d", epoch.ModRevision, terminal.Revision)
				}
			}
			var replay Versioned[TaskRecord]
			if terminalStatus == TaskStatusAborted {
				replay, err = fixture.tasks.AbortPendingTask(
					context.Background(), fixture.task.ID, fixture.now.Add(3*time.Second),
				)
			} else {
				replay, err = fixture.tasks.AcknowledgeTask(
					context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, fixture.tasks,
						fixture.task.ID),

					terminalStatus, *terminal.Record.Result, fixture.now.Add(2*time.Second))

			}
			if err != nil || replay.Revision != terminal.Revision {
				t.Fatalf("terminal replay = %#v, %v", replay, err)
			}
		})
	}
}

// Rationale: when etcd commits terminal cleanup but the response is lost, the
// exact acknowledgement retry must validate companion cleanup and return replay.
func TestEnvironmentDeletionTerminalUnknownOutcomeReplaysCommittedCleanup(t *testing.T) {
	t.Parallel()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	agentID := ids.NewAt(ids.KindAgent, fixture.now, 8040)
	if _, found, err := fixture.tasks.ClaimNextTask(
		context.Background(), agentID, 1, fixture.now.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %t/%v", found, err)
	}
	unknown := errs.New(errs.KindStorageUnavailable, "unknown Environment deletion outcome")
	failingStore := &environmentDeletionUnknownStore{memoryHierarchyStore: fixture.store, failNext: unknown}
	failingTasks, err := newTaskRepository(failingStore)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	result := TaskResultRecord{Kind: TaskResultEnvironmentDirectory, Diagnostic: TaskResultDiagnosticNone}
	terminalAt := fixture.now.Add(2 * time.Second)
	if _, err := failingTasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, failingTasks,
			fixture.task.ID),
		TaskStatusCompleted, result, terminalAt); !errors.Is(err, unknown) {
		t.Fatalf("AcknowledgeTask(unknown) error = %v", err)
	}
	replay, err := fixture.tasks.AcknowledgeTask(
		context.Background(), agentID, 1, fixture.task.ID, taskAssignmentIDForTest(t, fixture.tasks,
			fixture.task.ID),
		TaskStatusCompleted, result, terminalAt)

	if err != nil || replay.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeTask(after unknown) = %#v, %v", replay, err)
	}
}

type environmentDeletionLockFixture struct {
	store       *memoryHierarchyStore
	hierarchy   *HierarchyRepository
	tasks       *TaskRepository
	project     Versioned[ProjectRecord]
	environment Versioned[EnvironmentRecord]
	task        TaskRecord
	tombstone   DeletionTombstoneRecord
	marker      IdempotencyMarker
	now         time.Time
}

func newEnvironmentDeletionLockFixture(t *testing.T) *environmentDeletionLockFixture {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 8, 24, 18, 0, 0, 0, time.UTC)
	store := newMemoryHierarchyStore()
	hierarchy, err := newHierarchyRepository(store)
	if err != nil {
		t.Fatalf("newHierarchyRepository() error = %v", err)
	}
	tenantID := ids.NewAt(ids.KindTenant, now, 8000)
	if _, err := hierarchy.CreateTenant(ctx, TenantRecord{
		ID: tenantID, Slug: "deletion-tenant", Name: "Deletion Tenant",
	}); err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	project, err := hierarchy.CreateProject(ctx, ProjectRecord{
		ID: ids.NewAt(ids.KindProject, now, 8001), TenantID: tenantID,
		Slug: "deletion-project", Name: "Deletion Project", Kind: ProjectKindTenant,
	})
	if err != nil {
		t.Fatalf("CreateProject() error = %v", err)
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 8002)
	environment, err := hierarchy.CreateEnvironment(ctx, EnvironmentRecord{
		ID: environmentID, ProjectID: project.Record.ID,
		Name: "production", NetworkPool: "10.248.0.0/24",
		VolumeDir:         "/var/lib/groundplane/vol/" + tenantID + "/" + project.Record.ID + "/" + environmentID,
		ProvisioningState: EnvironmentProvisioningReady,
		CreateTaskID:      ids.NewAt(ids.KindTask, now, 8003), CreatedAt: now,
	})
	if err != nil {
		t.Fatalf("CreateEnvironment() error = %v", err)
	}
	owner, err := EnvironmentTaskOwner(project.Record, environment.Record)
	if err != nil {
		t.Fatalf("EnvironmentTaskOwner() error = %v", err)
	}
	task := validTaskRecord(now.Add(time.Minute))
	task.ID = ids.NewAt(ids.KindTask, task.CreatedAt, 8004)
	task.OperationID = ids.NewAt(ids.KindOperation, task.CreatedAt, 8005)
	task.IdempotencyKey = "environment-delete-key-0001"
	task.Owner = owner
	task.Actor = TaskActorOperator
	task.Executor = TaskExecutorAgent
	task.Type = TaskRemove
	task.Target = environment.Record.ID
	task.Params = map[string]string{TaskMaterializationEnvironmentParam: environment.Record.ID}
	marker := pendingTaskMarker(task)
	marker.Locator = IdempotencyLocator{
		ScopeKind: IdempotencyScopeEnvironment, ScopeID: environment.Record.ID,
		Method: http.MethodDelete, Route: "/environments/{id}", Key: task.IdempotencyKey,
	}
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetEnvironment, TargetID: environment.Record.ID,
		TargetRevision: environment.Revision, TaskID: task.ID,
		Phase: DeletionPhaseHostEffects, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt,
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	fixture := &environmentDeletionLockFixture{
		store: store, hierarchy: hierarchy, tasks: tasks, project: project, environment: environment,
		task: task, tombstone: tombstone, marker: marker, now: task.CreatedAt,
	}
	poolRegistryValue, err := encodeEnvelope("environment_pool_registry", EnvironmentPoolRegistry{
		Reservations: map[string]string{environmentID: environment.Record.NetworkPool},
	})
	if err != nil {
		t.Fatalf("encode Environment pool registry error = %v", err)
	}
	defer clear(poolRegistryValue)
	fixture.putRaw(t, environmentPoolRegistryKey, poolRegistryValue)
	return fixture
}

func (fixture *environmentDeletionLockFixture) begin() (IdempotencyTransactionResult, error) {
	return fixture.hierarchy.BeginEnvironmentDeletionWithTask(
		context.Background(), fixture.project, fixture.environment, 0,
		fixture.tombstone, fixture.task, fixture.marker,
	)
}

func (fixture *environmentDeletionLockFixture) mustBegin(t *testing.T) {
	t.Helper()
	result, err := fixture.begin()
	if err != nil {
		t.Fatalf("BeginEnvironmentDeletionWithTask() error = %v", err)
	}
	outcome, _, conflict, classifyErr := result.Classify()
	if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("BeginEnvironmentDeletionWithTask() = %#v/%v", result, classifyErr)
	}
}

func (fixture *environmentDeletionLockFixture) putLock(t *testing.T, record BackupOperationLockRecord) {
	t.Helper()
	value, err := encodeBackupOperationLockRecord(record)
	if err != nil {
		t.Fatalf("encodeBackupOperationLockRecord() error = %v", err)
	}
	defer clear(value)
	fixture.putRaw(t, environmentOperationLockKey(fixture.environment.Record.ID), value)
}

func (fixture *environmentDeletionLockFixture) rewriteEpoch(t *testing.T) {
	t.Helper()
	value := fixture.mustGet(t, environmentMutationEpochKey(fixture.environment.Record.ID))
	fixture.putRaw(t, value.Key, value.Value)
}

func (fixture *environmentDeletionLockFixture) putRaw(t *testing.T, key string, value []byte) {
	t.Helper()
	transaction, err := fixture.store.Transact(
		context.Background(), nil, []Mutation{{Type: MutationPut, Key: key, Value: value}},
	)
	if err != nil || !transaction.Succeeded {
		t.Fatalf("put %s = %#v, %v", key, transaction, err)
	}
}

func (fixture *environmentDeletionLockFixture) mustGet(t *testing.T, key string) *KeyValue {
	t.Helper()
	result, err := fixture.store.Get(context.Background(), key)
	if err != nil || result.Entry == nil {
		t.Fatalf("Get(%s) = %#v, %v", key, result, err)
	}
	return result.Entry
}

func assertEnvironmentDeletionCompanion(
	t *testing.T,
	store *memoryHierarchyStore,
	key string,
	want bool,
) {
	t.Helper()
	result, err := store.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("Get(%s) error = %v", key, err)
	}
	if got := result.Entry != nil; got != want {
		t.Fatalf("Get(%s) present = %t, want %t", key, got, want)
	}
}

type environmentDeletionUnknownStore struct {
	*memoryHierarchyStore
	failNext error
}

func (store *environmentDeletionUnknownStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	result, err := store.memoryHierarchyStore.Transact(ctx, conditions, mutations)
	if err == nil && result.Succeeded && store.failNext != nil {
		failure := store.failNext
		store.failNext = nil
		return TransactionResult{}, failure
	}
	return result, err
}
