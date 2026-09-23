package etcd

import (
	context "context"
	errors "errors"
	strings "strings"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testhierarchy "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: removal retry must distinguish missing or corrupt durable owner
// evidence from a legitimate hierarchy tombstone so operators see not-found or
// internal corruption instead of an inaccurate resource-in-use conflict.
func TestRunnerRemovalRetryClassifiesOwnerEvidenceAndDeletionFences(t *testing.T) {
	tests := []struct {
		name       string
		ownerKind  testrunners.RunnerOwnerKind
		mutate     func(*testing.T, *memoryHierarchyStore, testrunners.RunnerRecord)
		wantedKind errs.Kind
	}{
		{
			name: "missing tenant", ownerKind: testrunners.RunnerOwnerTenant, wantedKind: errs.KindTenantNotFound,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record testrunners.RunnerRecord) {
				connectorDeletionDeleteKey(t, store, testhierarchy.TenantKey(record.Desired.TenantID))
			},
		},
		{
			name: "corrupt tenant", ownerKind: testrunners.RunnerOwnerTenant, wantedKind: errs.KindInternal,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record testrunners.RunnerRecord) {
				connectorDeletionPutKey(t, store, testhierarchy.TenantKey(record.Desired.TenantID), []byte("corrupt"))
			},
		},
		{
			name: "tenant tombstone", ownerKind: testrunners.RunnerOwnerTenant, wantedKind: errs.KindResourceInUse,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record testrunners.RunnerRecord) {
				connectorDeletionPutKey(
					t,
					store,
					testdeletions.TombstoneKey(string(testdeletions.DeletionTargetTenant), record.Desired.TenantID),
					[]byte("fenced"),
				)
			},
		},
		{
			name: "missing project", ownerKind: testrunners.RunnerOwnerProject, wantedKind: errs.KindProjectNotFound,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record testrunners.RunnerRecord) {
				connectorDeletionDeleteKey(t, store, testhierarchy.ProjectKey(record.Desired.OwnerID))
			},
		},
		{
			name: "project tombstone", ownerKind: testrunners.RunnerOwnerProject, wantedKind: errs.KindResourceInUse,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record testrunners.RunnerRecord) {
				connectorDeletionPutKey(
					t,
					store,
					testdeletions.TombstoneKey(string(testdeletions.DeletionTargetProject), record.Desired.OwnerID),
					[]byte("fenced"),
				)
			},
		},
		{
			name: "missing owner index", ownerKind: testrunners.RunnerOwnerTenant, wantedKind: errs.KindInternal,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record testrunners.RunnerRecord) {
				connectorDeletionDeleteKey(t, store, testrunners.RunnerOwnerKey(
					record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID,
				))
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
			ownerID := tenantID
			if test.ownerKind == testrunners.RunnerOwnerProject {
				ownerID = projectID
			}
			desired := runnerTestDesired(380+index, test.ownerKind, ownerID, tenantID)
			createTask := runnerTestTask(
				desired,
				testtaskjournal.TaskCreate,
				390+index,
				"runner-owner-evidence-create-0"+string(rune('0'+index)),
			)
			if _, err := repository.CreateRunnerWithTask(
				ctx, runnerTestAllocationConfig(), desired, createTask, runnerTestMarker(createTask, desired),
			); err != nil {
				t.Fatalf("CreateRunnerWithTask() error = %v", err)
			}
			ready := runnerTestFinishCreate(t, store, repository, createTask, testtaskjournal.TaskStatusCompleted)
			removeTask := runnerTestTask(
				desired,
				testtaskjournal.TaskRemove,
				400+index,
				"runner-owner-evidence-remove-0"+string(rune('0'+index)),
			)
			removeTask.Params = testrunners.RunnerRemovalTaskParams(ready.Record)
			removeMarker := runnerTestMarker(removeTask, desired)
			tombstone := testdeletions.DeletionTombstoneRecord{
				TargetKind: testdeletions.DeletionTargetRunner, TargetID: desired.ID, TargetRevision: ready.Revision,
				TaskID: removeTask.ID, Phase: testdeletions.DeletionPhaseFinalizing,
				CreatedAt: removeTask.CreatedAt, UpdatedAt: removeTask.CreatedAt,
			}
			if _, err := repository.BeginRunnerRemovalWithTask(
				ctx, ready, tombstone, removeTask, removeMarker,
			); err != nil {
				t.Fatalf("BeginRunnerRemovalWithTask() error = %v", err)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatalf("newTaskRepository() error = %v", err)
			}
			if _, found, err := tasks.ClaimNextControllerTask(
				ctx, removeTask.CreatedAt.Add(time.Second),
			); err != nil || !found {
				t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
			}
			failed, err := tasks.AcknowledgeControllerTask(
				ctx, removeTask.ID, testtaskjournal.TaskStatusFailed, removeTask.CreatedAt.Add(2*time.Second),
			)
			if err != nil {
				t.Fatalf("AcknowledgeControllerTask() error = %v", err)
			}
			test.mutate(t, store, ready.Record)
			retryID := ids.NewAt(ids.KindTask, removeTask.CreatedAt.Add(3*time.Second), int64(7000+index))
			retryMarker := pendingRetryMarker(
				failed.Record, retryID, removeTask.CreatedAt.Add(3*time.Second),
				"runner-owner-evidence-retry-0"+string(rune('0'+index)),
			)
			if test.ownerKind == testrunners.RunnerOwnerProject {
				retryMarker.Locator.ScopeKind = testidempotency.IdempotencyScopeProject
				retryMarker.Locator.ScopeID = projectID
			} else {
				retryMarker.Locator.ScopeKind = testidempotency.IdempotencyScopeTenant
				retryMarker.Locator.ScopeID = tenantID
			}
			if _, err := tasks.RetryTask(
				ctx, removeTask.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker,
			); !errors.Is(err, errs.New(test.wantedKind, "")) {
				t.Fatalf("RetryTask() error = %v, want %v", err, test.wantedKind)
			}
		})
	}
}

func runnerTestFinishCreate(
	t *testing.T,
	store *memoryHierarchyStore,
	repository *RunnerRepository,
	task TaskRecord,
	status testtaskjournal.TaskStatus,
) testkeyvalue.Versioned[testrunners.RunnerRecord] {
	t.Helper()
	ctx := context.Background()
	taskRepository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := taskRepository.ClaimNextControllerTask(
		ctx, task.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(create) found/error = %v/%v", found, err)
	}
	if status == testtaskjournal.TaskStatusCompleted {
		runnerTestRecordReadinessProof(t, repository, task)
	}
	terminal, err := taskRepository.AcknowledgeControllerTask(
		ctx, task.ID, status, task.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != status {
		t.Fatalf("AcknowledgeControllerTask(create) = %#v, %v", terminal, err)
	}
	if err := taskRepository.validateRunnerTaskAcknowledgementReplay(
		ctx, terminal.Record, status, terminal.ReadRevision,
	); err != nil {
		t.Fatalf("validateRunnerTaskAcknowledgementReplay(create) error = %v", err)
	}
	if _, err := taskRepository.AcknowledgeControllerTask(
		ctx, task.ID, status, task.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(create replay) error = %v", err)
	}
	finished, err := repository.GetRunner(ctx, task.Target)
	if err != nil {
		t.Fatalf("GetRunner(finished) error = %v", err)
	}
	return finished
}

func runnerTestRecordReadinessProof(
	t *testing.T,
	repository *RunnerRepository,
	task TaskRecord,
) testkeyvalue.Versioned[testrunners.RunnerReadinessProofRecord] {
	t.Helper()
	ctx := context.Background()
	current, err := repository.GetRunner(ctx, task.Target)
	if err != nil {
		t.Fatalf("GetRunner(readiness) error = %v", err)
	}
	if current.Record.ContainerID == "" {
		containerID := strings.Repeat("a", testrunners.RunnerContainerIDEncodedLength)
		ownership := runnerTestRuntimeOwnership(current.Record.Desired.ID, current.Record.RuntimeEpoch+1)
		if _, err := repository.AttestRunnerRuntimeOwnership(ctx, current, containerID, ownership); err != nil {
			t.Fatalf("AttestRunnerRuntimeOwnership(readiness) error = %v", err)
		}
	}
	proof, err := repository.RecordRunnerReadinessProof(ctx, task.ID)
	if err != nil {
		t.Fatalf("RecordRunnerReadinessProof() error = %v", err)
	}
	if proof.Record.TaskID != task.ID || proof.Record.RunnerID != task.Target {
		t.Fatalf("Runner readiness proof = %#v", proof.Record)
	}
	return proof
}

func assertRunnerCreateAggregate(
	t *testing.T,
	store *memoryHierarchyStore,
	runner testkeyvalue.Versioned[testrunners.RunnerRecord],
	task TaskRecord,
	marker testidempotency.IdempotencyMarker,
) {
	t.Helper()
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	record := runner.Record
	keys := []string{
		testtaskjournal.TaskStorageKey(task.ID),
		testtaskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		testtaskjournal.TaskActiveOperationKey(task.OperationID),
		testtaskjournal.TaskQueueKey(task.Executor, task.ID),
		testrunners.RunnerKey(record.Desired.ID),
		testrunners.RunnerLifecycleKey(record.Desired.ID),
		testrunners.RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
		testrunners.RunnerTenantQuotaKey(record.Desired.TenantID),
		testrunners.RunnerHostSlotKey(record.Allocation.Slot),
		testrunners.SystemPoolRegistryKey,
		markerKey,
	}
	stored, err := store.GetMany(
		context.Background(),
		testkeyvalue.GetManyRequest{Keys: keys, Revision: runner.Revision},
	)
	if err != nil || stored == nil || len(stored.Values) != len(keys) {
		t.Fatalf("GetMany(create aggregate) = %#v, %v", stored, err)
	}
	for index, value := range stored.Values {
		if value == nil || value.ModRevision != runner.Revision {
			t.Fatalf("aggregate key %s = %#v, want create revision %d", keys[index], value, runner.Revision)
		}
	}
	storedTask, err := DecodeTaskRecord(stored.Values[0].Value)
	if err != nil || len(storedTask.Params) != 2 ||
		storedTask.Params[testrunners.RunnerRegistrationTokenPresentParam] != "true" {
		t.Fatalf("stored create Task = %#v, %v", storedTask, err)
	}
}

func assertRunnerRetryAggregate(
	t *testing.T,
	store *memoryHierarchyStore,
	runner testkeyvalue.Versioned[testrunners.RunnerRecord],
	task TaskRecord,
	marker testidempotency.IdempotencyMarker,
) {
	t.Helper()
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(retry) error = %v", err)
	}
	keys := []string{
		testtaskjournal.TaskStorageKey(task.ID),
		testtaskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
		testtaskjournal.TaskActiveOperationKey(task.OperationID),
		testtaskjournal.TaskQueueKey(task.Executor, task.ID),
		testrunners.RunnerKey(runner.Record.Desired.ID),
		testrunners.RunnerLifecycleKey(runner.Record.Desired.ID),
		markerKey,
	}
	stored, err := store.GetMany(
		context.Background(), testkeyvalue.GetManyRequest{Keys: keys, Revision: runner.ReadRevision},
	)
	if err != nil || stored == nil || len(stored.Values) != len(keys) {
		t.Fatalf("GetMany(retry aggregate) = %#v, %v", stored, err)
	}
	for index, value := range stored.Values {
		wantRevision := runner.Record.LifecycleRevision
		if index == 4 {
			wantRevision = runner.Revision
		}
		if value == nil || value.ModRevision != wantRevision {
			t.Fatalf("retry aggregate key %s = %#v, want revision %d", keys[index], value, wantRevision)
		}
	}
	allocation, err := testrunners.NewReader(store).ReadRunnerAllocationEvidence(
		context.Background(), runner.Record, runner.ReadRevision,
	)
	if err != nil || allocation.Owner == nil || allocation.Slug == nil || allocation.Quota == nil ||
		allocation.Host == nil ||
		allocation.System == nil {
		t.Fatalf("readRunnerAllocationEvidence(retry) = %#v, %v", allocation, err)
	}
}

func assertRunnerTerminalAggregate(
	t *testing.T,
	store *memoryHierarchyStore,
	terminal testkeyvalue.Versioned[TaskRecord],
	marker testidempotency.IdempotencyMarker,
	runnerID string,
) {
	t.Helper()
	markerKey, err := testidempotency.IdempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(terminal) error = %v", err)
	}
	keys := []string{
		testtaskjournal.TaskStorageKey(terminal.Record.ID),
		testtaskjournal.TaskActiveOperationKey(terminal.Record.OperationID),
		markerKey,
		testdeletions.TombstoneKey(string(testdeletions.DeletionTargetRunner), runnerID),
		testrunners.RunnerRemovalIntentKey(runnerID),
	}
	stored, err := store.GetMany(context.Background(), testkeyvalue.GetManyRequest{Keys: keys})
	if err != nil || stored == nil || len(stored.Values) != len(keys) {
		t.Fatalf("GetMany(terminal aggregate) = %#v, %v", stored, err)
	}
	if stored.Values[0] == nil || stored.Values[0].ModRevision != terminal.Revision ||
		stored.Values[1] != nil || stored.Values[2] == nil ||
		stored.Values[2].ModRevision != terminal.Revision || stored.Values[3] != nil ||
		stored.Values[4] != nil {
		t.Fatalf("terminal aggregate values = %#v, terminal revision %d", stored.Values, terminal.Revision)
	}
	storedMarker, err := testidempotency.DecodeIdempotencyMarker(stored.Values[2].Value, marker.Locator)
	if err != nil {
		t.Fatalf("decodeIdempotencyMarker(terminal) error = %v", err)
	}
	defer clear(storedMarker.Intent.Ciphertext)
	defer clear(storedMarker.Response.Body)
	wantState := testidempotency.IdempotencyMarkerFailed
	if terminal.Record.Status == testtaskjournal.TaskStatusCompleted {
		wantState = testidempotency.IdempotencyMarkerCompleted
	}
	if storedMarker.State != wantState || storedMarker.TaskID != terminal.Record.ID {
		t.Fatalf("terminal marker = %#v, want state %s", storedMarker, wantState)
	}
	tasks, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository(terminal replay) error = %v", err)
	}
	if err := tasks.validateRunnerTaskAcknowledgementReplay(
		context.Background(), terminal.Record, terminal.Record.Status, terminal.ReadRevision,
	); err != nil {
		t.Fatalf("validateRunnerTaskAcknowledgementReplay(terminal) error = %v", err)
	}
	if terminal.Record.Type == testtaskjournal.TaskRemove {
		evidence, err := decodeRunnerRemovalTaskEvidence(terminal.Record)
		if err != nil ||
			terminal.Record.Params[testrunners.RunnerHostSlotParam] != testrunners.RunnerHostSlotSegment(
				evidence.hostSlot,
			) {
			t.Fatalf("runner removal Task Params = %#v, %v", terminal.Record.Params, err)
		}
	}
}
