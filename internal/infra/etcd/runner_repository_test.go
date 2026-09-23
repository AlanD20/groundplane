package etcd

import (
	context "context"
	errors "errors"
	netip "net/netip"
	testing "testing"
	time "time"

	ids "github.com/AlanD20/groundplane/internal/common/ids"
	runnerallocation "github.com/AlanD20/groundplane/internal/common/runnerallocation"
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testidempotency "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testrunners "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	errs "github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: one atomic create must publish exactly one owner index while the
// Tenant-wide quota and host-global registries receive the lowest free slots.
func TestRunnerCreatePublishesOneOwnerAndLowestFreeAllocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()

	tenantRunner := runnerTestDesired(10, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	firstTask := runnerTestTask(tenantRunner, testtaskjournal.TaskCreate, 20, "runner-create-key-0001")
	firstMarker := runnerTestMarker(firstTask, tenantRunner)
	first, err := repository.CreateRunnerWithTask(
		ctx, config, tenantRunner, firstTask, firstMarker,
	)
	if err != nil {
		t.Fatalf("CreateRunnerWithTask(tenant) error = %v", err)
	}
	if outcome, _, _, classifyErr := first.Classify(); classifyErr != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("CreateRunnerWithTask(tenant) outcome = %v, %v", outcome, classifyErr)
	}

	projectRunner := runnerTestDesired(11, testrunners.RunnerOwnerProject, projectID, tenantID)
	secondTask := runnerTestTask(projectRunner, testtaskjournal.TaskCreate, 21, "runner-create-key-0002")
	if _, err := repository.CreateRunnerWithTask(
		ctx, config, projectRunner, secondTask, runnerTestMarker(secondTask, projectRunner),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask(project) error = %v", err)
	}
	firstRecord, err := repository.GetRunner(ctx, tenantRunner.ID)
	if err != nil {
		t.Fatalf("GetRunner(first) error = %v", err)
	}
	secondRecord, err := repository.GetRunner(ctx, projectRunner.ID)
	if err != nil {
		t.Fatalf("GetRunner(second) error = %v", err)
	}
	if firstRecord.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning ||
		firstRecord.Record.Allocation.Slot != 0 ||
		firstRecord.Record.Allocation.HostUID != config.HostPool.HostUIDStart ||
		firstRecord.Record.Allocation.NetworkCIDR != "10.240.0.0/29" {
		t.Fatalf("first Runner = %#v", firstRecord.Record)
	}
	assertRunnerCreateAggregate(t, store, firstRecord, firstTask, firstMarker)
	if secondRecord.Record.Allocation.Slot != 1 ||
		secondRecord.Record.Allocation.NetworkCIDR != "10.240.0.8/29" {
		t.Fatalf("second Runner = %#v", secondRecord.Record)
	}
	indexes, err := store.GetMany(
		ctx,
		testkeyvalue.GetManyRequest{
			Keys: []string{
				testrunners.RunnerOwnerKey(testrunners.RunnerOwnerProject, projectID, projectRunner.ID),
				testrunners.RunnerOwnerKey(testrunners.RunnerOwnerTenant, tenantID, projectRunner.ID),
			},
		},
	)
	if err != nil {
		t.Fatalf("GetMany(owner indexes) error = %v", err)
	}
	if indexes.Values[0] == nil || indexes.Values[1] != nil {
		t.Fatalf("project Runner owner indexes = %#v, want exactly the Project index", indexes.Values)
	}
	page, err := repository.ListRunners(
		ctx,
		testrunners.RunnerFilter{TenantID: tenantID},
		testkeyvalue.PageRequest{Limit: 1},
	)
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("ListRunners(first page) = %#v, %v", page, err)
	}
	next, err := repository.ListRunners(
		ctx, testrunners.RunnerFilter{TenantID: tenantID}, testkeyvalue.PageRequest{Limit: 1, Cursor: page.NextCursor},
	)
	if err != nil || len(next.Items) != 1 || next.NextCursor != "" {
		t.Fatalf("ListRunners(second page) = %#v, %v", next, err)
	}
}

// Rationale: all ownership scopes consume one Tenant quota and exact
// idempotency replay must not allocate a second host identity or subnet.
func TestRunnerCreateReplayAndCombinedTenantQuota(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()
	for index := 0; index < runnerallocation.MaximumTenantRunners; index++ {
		ownerKind, ownerID := testrunners.RunnerOwnerTenant, tenantID
		if index%2 == 1 {
			ownerKind, ownerID = testrunners.RunnerOwnerProject, projectID
		}
		desired := runnerTestDesired(30+index, ownerKind, ownerID, tenantID)
		key := "runner-quota-key-000" + string(rune('0'+index))
		task := runnerTestTask(desired, testtaskjournal.TaskCreate, 40+index, key)
		_, err := repository.CreateRunnerWithTask(ctx, config, desired, task, runnerTestMarker(task, desired))
		if err != nil {
			t.Fatalf("CreateRunnerWithTask(%d) error = %v", index, err)
		}
		if index == 0 {
			replay, replayErr := repository.CreateRunnerWithTask(
				ctx, config, desired, task, runnerTestMarker(task, desired),
			)
			if replayErr != nil {
				t.Fatalf("CreateRunnerWithTask(replay) error = %v", replayErr)
			}
			outcome, marker, conflict, classifyErr := replay.Classify()
			if classifyErr != nil || conflict != nil || outcome != IdempotencyKnownExisting ||
				marker.TaskID != task.ID {
				t.Fatalf("CreateRunnerWithTask(replay) = %v, %#v, %v, %v", outcome, marker, conflict, classifyErr)
			}
		}
	}
	sixth := runnerTestDesired(99, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	task := runnerTestTask(sixth, testtaskjournal.TaskCreate, 99, "runner-quota-key-9999")
	if _, err := repository.CreateRunnerWithTask(
		ctx, config, sixth, task, runnerTestMarker(task, sixth),
	); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
		t.Fatalf("CreateRunnerWithTask(sixth) error = %v, want resource.in_use", err)
	}
}

// Rationale: a failed create retry is a new protected request but preserves
// the operation inputs and every scarce allocation while storing only the
// fresh-token-present fact.
func TestRunnerFailedCreateRetryReusesAllocationAndReplays(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(50, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	source := runnerTestTask(desired, testtaskjournal.TaskCreate, 51, "runner-create-operation-key-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, source, runnerTestMarker(source, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	failed := runnerTestFinishCreate(t, store, repository, source, testtaskjournal.TaskStatusFailed)
	if failed.Record.ProvisioningState != testrunners.RunnerProvisioningFailed {
		t.Fatalf("failed Runner state = %s", failed.Record.ProvisioningState)
	}
	retry := runnerTestTask(desired, testtaskjournal.TaskCreate, 52, source.IdempotencyKey)
	retry.RetryOf = source.ID
	retry.OperationID = source.OperationID
	retryMarker := runnerTestMarker(retry, desired)
	retryMarker.Locator.Key = "runner-retry-request-key-01"
	result, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, retryMarker)
	if err != nil {
		t.Fatalf("RetryRunnerCreationWithTask() error = %v", err)
	}
	if outcome, _, conflict, classifyErr := result.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryRunnerCreationWithTask() = %v, %v, %v", outcome, conflict, classifyErr)
	}
	current, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || current.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning ||
		current.Record.CreateTaskID != retry.ID || current.Record.Allocation != failed.Record.Allocation {
		t.Fatalf("retried Runner = %#v, %v", current, err)
	}
	replayMarker := runnerTestMarker(retry, desired)
	replayMarker.Locator.Key = retryMarker.Locator.Key
	replay, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, replayMarker)
	if err != nil {
		t.Fatalf("RetryRunnerCreationWithTask(replay) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := replay.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownExisting {
		t.Fatalf("RetryRunnerCreationWithTask(replay) = %v, %v, %v", outcome, conflict, classifyErr)
	}
	assertRunnerRetryAggregate(t, store, current, retry, retryMarker)
}

// Rationale: each Runner operation owns one exact route, operation key, 202
// response, and replay-target shape so protected replay cannot cross actions.
func TestRunnerMarkersAreOperationSpecific(t *testing.T) {
	t.Parallel()
	_, _, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(210, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, testtaskjournal.TaskCreate, 211, "runner-marker-create-key")
	createMarker := runnerTestMarker(createTask, desired)
	if err := validateRunnerCreateMarker(desired, createTask, createMarker); err != nil {
		t.Fatalf("validateRunnerCreateMarker() error = %v", err)
	}
	createMarker.Locator.Route = "/tasks"
	if err := validateRunnerCreateMarker(desired, createTask, createMarker); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateRunnerCreateMarker(wrong route) error = %v", err)
	}
	createTask.IdempotencyKey = ""
	if err := validateRunnerCreateMarker(desired, createTask, runnerTestMarker(createTask, desired)); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateRunnerCreateMarker(empty task key) error = %v", err)
	}
	createTask = runnerTestTask(desired, testtaskjournal.TaskCreate, 211, "runner-marker-create-key")
	createMarker = runnerTestMarker(createTask, desired)
	createMarker.Locator.Key = "runner-marker-other-key"
	if err := validateRunnerCreateMarker(desired, createTask, createMarker); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateRunnerCreateMarker(mismatched task key) error = %v", err)
	}
	createMarker = runnerTestMarker(createTask, desired)
	createMarker.Response.Body = []byte(`{"task_id":"wrong"}`)
	if err := validateRunnerCreateMarker(desired, createTask, createMarker); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateRunnerCreateMarker(wrong response) error = %v", err)
	}
	removeTask := runnerTestTask(desired, testtaskjournal.TaskRemove, 212, "runner-marker-remove-key")
	allocation, err := runnerTestAllocationConfig().HostPool.Allocation(
		0, netip.MustParsePrefix("10.240.0.0/29"),
	)
	if err != nil {
		t.Fatalf("Allocation() error = %v", err)
	}
	removeRecord, err := testrunners.NewProvisioningRunner(desired, allocation, createTask.ID, createTask.CreatedAt)
	if err != nil {
		t.Fatalf("NewProvisioningRunner() error = %v", err)
	}
	removeTask.Params = testrunners.RunnerRemovalTaskParams(removeRecord)
	removeMarker := runnerTestMarker(removeTask, desired)
	if err := validateRunnerDeleteMarker(desired, removeTask, removeMarker); err != nil {
		t.Fatalf("validateRunnerDeleteMarker() error = %v", err)
	}
	removeTask.Params[testrunners.RunnerHostSlotParam] = "0"
	if applies, err := taskOwnsRunner(removeTask); applies || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskOwnsRunner(non-canonical slot) = %v, %v", applies, err)
	}
	removeTask.Params = testrunners.RunnerRemovalTaskParams(removeRecord)
	removeMarker.ReplayTarget = nil
	if err := validateRunnerDeleteMarker(desired, removeTask, removeMarker); !errors.Is(
		err, errs.New(errs.KindValidationFailed, ""),
	) {
		t.Fatalf("validateRunnerDeleteMarker(missing replay target) error = %v", err)
	}
}

// Rationale: online is replaceable observation state, while a completed
// removal finalizer retains and then releases every allocation fence atomically.
func TestRunnerObservationAndRemovalFinalizer(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()
	desired := runnerTestDesired(60, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, testtaskjournal.TaskCreate, 61, "runner-lifecycle-key-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, config, desired, createTask, runnerTestMarker(createTask, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	ready := runnerTestFinishCreate(t, store, repository, createTask, testtaskjournal.TaskStatusCompleted)
	if ready.Record.ProvisioningState != testrunners.RunnerProvisioningReady {
		t.Fatalf("Runner state = %s, want ready", ready.Record.ProvisioningState)
	}
	observedAt := taskJournalTime().Add(3 * time.Second)
	observation, err := repository.PutRunnerObservation(ctx, testrunners.RunnerObservationRecord{
		RunnerID: desired.ID, Online: true, ObservedAt: observedAt,
	}, 0)
	if err != nil || !observation.Record.Online {
		t.Fatalf("PutRunnerObservation() = %#v, %v", observation, err)
	}

	removeTask := runnerTestTask(desired, testtaskjournal.TaskRemove, 62, "runner-removal-key-001")
	removeTask.Params = testrunners.RunnerRemovalTaskParams(ready.Record)
	removeMarker := runnerTestMarker(removeTask, desired)
	tombstone := testdeletions.DeletionTombstoneRecord{
		TargetKind: testdeletions.DeletionTargetRunner, TargetID: desired.ID,
		TargetRevision: ready.Revision, TaskID: removeTask.ID,
		Phase: testdeletions.DeletionPhaseFinalizing, CreatedAt: removeTask.CreatedAt, UpdatedAt: removeTask.CreatedAt,
	}
	if _, err := repository.BeginRunnerRemovalWithTask(
		ctx, ready, tombstone, removeTask, removeMarker,
	); err != nil {
		t.Fatalf("BeginRunnerRemovalWithTask() error = %v", err)
	}
	replay, err := repository.BeginRunnerRemovalWithTask(
		ctx, ready, tombstone, removeTask, runnerTestMarker(removeTask, desired),
	)
	if err != nil {
		t.Fatalf("BeginRunnerRemovalWithTask(replay) error = %v", err)
	}
	if outcome, _, conflict, classifyErr := replay.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownExisting {
		t.Fatalf("BeginRunnerRemovalWithTask(replay) = %v, %v, %v", outcome, conflict, classifyErr)
	}
	retained, err := repository.GetRunner(ctx, desired.ID)
	if err != nil || retained.Record.Allocation != ready.Record.Allocation {
		t.Fatalf("Runner during removal = %#v, %v", retained, err)
	}
	ownership, found, err := repository.GetRunnerRuntimeOwnership(ctx, desired.ID)
	if err != nil || !found {
		t.Fatalf("GetRunnerRuntimeOwnership(remove) found/error = %t/%v", found, err)
	}
	if _, err := repository.DeleteRunnerRuntimeOwnershipAfterCleanup(
		ctx, retained, ownership.Record,
	); err != nil {
		t.Fatalf("DeleteRunnerRuntimeOwnershipAfterCleanup() error = %v", err)
	}
	taskRepository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, found, err := taskRepository.ClaimNextControllerTask(
		ctx, removeTask.CreatedAt.Add(time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(remove) found/error = %v/%v", found, err)
	}
	terminal, err := taskRepository.AcknowledgeControllerTask(
		ctx, removeTask.ID, testtaskjournal.TaskStatusCompleted, removeTask.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != testtaskjournal.TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask(remove) = %#v, %v", terminal, err)
	}
	assertRunnerTerminalAggregate(t, store, terminal, removeMarker, desired.ID)
	if _, err := taskRepository.AcknowledgeControllerTask(
		ctx, removeTask.ID, testtaskjournal.TaskStatusCompleted, removeTask.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(remove replay) error = %v", err)
	}
	if _, err := repository.GetRunner(ctx, desired.ID); !errors.Is(err, errs.New(errs.KindRunnerNotFound, "")) {
		t.Fatalf("GetRunner(removed) error = %v, want runner.not_found", err)
	}
	if _, found, err := repository.GetRunnerObservation(ctx, desired.ID); err != nil || found {
		t.Fatalf("GetRunnerObservation(removed) found = %v, error = %v", found, err)
	}
	idempotency, err := NewIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	locator, found, err := idempotency.ResolveReplayLocator(
		ctx,
		testidempotency.IdempotencyReplayTarget{Kind: testidempotency.IdempotencyReplayTargetRunner, ID: desired.ID},
		removeMarker.Locator.Method,
		removeMarker.Locator.Route,
		removeMarker.Locator.Key,
	)
	if err != nil || !found || locator != removeMarker.Locator {
		t.Fatalf("ResolveReplayLocator(remove) = %#v, %v, %v", locator, found, err)
	}
	replacement := runnerTestDesired(63, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	replacementTask := runnerTestTask(replacement, testtaskjournal.TaskCreate, 64, "runner-replace-key-001")
	if _, err := repository.CreateRunnerWithTask(
		ctx, config, replacement, replacementTask, runnerTestMarker(replacementTask, replacement),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask(replacement) error = %v", err)
	}
	replacementRecord, err := repository.GetRunner(ctx, replacement.ID)
	if err != nil || replacementRecord.Record.Allocation.Slot != 0 ||
		replacementRecord.Record.Allocation.NetworkCIDR != "10.240.0.0/29" {
		t.Fatalf("replacement Runner = %#v, %v", replacementRecord, err)
	}
}

// Rationale: every unsuccessful removal terminal state must atomically clear
// its active fence while retaining the Runner and exact allocations; retry
// must reacquire the complete hierarchy and allocation fence set.
func TestRunnerRemovalFailureTimeoutAbortAndRetryRetainAllocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(70, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, testtaskjournal.TaskCreate, 71, "runner-removal-source-key")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, createTask, runnerTestMarker(createTask, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	ready := runnerTestFinishCreate(t, store, repository, createTask, testtaskjournal.TaskStatusCompleted)
	allocation := ready.Record.Allocation
	removeTask := runnerTestTask(desired, testtaskjournal.TaskRemove, 72, "runner-removal-failure-key")
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
		t.Fatalf("ClaimNextControllerTask(remove) found/error = %v/%v", found, err)
	}
	failed, err := tasks.AcknowledgeControllerTask(
		ctx, removeTask.ID, testtaskjournal.TaskStatusFailed, removeTask.CreatedAt.Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("AcknowledgeControllerTask(failed) error = %v", err)
	}
	assertRunnerTerminalAggregate(t, store, failed, removeMarker, desired.ID)
	assertRunnerAllocationRetained(t, repository, ready.Record, allocation)

	retryID := ids.NewAt(ids.KindTask, removeTask.CreatedAt.Add(3*time.Second), 1500)
	retryMarker := pendingRetryMarker(
		removeTask, retryID, removeTask.CreatedAt.Add(3*time.Second), "runner-remove-retry-key-01",
	)
	retryMarker.Locator.ScopeKind = testidempotency.IdempotencyScopeTenant
	retryMarker.Locator.ScopeID = tenantID
	if _, err := tasks.RetryTask(ctx, removeTask.ID, retryID, testtaskjournal.TaskActorOperator, retryMarker); err != nil {
		t.Fatalf("RetryTask(after failure) error = %v", err)
	}
	if _, found, err := tasks.ClaimNextControllerTask(
		ctx, removeTask.CreatedAt.Add(4*time.Second),
	); err != nil || !found {
		t.Fatalf("ClaimNextControllerTask(retry) found/error = %v/%v", found, err)
	}
	claim, err := tasks.GetTask(ctx, retryID)
	if err != nil || claim.Record.StartedAt == nil {
		t.Fatalf("GetTask(running retry) = %#v, %v", claim, err)
	}
	expired, err := tasks.ExpireTimedOutTasks(
		ctx, claim.Record.StartedAt.Add(time.Duration(claim.Record.TimeoutSeconds+1)*time.Second),
	)
	if err != nil || expired != 1 {
		t.Fatalf("ExpireTimedOutTasks() = %d, %v", expired, err)
	}
	timedOut, err := tasks.GetTask(ctx, retryID)
	if err != nil || timedOut.Record.Status != testtaskjournal.TaskStatusTimedOut {
		t.Fatalf("GetTask(timed out) = %#v, %v", timedOut, err)
	}
	assertRunnerTerminalAggregate(t, store, timedOut, retryMarker, desired.ID)
	assertRunnerAllocationRetained(t, repository, ready.Record, allocation)

	abortID := ids.NewAt(ids.KindTask, removeTask.CreatedAt.Add(6*time.Second), 1501)
	abortMarker := pendingRetryMarker(
		timedOut.Record, abortID, removeTask.CreatedAt.Add(6*time.Second), "runner-remove-retry-key-02",
	)
	abortMarker.Locator.ScopeKind = testidempotency.IdempotencyScopeTenant
	abortMarker.Locator.ScopeID = tenantID
	if _, err := tasks.RetryTask(ctx, retryID, abortID, testtaskjournal.TaskActorOperator, abortMarker); err != nil {
		t.Fatalf("RetryTask(after timeout) error = %v", err)
	}
	aborted, err := tasks.AbortPendingTask(ctx, abortID, removeTask.CreatedAt.Add(7*time.Second))
	if err != nil || aborted.Record.Status != testtaskjournal.TaskStatusAborted {
		t.Fatalf("AbortPendingTask() = %#v, %v", aborted, err)
	}
	assertRunnerTerminalAggregate(t, store, aborted, abortMarker, desired.ID)
	assertRunnerAllocationRetained(t, repository, ready.Record, allocation)
}

// Rationale: host-slot discovery may scan a large configured pool, but one
// create transaction must stay below etcd's 96-operation ceiling and lose a
// selected-slot CAS race without overwriting the winner.
func TestRunnerHighSlotAllocationHasBoundedCASAndRejectsSelectedSlotRace(t *testing.T) {
	const (
		hostSlots        = uint32(96)
		occupiedSlots    = uint32(80)
		operationLimit   = 96
		contenderEntropy = int64(6200)
	)
	t.Run("bounded transaction", func(t *testing.T) {
		ctx := context.Background()
		store, _, tenantID, _ := newRunnerRepositoryFixture(t)
		seedRunnerHostSlots(t, store, occupiedSlots)
		audit := &runnerTransactionAuditStore{memoryHierarchyStore: store}
		repository, err := newRunnerRepository(audit)
		if err != nil {
			t.Fatalf("newRunnerRepository() error = %v", err)
		}
		desired := runnerTestDesired(300, testrunners.RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, testtaskjournal.TaskCreate, 301, "runner-high-slot-key-0001")
		if _, err := repository.CreateRunnerWithTask(
			ctx, runnerTestAllocationConfigWithSlots(hostSlots), desired, task, runnerTestMarker(task, desired),
		); err != nil {
			t.Fatalf("CreateRunnerWithTask() error = %v", err)
		}
		current, err := repository.GetRunner(ctx, desired.ID)
		if err != nil || current.Record.Allocation.Slot != occupiedSlots {
			t.Fatalf("GetRunner(high slot) = %#v, %v", current, err)
		}
		if audit.maximumOperations > operationLimit {
			t.Fatalf("Runner create operations = %d, limit %d", audit.maximumOperations, operationLimit)
		}
	})

	t.Run("selected slot race", func(t *testing.T) {
		ctx := context.Background()
		store, _, tenantID, _ := newRunnerRepositoryFixture(t)
		seedRunnerHostSlots(t, store, occupiedSlots)
		contenderID := ids.NewAt(ids.KindRunner, taskJournalTime(), contenderEntropy)
		contenderValue, err := testrunners.EncodeRunnerHostSlotRecord(testrunners.RunnerHostSlotRecord{
			Slot: occupiedSlots, RunnerID: contenderID,
		})
		if err != nil {
			t.Fatalf("encodeRunnerHostSlotRecord() error = %v", err)
		}
		defer clear(contenderValue)
		audit := &runnerTransactionAuditStore{
			memoryHierarchyStore: store,
			allocationRaceKey:    testrunners.RunnerHostSlotKey(occupiedSlots),
			allocationRaceValue:  contenderValue,
		}
		repository, err := newRunnerRepository(audit)
		if err != nil {
			t.Fatalf("newRunnerRepository() error = %v", err)
		}
		desired := runnerTestDesired(302, testrunners.RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, testtaskjournal.TaskCreate, 303, "runner-high-slot-race-key")
		result, err := repository.CreateRunnerWithTask(
			ctx, runnerTestAllocationConfigWithSlots(hostSlots), desired, task, runnerTestMarker(task, desired),
		)
		if err != nil {
			t.Fatalf("CreateRunnerWithTask(race) error = %v", err)
		}
		outcome, _, conflict, classifyErr := result.Classify()
		if classifyErr != nil || outcome != IdempotencyKnownConflict ||
			!errors.Is(conflict, errs.New(errs.KindStateConflict, "")) {
			t.Fatalf("CreateRunnerWithTask(race) = %v, %v, %v", outcome, conflict, classifyErr)
		}
		entry := mustOptionalKey(t, store, testrunners.RunnerHostSlotKey(occupiedSlots))
		owned, decodeErr := testrunners.DecodeRunnerHostSlotRecord(entry.Value)
		if decodeErr != nil || owned.RunnerID != contenderID {
			t.Fatalf("selected slot winner = %#v, %v", owned, decodeErr)
		}
		if audit.maximumOperations > operationLimit {
			t.Fatalf("Runner raced create operations = %d, limit %d", audit.maximumOperations, operationLimit)
		}
	})
}

// Rationale: both ready and failed create terminalization must refuse to
// publish when any one of the four allocation ownership records is missing.
func TestRunnerCreationTerminalizationRequiresEveryAllocationFence(t *testing.T) {
	tests := []struct {
		name   string
		status testtaskjournal.TaskStatus
		key    func(testrunners.RunnerRecord) string
	}{
		{
			name: "owner index on success", status: testtaskjournal.TaskStatusCompleted,
			key: func(record testrunners.RunnerRecord) string {
				return testrunners.RunnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID)
			},
		},
		{
			name:   "tenant quota on failure",
			status: testtaskjournal.TaskStatusFailed,
			key: func(record testrunners.RunnerRecord) string {
				return testrunners.RunnerTenantQuotaKey(record.Desired.TenantID)
			},
		},
		{
			name:   "host slot on success",
			status: testtaskjournal.TaskStatusCompleted,
			key: func(record testrunners.RunnerRecord) string {
				return testrunners.RunnerHostSlotKey(record.Allocation.Slot)
			},
		},
		{
			name:   "system pool on failure",
			status: testtaskjournal.TaskStatusFailed,
			key: func(testrunners.RunnerRecord) string {
				return testrunners.SystemPoolRegistryKey
			},
		},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
			desired := runnerTestDesired(320+index, testrunners.RunnerOwnerTenant, tenantID, tenantID)
			task := runnerTestTask(
				desired, testtaskjournal.TaskCreate, 330+index,
				"runner-terminal-fence-key-0"+string(rune('0'+index)),
			)
			if _, err := repository.CreateRunnerWithTask(
				ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
			); err != nil {
				t.Fatalf("CreateRunnerWithTask() error = %v", err)
			}
			current, err := repository.GetRunner(ctx, desired.ID)
			if err != nil {
				t.Fatalf("GetRunner() error = %v", err)
			}
			tasks, err := newTaskRepository(store)
			if err != nil {
				t.Fatalf("newTaskRepository() error = %v", err)
			}
			if _, found, err := tasks.ClaimNextControllerTask(
				ctx, task.CreatedAt.Add(time.Second),
			); err != nil || !found {
				t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
			}
			if test.status == testtaskjournal.TaskStatusCompleted {
				runnerTestRecordReadinessProof(t, repository, task)
			}
			connectorDeletionDeleteKey(t, store, test.key(current.Record))
			if _, err := tasks.AcknowledgeControllerTask(
				ctx, task.ID, test.status, task.CreatedAt.Add(2*time.Second),
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("AcknowledgeControllerTask() error = %v", err)
			}
			retained, err := repository.GetRunner(ctx, desired.ID)
			if err != nil || retained.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning {
				t.Fatalf("Runner after rejected terminalization = %#v, %v", retained, err)
			}
		})
	}
}

// Rationale: allocation ownership can change after acknowledgement reads; the
// terminal transaction must compare those exact revisions and leave the Runner
// provisioning when the CAS loses for either terminal result.
func TestRunnerCreationTerminalizationCASFencesAllocationRace(t *testing.T) {
	for index, status := range []testtaskjournal.TaskStatus{testtaskjournal.TaskStatusCompleted, testtaskjournal.TaskStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
			desired := runnerTestDesired(340+index, testrunners.RunnerOwnerTenant, tenantID, tenantID)
			task := runnerTestTask(
				desired,
				testtaskjournal.TaskCreate,
				350+index,
				"runner-terminal-race-key-0"+string(rune('0'+index)),
			)
			if _, err := repository.CreateRunnerWithTask(
				ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
			); err != nil {
				t.Fatalf("CreateRunnerWithTask() error = %v", err)
			}
			current, err := repository.GetRunner(ctx, desired.ID)
			if err != nil {
				t.Fatalf("GetRunner() error = %v", err)
			}
			audit := &runnerTransactionAuditStore{
				memoryHierarchyStore: store,
				terminalRunnerID:     desired.ID,
				terminalRaceKey:      testrunners.RunnerHostSlotKey(current.Record.Allocation.Slot),
			}
			tasks, err := newTaskRepository(audit)
			if err != nil {
				t.Fatalf("newTaskRepository() error = %v", err)
			}
			if _, found, err := tasks.ClaimNextControllerTask(
				ctx, task.CreatedAt.Add(time.Second),
			); err != nil || !found {
				t.Fatalf("ClaimNextControllerTask() found/error = %v/%v", found, err)
			}
			if status == testtaskjournal.TaskStatusCompleted {
				runnerTestRecordReadinessProof(t, repository, task)
			}
			if _, err := tasks.AcknowledgeControllerTask(
				ctx, task.ID, status, task.CreatedAt.Add(2*time.Second),
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("AcknowledgeControllerTask(race) error = %v", err)
			}
			retained, err := repository.GetRunner(ctx, desired.ID)
			if err != nil || retained.Record.ProvisioningState != testrunners.RunnerProvisioningProvisioning {
				t.Fatalf("Runner after CAS loss = %#v, %v", retained, err)
			}
		})
	}
}

// Rationale: an exact create-retry replay is determined by its protected
// marker before mutable source Task or Runner state, while malformed replay
// metadata must still be rejected before that lookup.
func TestRunnerCreationRetryReplayPrecedesMutableStateReads(t *testing.T) {
	ctx := context.Background()
	store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(360, testrunners.RunnerOwnerTenant, tenantID, tenantID)
	source := runnerTestTask(desired, testtaskjournal.TaskCreate, 361, "runner-replay-source-key")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, source, runnerTestMarker(source, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	runnerTestFinishCreate(t, store, repository, source, testtaskjournal.TaskStatusFailed)
	retry := runnerTestTask(desired, testtaskjournal.TaskCreate, 362, source.IdempotencyKey)
	retry.RetryOf = source.ID
	retry.OperationID = source.OperationID
	marker := runnerTestMarker(retry, desired)
	marker.Locator.Key = "runner-replay-request-key"
	if _, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, marker); err != nil {
		t.Fatalf("RetryRunnerCreationWithTask() error = %v", err)
	}
	result, err := store.Transact(ctx, nil, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskStorageKey(source.ID)},
		{Type: testkeyvalue.MutationDelete, Key: testrunners.RunnerKey(desired.ID)},
	})
	if err != nil || !result.Succeeded {
		t.Fatalf("remove mutable retry state = %#v, %v", result, err)
	}
	malformed := marker
	malformed.Response.Body = []byte(`{}`)
	if _, err := repository.RetryRunnerCreationWithTask(
		ctx, source.ID, retry, malformed,
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("RetryRunnerCreationWithTask(malformed replay) error = %v", err)
	}
	replay, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, marker)
	if err != nil {
		t.Fatalf("RetryRunnerCreationWithTask(replay) error = %v", err)
	}
	if outcome, existing, conflict, classifyErr := replay.Classify(); classifyErr != nil || conflict != nil ||
		outcome != IdempotencyKnownExisting || existing.TaskID != retry.ID {
		t.Fatalf("RetryRunnerCreationWithTask(replay) = %v, %#v, %v, %v", outcome, existing, conflict, classifyErr)
	}
}
