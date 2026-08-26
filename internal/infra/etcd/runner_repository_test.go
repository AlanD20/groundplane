package etcd

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Rationale: one atomic create must publish exactly one owner index while the
// Tenant-wide quota and host-global registries receive the lowest free slots.
func TestRunnerCreatePublishesOneOwnerAndLowestFreeAllocations(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, tenantID, projectID := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()

	tenantRunner := runnerTestDesired(10, RunnerOwnerTenant, tenantID, tenantID)
	firstTask := runnerTestTask(tenantRunner, TaskCreate, 20, "runner-create-key-0001")
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

	projectRunner := runnerTestDesired(11, RunnerOwnerProject, projectID, tenantID)
	secondTask := runnerTestTask(projectRunner, TaskCreate, 21, "runner-create-key-0002")
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
	if firstRecord.Record.ProvisioningState != RunnerProvisioningProvisioning ||
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
	indexes, err := store.GetMany(ctx, GetManyRequest{Keys: []string{
		runnerOwnerKey(RunnerOwnerProject, projectID, projectRunner.ID),
		runnerOwnerKey(RunnerOwnerTenant, tenantID, projectRunner.ID),
	}})
	if err != nil {
		t.Fatalf("GetMany(owner indexes) error = %v", err)
	}
	if indexes.Values[0] == nil || indexes.Values[1] != nil {
		t.Fatalf("project Runner owner indexes = %#v, want exactly the Project index", indexes.Values)
	}
	page, err := repository.ListRunners(ctx, RunnerFilter{TenantID: tenantID}, PageRequest{Limit: 1})
	if err != nil || len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("ListRunners(first page) = %#v, %v", page, err)
	}
	next, err := repository.ListRunners(
		ctx, RunnerFilter{TenantID: tenantID}, PageRequest{Limit: 1, Cursor: page.NextCursor},
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
		ownerKind, ownerID := RunnerOwnerTenant, tenantID
		if index%2 == 1 {
			ownerKind, ownerID = RunnerOwnerProject, projectID
		}
		desired := runnerTestDesired(30+index, ownerKind, ownerID, tenantID)
		key := "runner-quota-key-000" + string(rune('0'+index))
		task := runnerTestTask(desired, TaskCreate, 40+index, key)
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
	sixth := runnerTestDesired(99, RunnerOwnerTenant, tenantID, tenantID)
	task := runnerTestTask(sixth, TaskCreate, 99, "runner-quota-key-9999")
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
	desired := runnerTestDesired(50, RunnerOwnerTenant, tenantID, tenantID)
	source := runnerTestTask(desired, TaskCreate, 51, "runner-create-operation-key-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, source, runnerTestMarker(source, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	failed := runnerTestFinishCreate(t, store, repository, source, TaskStatusFailed)
	if failed.Record.ProvisioningState != RunnerProvisioningFailed {
		t.Fatalf("failed Runner state = %s", failed.Record.ProvisioningState)
	}
	retry := runnerTestTask(desired, TaskCreate, 52, source.IdempotencyKey)
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
	if err != nil || current.Record.ProvisioningState != RunnerProvisioningProvisioning ||
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

// Rationale: allocation exhaustion is a use conflict while malformed durable
// slot ownership is corruption; callers must never receive the same class.
func TestRunnerAllocationExhaustionAndCorruptionAreExplicit(t *testing.T) {
	t.Run("host slots", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		config := runnerTestAllocationConfig()
		config.HostPool.HostUIDEnd = config.HostPool.HostUIDStart + 4
		config.HostPool.SubUIDEnd = config.HostPool.SubUIDStart + 5*runnerallocation.RunnerSubordinateBlockSize - 1
		config.HostPool.SubGIDEnd = config.HostPool.SubGIDStart + 5*runnerallocation.RunnerSubordinateBlockSize - 1
		for index := 0; index < 5; index++ {
			desired := runnerTestDesired(100+index, RunnerOwnerTenant, tenantID, tenantID)
			task := runnerTestTask(desired, TaskCreate, 110+index, "runner-host-capacity-key-0"+string(rune('0'+index)))
			if _, err := repository.CreateRunnerWithTask(
				ctx, config, desired, task, runnerTestMarker(task, desired),
			); err != nil {
				t.Fatalf("CreateRunnerWithTask(%d) error = %v", index, err)
			}
		}
		hierarchy, err := newHierarchyRepository(store)
		if err != nil {
			t.Fatalf("newHierarchyRepository() error = %v", err)
		}
		otherTenantID := ids.NewAt(ids.KindTenant, taskJournalTime(), 1800)
		if _, err := hierarchy.CreateTenant(ctx, TenantRecord{
			ID: otherTenantID, Slug: "other", Name: "Other",
		}); err != nil {
			t.Fatalf("CreateTenant(other) error = %v", err)
		}
		desired := runnerTestDesired(199, RunnerOwnerTenant, otherTenantID, otherTenantID)
		task := runnerTestTask(desired, TaskCreate, 199, "runner-host-capacity-over")
		if _, err := repository.CreateRunnerWithTask(
			ctx, config, desired, task, runnerTestMarker(task, desired),
		); !errors.Is(err, errs.New(errs.KindResourceInUse, "")) {
			t.Fatalf("CreateRunnerWithTask(host exhausted) error = %v", err)
		}
	})

	t.Run("network pool", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		config := runnerTestAllocationConfig()
		registry := runnerallocation.SystemPoolRegistry{
			RunnerNetworkPool: config.RunnerPool.String(),
			Reservations:      map[string]string{},
		}
		for index := 0; index < 32; index++ {
			prefix := netip.PrefixFrom(
				netip.AddrFrom4([4]byte{10, 240, 0, byte(index * 8)}), runnerallocation.RunnerSubnetBits,
			)
			registry.Reservations["component/"+string(rune('a'+index))] = prefix.String()
		}
		value, err := encodeEnvelope("system_pool_registry", registry)
		if err != nil {
			t.Fatalf("encode system pool error = %v", err)
		}
		defer clear(value)
		current := mustOptionalKey(t, store, systemPoolRegistryKey)
		result, err := store.Transact(ctx,
			[]Condition{{Key: systemPoolRegistryKey, ModRevision: current.ModRevision}},
			[]Mutation{{Type: MutationPut, Key: systemPoolRegistryKey, Value: value}},
		)
		if err != nil || !result.Succeeded {
			t.Fatalf("seed system pool = %#v, %v", result, err)
		}
		desired := runnerTestDesired(200, RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, TaskCreate, 201, "runner-network-capacity-key")
		if _, err := repository.CreateRunnerWithTask(
			ctx, config, desired, task, runnerTestMarker(task, desired),
		); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("CreateRunnerWithTask(non-Runner overlap) error = %v", err)
		}
	})

	t.Run("corrupt slot", func(t *testing.T) {
		ctx := context.Background()
		store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
		ownerID := ids.NewAt(ids.KindRunner, taskJournalTime(), 1900)
		value, err := encodeRunnerHostSlotRecord(RunnerHostSlotRecord{Slot: 1, RunnerID: ownerID})
		if err != nil {
			t.Fatalf("encodeRunnerHostSlotRecord() error = %v", err)
		}
		defer clear(value)
		result, err := store.Transact(ctx,
			[]Condition{{Key: runnerHostSlotKey(0)}},
			[]Mutation{{Type: MutationPut, Key: runnerHostSlotKey(0), Value: value}},
		)
		if err != nil || !result.Succeeded {
			t.Fatalf("seed corrupt host slot = %#v, %v", result, err)
		}
		desired := runnerTestDesired(202, RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, TaskCreate, 203, "runner-corrupt-slot-key")
		if _, err := repository.CreateRunnerWithTask(
			ctx, runnerTestAllocationConfig(), desired, task, runnerTestMarker(task, desired),
		); !errors.Is(err, errs.New(errs.KindInternal, "")) {
			t.Fatalf("CreateRunnerWithTask(corrupt slot) error = %v", err)
		}
	})
}

func TestRunnerNetworkPoolBootstrapReservationIsExactAndReplayable(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store, repository, _, _ := newRunnerRepositoryFixture(t)
	config := runnerTestAllocationConfig()
	if err := repository.EnsureRunnerNetworkPool(ctx, config); err != nil {
		t.Fatalf("EnsureRunnerNetworkPool(replay) error = %v", err)
	}
	entry := mustOptionalKey(t, store, systemPoolRegistryKey)
	registry, err := decodeSystemPoolRegistry(entry.Value)
	if err != nil || registry.RunnerNetworkPool != config.RunnerPool.String() || registry.Reservations == nil {
		t.Fatalf("bootstrap registry = %#v, %v", registry, err)
	}
	mismatch := config
	mismatch.RunnerPool = netip.MustParsePrefix("10.242.0.0/16")
	if err := repository.EnsureRunnerNetworkPool(ctx, mismatch); !errors.Is(err, errs.New(errs.KindStateConflict, "")) {
		t.Fatalf("EnsureRunnerNetworkPool(mismatch) error = %v", err)
	}
}

// Rationale: each Runner operation owns one exact route, operation key, 202
// response, and replay-target shape so protected replay cannot cross actions.
func TestRunnerMarkersAreOperationSpecific(t *testing.T) {
	t.Parallel()
	_, _, tenantID, _ := newRunnerRepositoryFixture(t)
	desired := runnerTestDesired(210, RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, TaskCreate, 211, "runner-marker-create-key")
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
	createTask = runnerTestTask(desired, TaskCreate, 211, "runner-marker-create-key")
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
	removeTask := runnerTestTask(desired, TaskRemove, 212, "runner-marker-remove-key")
	allocation, err := runnerTestAllocationConfig().HostPool.Allocation(
		0, netip.MustParsePrefix("10.240.0.0/29"),
	)
	if err != nil {
		t.Fatalf("Allocation() error = %v", err)
	}
	removeRecord, err := NewProvisioningRunner(desired, allocation, createTask.ID, createTask.CreatedAt)
	if err != nil {
		t.Fatalf("NewProvisioningRunner() error = %v", err)
	}
	removeTask.Params = runnerRemovalTaskParams(removeRecord)
	removeMarker := runnerTestMarker(removeTask, desired)
	if err := validateRunnerDeleteMarker(desired, removeTask, removeMarker); err != nil {
		t.Fatalf("validateRunnerDeleteMarker() error = %v", err)
	}
	removeTask.Params[RunnerHostSlotParam] = "0"
	if applies, err := taskOwnsRunner(removeTask); applies || !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("taskOwnsRunner(non-canonical slot) = %v, %v", applies, err)
	}
	removeTask.Params = runnerRemovalTaskParams(removeRecord)
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
	desired := runnerTestDesired(60, RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, TaskCreate, 61, "runner-lifecycle-key-01")
	if _, err := repository.CreateRunnerWithTask(
		ctx, config, desired, createTask, runnerTestMarker(createTask, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	ready := runnerTestFinishCreate(t, store, repository, createTask, TaskStatusCompleted)
	if ready.Record.ProvisioningState != RunnerProvisioningReady {
		t.Fatalf("Runner state = %s, want ready", ready.Record.ProvisioningState)
	}
	observedAt := taskJournalTime().Add(3 * time.Second)
	observation, err := repository.PutRunnerObservation(ctx, RunnerObservationRecord{
		RunnerID: desired.ID, Online: true, ObservedAt: observedAt,
	}, 0)
	if err != nil || !observation.Record.Online {
		t.Fatalf("PutRunnerObservation() = %#v, %v", observation, err)
	}

	removeTask := runnerTestTask(desired, TaskRemove, 62, "runner-removal-key-001")
	removeTask.Params = runnerRemovalTaskParams(ready.Record)
	removeMarker := runnerTestMarker(removeTask, desired)
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetRunner, TargetID: desired.ID,
		TargetRevision: ready.Revision, TaskID: removeTask.ID,
		Phase: DeletionPhaseFinalizing, CreatedAt: removeTask.CreatedAt, UpdatedAt: removeTask.CreatedAt,
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
		ctx, removeTask.ID, TaskStatusCompleted, removeTask.CreatedAt.Add(2*time.Second),
	)
	if err != nil || terminal.Record.Status != TaskStatusCompleted {
		t.Fatalf("AcknowledgeControllerTask(remove) = %#v, %v", terminal, err)
	}
	assertRunnerTerminalAggregate(t, store, terminal, removeMarker, desired.ID)
	if _, err := taskRepository.AcknowledgeControllerTask(
		ctx, removeTask.ID, TaskStatusCompleted, removeTask.CreatedAt.Add(2*time.Second),
	); err != nil {
		t.Fatalf("AcknowledgeControllerTask(remove replay) error = %v", err)
	}
	if _, err := repository.GetRunner(ctx, desired.ID); !errors.Is(err, errs.New(errs.KindRunnerNotFound, "")) {
		t.Fatalf("GetRunner(removed) error = %v, want runner.not_found", err)
	}
	if _, found, err := repository.GetRunnerObservation(ctx, desired.ID); err != nil || found {
		t.Fatalf("GetRunnerObservation(removed) found = %v, error = %v", found, err)
	}
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	locator, found, err := idempotency.ResolveReplayLocator(
		ctx,
		IdempotencyReplayTarget{Kind: IdempotencyReplayTargetRunner, ID: desired.ID},
		removeMarker.Locator.Method,
		removeMarker.Locator.Route,
		removeMarker.Locator.Key,
	)
	if err != nil || !found || locator != removeMarker.Locator {
		t.Fatalf("ResolveReplayLocator(remove) = %#v, %v, %v", locator, found, err)
	}
	replacement := runnerTestDesired(63, RunnerOwnerTenant, tenantID, tenantID)
	replacementTask := runnerTestTask(replacement, TaskCreate, 64, "runner-replace-key-001")
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
	desired := runnerTestDesired(70, RunnerOwnerTenant, tenantID, tenantID)
	createTask := runnerTestTask(desired, TaskCreate, 71, "runner-removal-source-key")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, createTask, runnerTestMarker(createTask, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	ready := runnerTestFinishCreate(t, store, repository, createTask, TaskStatusCompleted)
	allocation := ready.Record.Allocation
	removeTask := runnerTestTask(desired, TaskRemove, 72, "runner-removal-failure-key")
	removeTask.Params = runnerRemovalTaskParams(ready.Record)
	removeMarker := runnerTestMarker(removeTask, desired)
	tombstone := DeletionTombstoneRecord{
		TargetKind: DeletionTargetRunner, TargetID: desired.ID, TargetRevision: ready.Revision,
		TaskID: removeTask.ID, Phase: DeletionPhaseFinalizing,
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
		ctx, removeTask.ID, TaskStatusFailed, removeTask.CreatedAt.Add(2*time.Second),
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
	retryMarker.Locator.ScopeKind = IdempotencyScopeTenant
	retryMarker.Locator.ScopeID = tenantID
	if _, err := tasks.RetryTask(ctx, removeTask.ID, retryID, TaskActorOperator, retryMarker); err != nil {
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
	if err != nil || timedOut.Record.Status != TaskStatusTimedOut {
		t.Fatalf("GetTask(timed out) = %#v, %v", timedOut, err)
	}
	assertRunnerTerminalAggregate(t, store, timedOut, retryMarker, desired.ID)
	assertRunnerAllocationRetained(t, repository, ready.Record, allocation)

	abortID := ids.NewAt(ids.KindTask, removeTask.CreatedAt.Add(6*time.Second), 1501)
	abortMarker := pendingRetryMarker(
		timedOut.Record, abortID, removeTask.CreatedAt.Add(6*time.Second), "runner-remove-retry-key-02",
	)
	abortMarker.Locator.ScopeKind = IdempotencyScopeTenant
	abortMarker.Locator.ScopeID = tenantID
	if _, err := tasks.RetryTask(ctx, retryID, abortID, TaskActorOperator, abortMarker); err != nil {
		t.Fatalf("RetryTask(after timeout) error = %v", err)
	}
	aborted, err := tasks.AbortPendingTask(ctx, abortID, removeTask.CreatedAt.Add(7*time.Second))
	if err != nil || aborted.Record.Status != TaskStatusAborted {
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
		desired := runnerTestDesired(300, RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, TaskCreate, 301, "runner-high-slot-key-0001")
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
		contenderValue, err := encodeRunnerHostSlotRecord(RunnerHostSlotRecord{
			Slot: occupiedSlots, RunnerID: contenderID,
		})
		if err != nil {
			t.Fatalf("encodeRunnerHostSlotRecord() error = %v", err)
		}
		defer clear(contenderValue)
		audit := &runnerTransactionAuditStore{
			memoryHierarchyStore: store,
			allocationRaceKey:    runnerHostSlotKey(occupiedSlots),
			allocationRaceValue:  contenderValue,
		}
		repository, err := newRunnerRepository(audit)
		if err != nil {
			t.Fatalf("newRunnerRepository() error = %v", err)
		}
		desired := runnerTestDesired(302, RunnerOwnerTenant, tenantID, tenantID)
		task := runnerTestTask(desired, TaskCreate, 303, "runner-high-slot-race-key")
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
		entry := mustOptionalKey(t, store, runnerHostSlotKey(occupiedSlots))
		owned, decodeErr := decodeRunnerHostSlotRecord(entry.Value)
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
		status TaskStatus
		key    func(RunnerRecord) string
	}{
		{
			name: "owner index on success", status: TaskStatusCompleted,
			key: func(record RunnerRecord) string {
				return runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID)
			},
		},
		{name: "tenant quota on failure", status: TaskStatusFailed, key: func(record RunnerRecord) string {
			return runnerTenantQuotaKey(record.Desired.TenantID)
		}},
		{name: "host slot on success", status: TaskStatusCompleted, key: func(record RunnerRecord) string {
			return runnerHostSlotKey(record.Allocation.Slot)
		}},
		{name: "system pool on failure", status: TaskStatusFailed, key: func(RunnerRecord) string {
			return systemPoolRegistryKey
		}},
	}
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
			desired := runnerTestDesired(320+index, RunnerOwnerTenant, tenantID, tenantID)
			task := runnerTestTask(
				desired,
				TaskCreate,
				330+index,
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
			if test.status == TaskStatusCompleted {
				runnerTestRecordReadinessProof(t, repository, task)
			}
			connectorDeletionDeleteKey(t, store, test.key(current.Record))
			if _, err := tasks.AcknowledgeControllerTask(
				ctx, task.ID, test.status, task.CreatedAt.Add(2*time.Second),
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("AcknowledgeControllerTask() error = %v", err)
			}
			retained, err := repository.GetRunner(ctx, desired.ID)
			if err != nil || retained.Record.ProvisioningState != RunnerProvisioningProvisioning {
				t.Fatalf("Runner after rejected terminalization = %#v, %v", retained, err)
			}
		})
	}
}

// Rationale: allocation ownership can change after acknowledgement reads; the
// terminal transaction must compare those exact revisions and leave the Runner
// provisioning when the CAS loses for either terminal result.
func TestRunnerCreationTerminalizationCASFencesAllocationRace(t *testing.T) {
	for index, status := range []TaskStatus{TaskStatusCompleted, TaskStatusFailed} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			store, repository, tenantID, _ := newRunnerRepositoryFixture(t)
			desired := runnerTestDesired(340+index, RunnerOwnerTenant, tenantID, tenantID)
			task := runnerTestTask(desired, TaskCreate, 350+index, "runner-terminal-race-key-0"+string(rune('0'+index)))
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
				terminalRaceKey:      runnerHostSlotKey(current.Record.Allocation.Slot),
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
			if status == TaskStatusCompleted {
				runnerTestRecordReadinessProof(t, repository, task)
			}
			if _, err := tasks.AcknowledgeControllerTask(
				ctx, task.ID, status, task.CreatedAt.Add(2*time.Second),
			); !errors.Is(err, errs.New(errs.KindInternal, "")) {
				t.Fatalf("AcknowledgeControllerTask(race) error = %v", err)
			}
			retained, err := repository.GetRunner(ctx, desired.ID)
			if err != nil || retained.Record.ProvisioningState != RunnerProvisioningProvisioning {
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
	desired := runnerTestDesired(360, RunnerOwnerTenant, tenantID, tenantID)
	source := runnerTestTask(desired, TaskCreate, 361, "runner-replay-source-key")
	if _, err := repository.CreateRunnerWithTask(
		ctx, runnerTestAllocationConfig(), desired, source, runnerTestMarker(source, desired),
	); err != nil {
		t.Fatalf("CreateRunnerWithTask() error = %v", err)
	}
	runnerTestFinishCreate(t, store, repository, source, TaskStatusFailed)
	retry := runnerTestTask(desired, TaskCreate, 362, source.IdempotencyKey)
	retry.RetryOf = source.ID
	retry.OperationID = source.OperationID
	marker := runnerTestMarker(retry, desired)
	marker.Locator.Key = "runner-replay-request-key"
	if _, err := repository.RetryRunnerCreationWithTask(ctx, source.ID, retry, marker); err != nil {
		t.Fatalf("RetryRunnerCreationWithTask() error = %v", err)
	}
	result, err := store.Transact(ctx, nil, []Mutation{
		{Type: MutationDelete, Key: taskKey(source.ID)},
		{Type: MutationDelete, Key: runnerKey(desired.ID)},
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

// Rationale: removal retry must distinguish missing or corrupt durable owner
// evidence from a legitimate hierarchy tombstone so operators see not-found or
// internal corruption instead of an inaccurate resource-in-use conflict.
func TestRunnerRemovalRetryClassifiesOwnerEvidenceAndDeletionFences(t *testing.T) {
	tests := []struct {
		name       string
		ownerKind  RunnerOwnerKind
		mutate     func(*testing.T, *memoryHierarchyStore, RunnerRecord)
		wantedKind errs.Kind
	}{
		{
			name: "missing tenant", ownerKind: RunnerOwnerTenant, wantedKind: errs.KindTenantNotFound,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record RunnerRecord) {
				connectorDeletionDeleteKey(t, store, tenantKey(record.Desired.TenantID))
			},
		},
		{
			name: "corrupt tenant", ownerKind: RunnerOwnerTenant, wantedKind: errs.KindInternal,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record RunnerRecord) {
				connectorDeletionPutKey(t, store, tenantKey(record.Desired.TenantID), []byte("corrupt"))
			},
		},
		{
			name: "tenant tombstone", ownerKind: RunnerOwnerTenant, wantedKind: errs.KindResourceInUse,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record RunnerRecord) {
				connectorDeletionPutKey(
					t,
					store,
					deletionTombstoneKey(string(DeletionTargetTenant), record.Desired.TenantID),
					[]byte("fenced"),
				)
			},
		},
		{
			name: "missing project", ownerKind: RunnerOwnerProject, wantedKind: errs.KindProjectNotFound,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record RunnerRecord) {
				connectorDeletionDeleteKey(t, store, projectKey(record.Desired.OwnerID))
			},
		},
		{
			name: "project tombstone", ownerKind: RunnerOwnerProject, wantedKind: errs.KindResourceInUse,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record RunnerRecord) {
				connectorDeletionPutKey(
					t,
					store,
					deletionTombstoneKey(string(DeletionTargetProject), record.Desired.OwnerID),
					[]byte("fenced"),
				)
			},
		},
		{
			name: "missing owner index", ownerKind: RunnerOwnerTenant, wantedKind: errs.KindInternal,
			mutate: func(t *testing.T, store *memoryHierarchyStore, record RunnerRecord) {
				connectorDeletionDeleteKey(t, store, runnerOwnerKey(
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
			if test.ownerKind == RunnerOwnerProject {
				ownerID = projectID
			}
			desired := runnerTestDesired(380+index, test.ownerKind, ownerID, tenantID)
			createTask := runnerTestTask(
				desired, TaskCreate, 390+index, "runner-owner-evidence-create-0"+string(rune('0'+index)),
			)
			if _, err := repository.CreateRunnerWithTask(
				ctx, runnerTestAllocationConfig(), desired, createTask, runnerTestMarker(createTask, desired),
			); err != nil {
				t.Fatalf("CreateRunnerWithTask() error = %v", err)
			}
			ready := runnerTestFinishCreate(t, store, repository, createTask, TaskStatusCompleted)
			removeTask := runnerTestTask(
				desired, TaskRemove, 400+index, "runner-owner-evidence-remove-0"+string(rune('0'+index)),
			)
			removeTask.Params = runnerRemovalTaskParams(ready.Record)
			removeMarker := runnerTestMarker(removeTask, desired)
			tombstone := DeletionTombstoneRecord{
				TargetKind: DeletionTargetRunner, TargetID: desired.ID, TargetRevision: ready.Revision,
				TaskID: removeTask.ID, Phase: DeletionPhaseFinalizing,
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
				ctx, removeTask.ID, TaskStatusFailed, removeTask.CreatedAt.Add(2*time.Second),
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
			if test.ownerKind == RunnerOwnerProject {
				retryMarker.Locator.ScopeKind = IdempotencyScopeProject
				retryMarker.Locator.ScopeID = projectID
			} else {
				retryMarker.Locator.ScopeKind = IdempotencyScopeTenant
				retryMarker.Locator.ScopeID = tenantID
			}
			if _, err := tasks.RetryTask(
				ctx, removeTask.ID, retryID, TaskActorOperator, retryMarker,
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
	status TaskStatus,
) Versioned[RunnerRecord] {
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
	if status == TaskStatusCompleted {
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
) Versioned[RunnerReadinessProofRecord] {
	t.Helper()
	ctx := context.Background()
	current, err := repository.GetRunner(ctx, task.Target)
	if err != nil {
		t.Fatalf("GetRunner(readiness) error = %v", err)
	}
	if current.Record.ContainerID == "" {
		containerID := strings.Repeat("a", runnerContainerIDEncodedLength)
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
	runner Versioned[RunnerRecord],
	task TaskRecord,
	marker IdempotencyMarker,
) {
	t.Helper()
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey() error = %v", err)
	}
	record := runner.Record
	keys := []string{
		taskKey(task.ID),
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		runnerKey(record.Desired.ID),
		runnerLifecycleKey(record.Desired.ID),
		runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, record.Desired.ID),
		runnerTenantQuotaKey(record.Desired.TenantID),
		runnerHostSlotKey(record.Allocation.Slot),
		systemPoolRegistryKey,
		markerKey,
	}
	stored, err := store.GetMany(context.Background(), GetManyRequest{Keys: keys, Revision: runner.Revision})
	if err != nil || stored == nil || len(stored.Values) != len(keys) {
		t.Fatalf("GetMany(create aggregate) = %#v, %v", stored, err)
	}
	for index, value := range stored.Values {
		if value == nil || value.ModRevision != runner.Revision {
			t.Fatalf("aggregate key %s = %#v, want create revision %d", keys[index], value, runner.Revision)
		}
	}
	storedTask, err := decodeTaskRecord(stored.Values[0].Value)
	if err != nil || len(storedTask.Params) != 2 ||
		storedTask.Params[RunnerRegistrationTokenPresentParam] != "true" {
		t.Fatalf("stored create Task = %#v, %v", storedTask, err)
	}
}

func assertRunnerRetryAggregate(
	t *testing.T,
	store *memoryHierarchyStore,
	runner Versioned[RunnerRecord],
	task TaskRecord,
	marker IdempotencyMarker,
) {
	t.Helper()
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(retry) error = %v", err)
	}
	keys := []string{
		taskKey(task.ID),
		taskOperationIndexKey(task.OperationID, task.ID),
		taskActiveOperationKey(task.OperationID),
		taskQueueKey(task.Executor, task.ID),
		runnerKey(runner.Record.Desired.ID),
		runnerLifecycleKey(runner.Record.Desired.ID),
		markerKey,
	}
	stored, err := store.GetMany(
		context.Background(),
		GetManyRequest{Keys: keys, Revision: runner.ReadRevision},
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
	allocation, err := (&RunnerRepository{store: store}).readRunnerAllocationEvidence(
		context.Background(), runner.Record, runner.ReadRevision,
	)
	if err != nil || allocation.owner == nil || allocation.slug == nil || allocation.quota == nil ||
		allocation.host == nil ||
		allocation.system == nil {
		t.Fatalf("readRunnerAllocationEvidence(retry) = %#v, %v", allocation, err)
	}
}

func assertRunnerTerminalAggregate(
	t *testing.T,
	store *memoryHierarchyStore,
	terminal Versioned[TaskRecord],
	marker IdempotencyMarker,
	runnerID string,
) {
	t.Helper()
	markerKey, err := idempotencyMarkerKey(marker.Locator)
	if err != nil {
		t.Fatalf("idempotencyMarkerKey(terminal) error = %v", err)
	}
	keys := []string{
		taskKey(terminal.Record.ID),
		taskActiveOperationKey(terminal.Record.OperationID),
		markerKey,
		deletionTombstoneKey(string(DeletionTargetRunner), runnerID),
		runnerRemovalIntentKey(runnerID),
	}
	stored, err := store.GetMany(context.Background(), GetManyRequest{Keys: keys})
	if err != nil || stored == nil || len(stored.Values) != len(keys) {
		t.Fatalf("GetMany(terminal aggregate) = %#v, %v", stored, err)
	}
	if stored.Values[0] == nil || stored.Values[0].ModRevision != terminal.Revision ||
		stored.Values[1] != nil || stored.Values[2] == nil ||
		stored.Values[2].ModRevision != terminal.Revision || stored.Values[3] != nil ||
		stored.Values[4] != nil {
		t.Fatalf("terminal aggregate values = %#v, terminal revision %d", stored.Values, terminal.Revision)
	}
	storedMarker, err := decodeIdempotencyMarker(stored.Values[2].Value, marker.Locator)
	if err != nil {
		t.Fatalf("decodeIdempotencyMarker(terminal) error = %v", err)
	}
	defer clear(storedMarker.Intent.Ciphertext)
	defer clear(storedMarker.Response.Body)
	wantState := IdempotencyMarkerFailed
	if terminal.Record.Status == TaskStatusCompleted {
		wantState = IdempotencyMarkerCompleted
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
	if terminal.Record.Type == TaskRemove {
		evidence, err := decodeRunnerRemovalTaskEvidence(terminal.Record)
		if err != nil || terminal.Record.Params[RunnerHostSlotParam] != runnerHostSlotSegment(evidence.hostSlot) {
			t.Fatalf("runner removal Task Params = %#v, %v", terminal.Record.Params, err)
		}
	}
}
