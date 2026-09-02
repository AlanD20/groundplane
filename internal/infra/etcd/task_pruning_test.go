package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

func TestTaskPruningRetainsOwnedEnvironmentDeletionRetrySource(t *testing.T) {
	// Rationale: expired terminal Task history is still live retry authority
	// while an Environment deletion tombstone, operation lock, and cleanup
	// intent name that attempt, so pruning must retain the source Task.
	t.Parallel()
	ctx := context.Background()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	terminalAt := fixture.now.Add(time.Second)
	terminal, err := fixture.tasks.AbortPendingTask(ctx, fixture.task.ID, terminalAt)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	pruneAt := terminalAt.Add(TaskRetention).Add(time.Nanosecond)
	idempotency, err := newIdempotencyRepository(fixture.store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(marker) = %d, %v", count, err)
	}
	if count, err := fixture.tasks.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 0 {
		t.Fatalf("PruneExpiredTasks(owned deletion) = %d, %v", count, err)
	}
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		taskRetentionIndexKey(terminal.Record.ID, *terminal.Record.RetainUntil),
		false,
	)
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(terminal.Record.ID), true)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
		true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		environmentOperationLockKey(fixture.environment.Record.ID),
		true,
	)
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		environmentDeletionIntentKey(fixture.task.OperationID),
		true,
	)
}

func TestTaskPruningRetainedEnvironmentDeletionDoesNotStarveLaterTask(t *testing.T) {
	// Rationale: a deletion-owned Task at the global retention head must move
	// out of discovery durably so a later unrelated expired Task can progress.
	t.Parallel()
	ctx := context.Background()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	deletionTerminal, err := fixture.tasks.AbortPendingTask(
		ctx,
		fixture.task.ID,
		fixture.task.CreatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask(deletion) error = %v", err)
	}
	unrelated := validTaskRecord(fixture.task.CreatedAt.Add(time.Minute))
	createLifecycleTask(t, fixture.tasks, unrelated)
	unrelatedTerminal, err := fixture.tasks.AbortPendingTask(
		ctx,
		unrelated.ID,
		unrelated.CreatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask(unrelated) error = %v", err)
	}
	pruneAt := unrelatedTerminal.Record.RetainUntil.Add(time.Nanosecond)
	idempotency, err := newIdempotencyRepository(fixture.store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	for {
		count, pruneErr := idempotency.PruneExpired(ctx, pruneAt)
		if pruneErr != nil {
			t.Fatalf("PruneExpired(marker) error = %v", pruneErr)
		}
		if count == 0 {
			break
		}
	}
	if count, err := fixture.tasks.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 0 {
		t.Fatalf("PruneExpiredTasks(retained head) = %d, %v", count, err)
	}
	assertEnvironmentDeletionCompanion(
		t,
		fixture.store,
		taskRetentionIndexKey(
			deletionTerminal.Record.ID,
			*deletionTerminal.Record.RetainUntil,
		),
		false,
	)
	if count, err := fixture.tasks.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(unrelated) = %d, %v", count, err)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(deletionTerminal.Record.ID), true)
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(unrelated.ID), false)
}

func TestTaskPruningEnvironmentDeletionRetryRequeuesSource(t *testing.T) {
	// Rationale: once pruning detaches a retained source from discovery, retry
	// must re-enqueue it in the same ownership-transfer transaction; the source
	// and later unrelated work remain collectible while the retry stays active.
	t.Parallel()
	ctx := context.Background()
	fixture := newEnvironmentDeletionLockFixture(t)
	fixture.mustBegin(t)
	terminal, err := fixture.tasks.AbortPendingTask(
		ctx,
		fixture.task.ID,
		fixture.task.CreatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask(source) error = %v", err)
	}
	firstPruneAt := terminal.Record.RetainUntil.Add(time.Nanosecond)
	idempotency, err := newIdempotencyRepository(fixture.store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	if count, err := idempotency.PruneExpired(ctx, firstPruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(source marker) = %d, %v", count, err)
	}
	if count, err := fixture.tasks.PruneExpiredTasks(ctx, firstPruneAt); err != nil || count != 0 {
		t.Fatalf("PruneExpiredTasks(retained source) = %d, %v", count, err)
	}
	retentionKey := taskRetentionIndexKey(terminal.Record.ID, *terminal.Record.RetainUntil)
	assertEnvironmentDeletionCompanion(t, fixture.store, retentionKey, false)

	retryAt := firstPruneAt.Add(time.Second)
	retryID := ids.NewAt(ids.KindTask, retryAt, 1500)
	marker := pendingRetryMarker(
		terminal.Record,
		retryID,
		retryAt,
		"environment-prune-retry-key-0001",
	)
	retryResult, err := fixture.tasks.RetryTask(
		ctx,
		terminal.Record.ID,
		retryID,
		TaskActorOperator,
		marker,
	)
	if err != nil {
		t.Fatalf("RetryTask() error = %v", err)
	}
	outcome, _, conflict, err := retryResult.Classify()
	if err != nil || conflict != nil || outcome != IdempotencyKnownApplied {
		t.Fatalf("RetryTask() = %#v, %v", retryResult, err)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, retentionKey, true)
	unrelated := validTaskRecord(retryAt.Add(time.Minute))
	createLifecycleTask(t, fixture.tasks, unrelated)
	unrelatedTerminal, err := fixture.tasks.AbortPendingTask(
		ctx,
		unrelated.ID,
		unrelated.CreatedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AbortPendingTask(unrelated) error = %v", err)
	}
	finalPruneAt := unrelatedTerminal.Record.RetainUntil.Add(time.Nanosecond)
	if count, err := idempotency.PruneExpired(ctx, finalPruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(unrelated marker) = %d, %v", count, err)
	}
	if count, err := fixture.tasks.PruneExpiredTasks(
		ctx,
		finalPruneAt,
	); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(source with active retry) = %d, %v", count, err)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(terminal.Record.ID), false)
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(retryID), true)
	if count, err := fixture.tasks.PruneExpiredTasks(ctx, finalPruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(unrelated after source) = %d, %v", count, err)
	}
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(unrelated.ID), false)
	assertEnvironmentDeletionCompanion(t, fixture.store, taskKey(retryID), true)
	active := fixture.mustGet(t, taskActiveOperationKey(fixture.task.OperationID))
	activeTaskID, err := decodeTaskReference(active.Value)
	if err != nil || activeTaskID != retryID {
		t.Fatalf("active Environment deletion retry = %q, %v", activeTaskID, err)
	}
	tombstoneEntry := fixture.mustGet(
		t,
		deletionTombstoneKey(string(DeletionTargetEnvironment), fixture.environment.Record.ID),
	)
	tombstone, err := decodeDeletionTombstone(tombstoneEntry.Value)
	if err != nil || tombstone.TaskID != retryID {
		t.Fatalf("Environment deletion retry tombstone = %#v, %v", tombstone, err)
	}
	lockEntry := fixture.mustGet(t, environmentOperationLockKey(fixture.environment.Record.ID))
	lock, err := decodeEnvironmentOperationLock(lockEntry, fixture.environment.Record.ID)
	if err != nil || lock.TaskID != retryID {
		t.Fatalf("Environment deletion retry lock = %#v, %v", lock, err)
	}
	intentEntry := fixture.mustGet(t, environmentDeletionIntentKey(fixture.task.OperationID))
	intent, err := decodeEnvironmentDeletionIntent(intentEntry.Value)
	if err != nil || intent.TaskID != retryID {
		t.Fatalf("Environment deletion retry intent = %#v, %v", intent, err)
	}
}

func TestTaskPruningWaitsForMarkerAndRemovesComponentIntent(t *testing.T) {
	// Rationale: ADR 0021 orders marker deletion before Task deletion, while
	// ADR 0035 requires the retained attempt-owned Component intent to expire
	// with the public Task.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(50 * time.Second)
	environmentID := ids.NewAt(ids.KindEnvironment, now, 1501)
	task := validTaskRecord(now)
	task.Type = TaskUpdate
	task.Target = environmentID
	pinComponentTaskDesiredRevision(&task)
	createLifecycleTask(t, repository, task)
	records := componentTaskLifecycleRecords(t, environmentID, now, true)
	seedComponentTaskLifecycle(t, store, task, records)
	terminalAt := now.Add(time.Second)
	terminal, err := repository.AbortPendingTask(ctx, task.ID, terminalAt)
	if err != nil {
		t.Fatalf("AbortPendingTask() error = %v", err)
	}
	pruneAt := terminalAt.Add(TaskRetention).Add(time.Nanosecond)
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 0 {
		t.Fatalf("PruneExpiredTasks(before marker) = %d, %v", count, err)
	}
	assertTaskLifecycleValue(t, store, taskKey(task.ID), true)
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(marker) = %d, %v", count, err)
	}
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks() = %d, %v", count, err)
	}
	assertTaskLifecycleValue(t, store, taskKey(task.ID), false)
	assertTaskLifecycleValue(t, store, taskOperationIndexKey(task.OperationID, task.ID), false)
	assertTaskLifecycleValue(t, store, taskWorkspacePlatformIndexKey(task.ID), false)
	assertTaskLifecycleValue(t, store, componentTaskIntentKey(task.ID), false)
	assertTaskLifecycleValue(t, store, taskPruneIntentKey(task.ID), false)
	assertTaskLifecycleValue(
		t,
		store,
		taskRetentionIndexKey(task.ID, *terminal.Record.RetainUntil),
		false,
	)
}

func TestTaskPruningCheckpointsMaximumTransactionBatch(t *testing.T) {
	// Rationale: 128 events require multiple deletion transactions and prove the
	// first 127-record checkpoint uses, but never exceeds, the 256-operation cap.
	t.Parallel()
	ctx := context.Background()
	store := &taskPruneOperationStore{memoryTaskStore: newMemoryTaskStore()}
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(70 * time.Second)
	task := validTaskRecord(now)
	createLifecycleTask(t, repository, task)
	agentID := ids.NewAt(ids.KindAgent, now, 1601)
	claim, found, err := repository.ClaimNextTask(ctx, agentID, 4, now.Add(time.Second))
	if err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	for ordinal := uint64(1); ordinal <= 128; ordinal++ {
		input := taskEventInput(task.ID, ordinal, TaskEventStateRunning)
		input.Identity.AssignmentID = claim.Assignment.Record.AssignmentID
		input.Identity.AgentID = agentID
		input.Identity.AgentGeneration = 4
		if _, err := repository.AppendTaskEvent(
			ctx,
			input,
			now.Add(time.Duration(ordinal+1)*time.Second),
		); err != nil {
			t.Fatalf("AppendTaskEvent(%d) error = %v", ordinal, err)
		}
	}
	terminalAt := now.Add(3 * time.Minute)
	terminal, err := repository.AcknowledgeTask(
		ctx, agentID, 4, task.ID, taskAssignmentIDForTest(t, repository,
			task.ID),
		TaskStatusCompleted, completedComposeTaskResult(), terminalAt)

	if err != nil {
		t.Fatalf("AcknowledgeTask() error = %v", err)
	}
	pruneAt := terminalAt.Add(TaskRetention).Add(time.Nanosecond)
	idempotency, err := newIdempotencyRepository(store)
	if err != nil {
		t.Fatalf("newIdempotencyRepository() error = %v", err)
	}
	if count, err := idempotency.PruneExpired(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpired(marker) = %d, %v", count, err)
	}
	store.trackPruning = true
	if count, err := repository.PruneExpiredTasks(ctx, pruneAt); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks() = %d, %v", count, err)
	}
	if store.maximumOperations != maximumTransactionOperations {
		t.Fatalf(
			"maximum prune transaction operations = %d, want %d",
			store.maximumOperations,
			maximumTransactionOperations,
		)
	}
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskKey(task.ID), false)
	assertTaskLifecycleValue(
		t,
		store.memoryTaskStore,
		taskWorkspacePlatformIndexKey(task.ID),
		false,
	)
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskPruneIntentKey(task.ID), false)
	if terminal.Record.EventCount != 128 {
		t.Fatalf("terminal EventCount = %d, want 128", terminal.Record.EventCount)
	}
	for _, prefix := range []string{taskEventScopePrefix(task.ID), taskEventDedupScopePrefix(task.ID)} {
		page, err := store.Range(ctx, RangeRequest{Prefix: prefix, Limit: 1})
		if err != nil || len(page.Values) != 0 {
			t.Fatalf("remaining Task subordinate records under %s = %#v, %v", prefix, page, err)
		}
	}
}

func TestTaskPruningRemovesAttachInputOnlyAfterFinalPlanReference(t *testing.T) {
	// Rationale: Attach retries share one immutable plan input, so pruning one
	// attempt must retain it while another attempt reference remains.
	t.Parallel()
	ctx := context.Background()
	store := newMemoryTaskStore()
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	now := taskJournalTime().Add(90 * time.Second)
	planID := ids.NewAt(ids.KindPlan, now, 1701)
	firstTaskID := ids.NewAt(ids.KindTask, now, 1702)
	lastTaskID := ids.NewAt(ids.KindTask, now, 1703)
	input := taskPruningAttachRenderInput(now, planID)
	inputValue, err := encodeAttachTaskRenderInput(input)
	if err != nil {
		t.Fatalf("encodeAttachTaskRenderInput() error = %v", err)
	}
	seedTaskRepositoryValue(t, store, attachTaskRenderInputKey(planID), inputValue)
	lastReference, err := encodeTaskReference(lastTaskID)
	if err != nil {
		t.Fatalf("encodeTaskReference() error = %v", err)
	}
	lastReferenceKey := attachTaskPlanReferenceKey(planID, lastTaskID)
	seedTaskRepositoryValue(t, store, lastReferenceKey, lastReference)
	firstIntentValue, err := encodeTaskPruneIntent(taskPruneIntent{
		TaskID: firstTaskID, TaskRevision: 1,
		BackupCheckpointCursorsComplete:        true,
		BackupCheckpointDeduplicationsComplete: true,
		TaskPrimaryDeleted:                     true,
		AttachPlanID:                           planID,
	})
	if err != nil {
		t.Fatalf("encodeTaskPruneIntent(first) error = %v", err)
	}
	seedTaskRepositoryValue(t, store, taskPruneIntentKey(firstTaskID), firstIntentValue)
	if count, err := repository.PruneExpiredTasks(ctx, now); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(first) = %d, %v", count, err)
	}
	assertTaskLifecycleValue(t, store, attachTaskRenderInputKey(planID), true)

	referenceResult, err := store.Get(ctx, lastReferenceKey)
	if err != nil || referenceResult.Entry == nil {
		t.Fatalf("Get(last plan reference) = %#v, %v", referenceResult, err)
	}
	lastIntentValue, err := encodeTaskPruneIntent(taskPruneIntent{
		TaskID: lastTaskID, TaskRevision: 1,
		BackupCheckpointCursorsComplete:        true,
		BackupCheckpointDeduplicationsComplete: true,
		TaskPrimaryDeleted:                     true,
		AttachPlanID:                           planID,
	})
	if err != nil {
		t.Fatalf("encodeTaskPruneIntent(last) error = %v", err)
	}
	transaction, err := store.Transact(ctx, []Condition{
		{Key: lastReferenceKey, ModRevision: referenceResult.Entry.ModRevision},
		{Key: taskPruneIntentKey(lastTaskID)},
	}, []Mutation{
		{Type: MutationDelete, Key: lastReferenceKey},
		{Type: MutationPut, Key: taskPruneIntentKey(lastTaskID), Value: lastIntentValue},
	})
	if err != nil || !transaction.Succeeded {
		t.Fatalf("publish final prune intent = %#v, %v", transaction, err)
	}
	if count, err := repository.PruneExpiredTasks(ctx, now); err != nil || count != 1 {
		t.Fatalf("PruneExpiredTasks(last) = %d, %v", count, err)
	}
	assertTaskLifecycleValue(t, store, attachTaskRenderInputKey(planID), false)
}

func taskPruningAttachRenderInput(now time.Time, planID string) AttachTaskRenderInput {
	return AttachTaskRenderInput{
		PlanID:              planID,
		AttachID:            ids.NewAt(ids.KindAttach, now, 1710),
		AttachName:          "database",
		TenantID:            ids.NewAt(ids.KindTenant, now, 1711),
		TenantSlug:          "tenant",
		ProjectID:           ids.NewAt(ids.KindProject, now, 1712),
		ProjectSlug:         "project",
		EnvironmentID:       ids.NewAt(ids.KindEnvironment, now, 1713),
		EnvironmentName:     "main",
		AuthorizedVolumeDir: "/srv/groundplane/env",
		BackingServiceID:    ids.NewAt(ids.KindService, now, 1714),
		BackingProjectID:    ids.NewAt(ids.KindProject, now, 1718),
		AdapterKey:          "manual",
		DesiredRevisionID:   ids.NewAt(ids.KindTask, now, 1715),
		ArtifactID:          ids.NewAt(ids.KindConfig, now, 1716),
		RenderGeneration:    1,
		Services: []AttachTaskServiceSnapshot{
			{ID: ids.NewAt(ids.KindService, now, 1717), Name: "app"},
		},
		ConsumerServiceIDs: []string{ids.NewAt(ids.KindService, now, 1717)},
	}
}

type taskPruneOperationStore struct {
	*memoryTaskStore
	trackPruning      bool
	maximumOperations int
}

func (store *taskPruneOperationStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if store.trackPruning && len(conditions)+len(mutations) > store.maximumOperations {
		store.maximumOperations = len(conditions) + len(mutations)
	}
	return store.memoryTaskStore.Transact(ctx, conditions, mutations)
}
