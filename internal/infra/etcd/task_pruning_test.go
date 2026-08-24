package etcd

import (
	"context"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
)

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
	assertTaskLifecycleValue(t, store, taskRetentionIndexKey(task.ID, *terminal.Record.RetainUntil), false)
}

func TestTaskPruningCheckpointsMaximumTransactionBatch(t *testing.T) {
	// Rationale: 48 events require multiple deletion transactions and prove the
	// first 47-record checkpoint uses, but never exceeds, the 96-operation cap.
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
	if _, found, err := repository.ClaimNextTask(ctx, agentID, 4, now.Add(time.Second)); err != nil || !found {
		t.Fatalf("ClaimNextTask() found/error = %v/%v", found, err)
	}
	for ordinal := uint64(1); ordinal <= 48; ordinal++ {
		if _, err := repository.AppendTaskEvent(
			ctx,
			taskEventInput(task.ID, ordinal, TaskEventStateRunning),
			now.Add(time.Duration(ordinal+1)*time.Second),
		); err != nil {
			t.Fatalf("AppendTaskEvent(%d) error = %v", ordinal, err)
		}
	}
	terminalAt := now.Add(2 * time.Minute)
	terminal, err := repository.AcknowledgeTask(
		ctx, agentID, 4, task.ID, TaskStatusCompleted, completedComposeTaskResult(), terminalAt,
	)
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
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskWorkspacePlatformIndexKey(task.ID), false)
	assertTaskLifecycleValue(t, store.memoryTaskStore, taskPruneIntentKey(task.ID), false)
	if terminal.Record.EventCount != 48 {
		t.Fatalf("terminal EventCount = %d, want 48", terminal.Record.EventCount)
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
		TaskID: firstTaskID, AttachPlanID: planID,
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
		TaskID: lastTaskID, AttachPlanID: planID,
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
		PlanID: planID, AttachID: ids.NewAt(ids.KindAttach, now, 1710),
		TenantID: ids.NewAt(ids.KindTenant, now, 1711), TenantSlug: "tenant",
		ProjectID: ids.NewAt(ids.KindProject, now, 1712), ProjectSlug: "project",
		EnvironmentID: ids.NewAt(ids.KindEnvironment, now, 1713), EnvironmentName: "main",
		AuthorizedVolumeDir: "/srv/groundplane/env", BackingServiceID: ids.NewAt(ids.KindService, now, 1714),
		AdapterKey: "manual", BlueprintRevisionID: ids.NewAt(ids.KindTask, now, 1715),
		ArtifactID: ids.NewAt(ids.KindConfig, now, 1716), RenderGeneration: 1,
		Services: []EnvironmentComposeIdentity{{ID: ids.NewAt(ids.KindService, now, 1717), Name: "app"}},
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
