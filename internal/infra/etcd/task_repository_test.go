package etcd

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskRepositoryKeysMatchTheApprovedLayout(t *testing.T) {
	// Rationale: a path spelling or padding drift creates a second durable
	// schema. The accepted v1 keys have exactly one canonical representation.
	now := taskJournalTime()
	taskID := ids.NewAt(ids.KindTask, now, 21)
	operationID := ids.NewAt(ids.KindOperation, now, 22)
	stepID := ids.NewAt(ids.KindStep, now, 23)
	identity := TaskEventIdentity{TaskID: taskID, StepID: stepID, Attempt: 3, Ordinal: 17}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 24)

	wants := map[string]string{
		taskKey(taskID): "/v1/tasks/" + taskID,
		taskOperationIndexKey(operationID, taskID):         "/v1/indexes/tasks/operation/" + operationID + "/" + taskID,
		taskActiveOperationKey(operationID):                "/v1/indexes/tasks/active-operation/" + operationID,
		taskEventKey(taskID, 42):                           "/v1/runtime/task-events/" + taskID + "/00000000000000000042",
		taskEventDedupKey(identity):                        "/v1/runtime/task-event-dedup/" + taskID + "/" + stepID + "/3/17",
		deletionTombstoneKey("environment", environmentID): "/v1/runtime/deletions/environment/" + environmentID,
	}
	for got, want := range wants {
		if got != want {
			t.Fatalf("key = %q, want %q", got, want)
		}
	}
	if sequence, err := taskEventSequenceFromKey(taskID, taskEventKey(taskID, 42)); err != nil || sequence != 42 {
		t.Fatalf("taskEventSequenceFromKey() = %d, %v, want 42", sequence, err)
	}
}

func TestTaskRepositoryAppendIsRestartSafeAndDeduplicated(t *testing.T) {
	// Rationale: Agent reconnects replay the same stable event identity. A new
	// Controller process must return the original sequence without a new write.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	first, err := repository.AppendTaskEvent(ctx, input, taskJournalTime().Add(time.Second))
	if err != nil {
		t.Fatalf("AppendTaskEvent(first) error = %v", err)
	}
	if first.Sequence != 1 || first.Duplicate {
		t.Fatalf("AppendTaskEvent(first) = %#v", first)
	}

	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository(restart) error = %v", err)
	}
	replay, err := restarted.AppendTaskEvent(ctx, input, taskJournalTime().Add(2*time.Second))
	if err != nil {
		t.Fatalf("AppendTaskEvent(replay) error = %v", err)
	}
	if replay.Sequence != first.Sequence || !replay.Duplicate {
		t.Fatalf("AppendTaskEvent(replay) = %#v, want duplicate sequence 1", replay)
	}

	mismatch := input
	mismatch.Payload = json.RawMessage(`{"message":"different"}`)
	if _, err := restarted.AppendTaskEvent(
		ctx,
		mismatch,
		taskJournalTime().Add(3*time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("AppendTaskEvent(mismatch) error = %v, want internal", err)
	}
}

func TestTaskRepositoryConcurrentAppendsAllocateUniqueSequences(t *testing.T) {
	// Rationale: concurrent Agent workers may report distinct events for one
	// Task. CAS retries must serialize the durable sequence without duplicates.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}

	const count = 24
	sequences := make(chan uint64, count)
	errorsFound := make(chan error, count)
	var workers sync.WaitGroup
	for ordinal := 1; ordinal <= count; ordinal++ {
		ordinal := ordinal
		workers.Add(1)
		go func() {
			defer workers.Done()
			result, err := repository.AppendTaskEvent(
				ctx,
				taskEventInput(task.ID, uint64(ordinal), TaskEventStateRunning),
				taskJournalTime().Add(time.Duration(ordinal)*time.Second),
			)
			if err != nil {
				errorsFound <- err
				return
			}
			sequences <- result.Sequence
		}()
	}
	workers.Wait()
	close(sequences)
	close(errorsFound)
	for err := range errorsFound {
		t.Fatalf("AppendTaskEvent(concurrent) error = %v", err)
	}
	got := make([]int, 0, count)
	for sequence := range sequences {
		got = append(got, int(sequence))
	}
	sort.Ints(got)
	for index, sequence := range got {
		if sequence != index+1 {
			t.Fatalf("sequences = %v, want 1 through %d", got, count)
		}
	}
}

func TestTaskRepositoryBacksOffAfterCASConflicts(t *testing.T) {
	// Rationale: hot Tasks must not spin against etcd. Exponential delays are
	// bounded, jittered, context-aware, and injectable for deterministic tests.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	store.conflictNextTransactions(3)
	waits := make([]time.Duration, 0, 3)
	policy := taskCASRetryPolicy{
		initialDelay: time.Millisecond,
		maximumDelay: 10 * time.Millisecond,
		jitter:       func(bound time.Duration) time.Duration { return bound },
		wait: func(waitContext context.Context, delay time.Duration) error {
			if waitContext != ctx {
				t.Fatal("retry wait lost the caller context")
			}
			waits = append(waits, delay)
			return nil
		},
	}
	repository, err := newTaskRepositoryWithRetryPolicy(store, policy)
	if err != nil {
		t.Fatalf("newTaskRepositoryWithRetryPolicy() error = %v", err)
	}
	result, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(task.ID, 1, TaskEventStateRunning),
		taskJournalTime().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AppendTaskEvent() error = %v", err)
	}
	if result.Sequence != 1 || result.Duplicate {
		t.Fatalf("AppendTaskEvent() = %#v", result)
	}
	wantWaits := []time.Duration{1500 * time.Microsecond, 3 * time.Millisecond, 6 * time.Millisecond}
	if len(waits) != len(wantWaits) {
		t.Fatalf("retry waits = %v, want %v", waits, wantWaits)
	}
	for index := range wantWaits {
		if waits[index] != wantWaits[index] {
			t.Fatalf("retry waits = %v, want %v", waits, wantWaits)
		}
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := waitForTaskCASRetry(canceled, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForTaskCASRetry(canceled) error = %v, want context.Canceled", err)
	}

	store = newMemoryTaskStore()
	seedTaskRepositoryTask(t, store, task)
	store.conflictNextTransactions(12)
	longWaits := 0
	policy.wait = func(context.Context, time.Duration) error {
		longWaits++
		return nil
	}
	repository, err = newTaskRepositoryWithRetryPolicy(store, policy)
	if err != nil {
		t.Fatalf("newTaskRepositoryWithRetryPolicy(long contention) error = %v", err)
	}
	result, err = repository.AppendTaskEvent(
		context.Background(),
		taskEventInput(task.ID, 1, TaskEventStateRunning),
		taskJournalTime().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AppendTaskEvent(long contention) error = %v", err)
	}
	if result.Sequence != 1 || result.Duplicate || longWaits != 12 {
		t.Fatalf("AppendTaskEvent(long contention) = %#v after %d waits", result, longWaits)
	}

	waitCalled := false
	canceledPolicy := policy
	canceledPolicy.jitter = func(time.Duration) time.Duration {
		t.Fatal("canceled retry evaluated jitter")
		return 0
	}
	canceledPolicy.wait = func(context.Context, time.Duration) error {
		waitCalled = true
		return nil
	}
	if err := canceledPolicy.waitAfterConflict(canceled, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("waitAfterConflict(canceled) error = %v, want context.Canceled", err)
	}
	if waitCalled {
		t.Fatal("canceled retry started a delay")
	}
}

func TestTaskRepositoryDoesNotInferSuccessAfterUnknownOutcome(t *testing.T) {
	// Rationale: a transport error after commit is ambiguous without the
	// separate idempotency marker. The first call must preserve that retryable
	// error; a later replay can use the durable event dedupe record.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	store.failAfterCommit(errs.New(errs.KindStorageUnavailable, "unknown write outcome"))
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	if _, err := repository.AppendTaskEvent(
		ctx,
		input,
		taskJournalTime().Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindStorageUnavailable, "")) {
		t.Fatalf("AppendTaskEvent(unknown) error = %v, want storage.unavailable", err)
	}

	restarted, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository(restart) error = %v", err)
	}
	replay, err := restarted.AppendTaskEvent(ctx, input, taskJournalTime().Add(2*time.Second))
	if err != nil {
		t.Fatalf("AppendTaskEvent(replay) error = %v", err)
	}
	if replay.Sequence != 1 || !replay.Duplicate {
		t.Fatalf("AppendTaskEvent(replay) = %#v, want duplicate sequence 1", replay)
	}
}

func TestTaskRepositoryRejectsSequenceCollisionAndEventCap(t *testing.T) {
	// Rationale: an occupied sequence without its matching dedupe record is
	// corruption, not a CAS contention loop; the accepted event cap is hard.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	collidingInput := taskEventInput(task.ID, 99, TaskEventStateRunning)
	colliding, err := prepareTaskEvent(task, collidingInput, nil, taskJournalTime().Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent(collision) error = %v", err)
	}
	encodedCollision, err := encodeTaskEventRecord(colliding.Event)
	if err != nil {
		t.Fatalf("encodeTaskEventRecord(collision) error = %v", err)
	}
	seedTaskRepositoryValue(t, store, taskEventKey(task.ID, 1), encodedCollision)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(task.ID, 1, TaskEventStateRunning),
		taskJournalTime().Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("AppendTaskEvent(collision) error = %v, want internal", err)
	}

	cappedStore := newMemoryTaskStore()
	capped := validTaskRecord(taskJournalTime())
	capped.EventCount = MaximumTaskEvents
	capped.NextEventSequence = MaximumTaskEvents + 1
	seedTaskRepositoryTask(t, cappedStore, capped)
	cappedRepository, err := newTaskRepository(cappedStore)
	if err != nil {
		t.Fatalf("newTaskRepository(capped) error = %v", err)
	}
	if _, err := cappedRepository.AppendTaskEvent(
		ctx,
		taskEventInput(capped.ID, MaximumTaskEvents+1, TaskEventStateRunning),
		taskJournalTime().Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindValidationFailed, "")) {
		t.Fatalf("AppendTaskEvent(cap) error = %v, want validation.failed", err)
	}
}

func TestTaskRepositoryTerminalReplayDoesNotPermitNewWrites(t *testing.T) {
	// Rationale: replay lookup precedes the terminal guard, while a new
	// identity must fail before the repository opens a transaction.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	if _, err := repository.AppendTaskEvent(ctx, input, taskJournalTime().Add(time.Second)); err != nil {
		t.Fatalf("AppendTaskEvent(first) error = %v", err)
	}
	current, err := repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	running, err := transitionTaskStatus(
		current.Record,
		TaskStatusPending,
		TaskStatusRunning,
		taskJournalTime().Add(2*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(running) error = %v", err)
	}
	terminal, err := transitionTaskStatus(
		running,
		TaskStatusRunning,
		TaskStatusCompleted,
		taskJournalTime().Add(3*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(terminal) error = %v", err)
	}
	encoded, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatalf("encodeTaskRecord(terminal) error = %v", err)
	}
	transition, err := store.Transact(ctx, []Condition{{
		Key: taskKey(task.ID), ModRevision: current.Revision,
	}}, []Mutation{{Type: MutationPut, Key: taskKey(task.ID), Value: encoded}})
	if err != nil || !transition.Succeeded {
		t.Fatalf("persist terminal Task = %#v, %v", transition, err)
	}
	replay, err := repository.AppendTaskEvent(ctx, input, taskJournalTime().Add(4*time.Second))
	if err != nil {
		t.Fatalf("AppendTaskEvent(replay) error = %v", err)
	}
	if !replay.Duplicate || replay.Sequence != 1 {
		t.Fatalf("AppendTaskEvent(replay) = %#v", replay)
	}
	revisionBefore := store.currentRevision()
	if _, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(task.ID, 2, TaskEventStateRunning),
		taskJournalTime().Add(5*time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("AppendTaskEvent(new terminal identity) error = %v, want internal", err)
	}
	if revisionAfter := store.currentRevision(); revisionAfter != revisionBefore {
		t.Fatalf("terminal event rejection changed revision from %d to %d", revisionBefore, revisionAfter)
	}
}

func TestTaskRepositoryListsAtFixedRevisions(t *testing.T) {
	// Rationale: task pagination and activity replay must describe one MVCC
	// view even while new Tasks and events are committed.
	ctx := context.Background()
	store := newMemoryTaskStore()
	first := validTaskRecord(taskJournalTime())
	second := validTaskRecord(taskJournalTime())
	second.ID = ids.NewAt(ids.KindTask, taskJournalTime().Add(time.Second), 31)
	second.OperationID = ids.NewAt(ids.KindOperation, taskJournalTime(), 32)
	seedTaskRepositoryTask(t, store, first)
	seedTaskRepositoryTask(t, store, second)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	page, err := repository.ListTasks(ctx, PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("ListTasks(first page) error = %v", err)
	}
	if len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("ListTasks(first page) = %#v", page)
	}
	third := validTaskRecord(taskJournalTime())
	third.ID = ids.NewAt(ids.KindTask, taskJournalTime().Add(2*time.Second), 33)
	third.OperationID = ids.NewAt(ids.KindOperation, taskJournalTime(), 34)
	seedTaskRepositoryTask(t, store, third)
	secondPage, err := repository.ListTasks(ctx, PageRequest{Limit: 1, Cursor: page.NextCursor})
	if err != nil {
		t.Fatalf("ListTasks(second page) error = %v", err)
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].Record.ID != second.ID ||
		secondPage.Revision != page.Revision {
		t.Fatalf("ListTasks(second page) = %#v, want fixed-revision second Task", secondPage)
	}

	one, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(first.ID, 1, TaskEventStateRunning),
		taskJournalTime().Add(time.Second),
	)
	if err != nil {
		t.Fatalf("AppendTaskEvent(one) error = %v", err)
	}
	snapshot, err := repository.ListTaskEvents(ctx, first.ID, 0)
	if err != nil {
		t.Fatalf("ListTaskEvents(current) error = %v", err)
	}
	if len(snapshot.Events) != 1 || snapshot.Events[0].Sequence != one.Sequence {
		t.Fatalf("ListTaskEvents(current) = %#v", snapshot)
	}
	if _, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(first.ID, 2, TaskEventStateRunning),
		taskJournalTime().Add(2*time.Second),
	); err != nil {
		t.Fatalf("AppendTaskEvent(two) error = %v", err)
	}
	replay, err := repository.ListTaskEvents(ctx, first.ID, snapshot.Revision)
	if err != nil {
		t.Fatalf("ListTaskEvents(fixed) error = %v", err)
	}
	if len(replay.Events) != 1 || replay.Revision != snapshot.Revision {
		t.Fatalf("ListTaskEvents(fixed) = %#v, want first snapshot", replay)
	}
}

func seedTaskRepositoryTask(t *testing.T, store *memoryTaskStore, task TaskRecord) {
	t.Helper()
	value, err := encodeTaskRecord(task)
	if err != nil {
		t.Fatalf("encodeTaskRecord(seed) error = %v", err)
	}
	seedTaskRepositoryValue(t, store, taskKey(task.ID), value)
}

func seedTaskRepositoryValue(t *testing.T, store *memoryTaskStore, key string, value []byte) {
	t.Helper()
	if _, err := store.Transact(context.Background(), []Condition{{Key: key}}, []Mutation{{
		Type: MutationPut, Key: key, Value: value,
	}}); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}
}

type memoryTaskVersion struct {
	revision int64
	value    []byte
	present  bool
}

type memoryTaskStore struct {
	mu                  sync.Mutex
	revision            int64
	history             map[string][]memoryTaskVersion
	failAfterCommitOnce error
	conflictsRemaining  int
}

func newMemoryTaskStore() *memoryTaskStore {
	return &memoryTaskStore{history: make(map[string][]memoryTaskVersion)}
}

func (store *memoryTaskStore) failAfterCommit(err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.failAfterCommitOnce = err
}

func (store *memoryTaskStore) conflictNextTransactions(count int) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.conflictsRemaining = count
}

func (store *memoryTaskStore) currentRevision() int64 {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.revision
}

func (store *memoryTaskStore) Get(ctx context.Context, key string) (*GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return &GetResult{Entry: store.valueAtLocked(key, store.revision), ReadRevision: store.revision}, nil
}

func (store *memoryTaskStore) GetMany(
	ctx context.Context,
	request GetManyRequest,
) (*GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAtLocked(key, revision)
	}
	return &GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision,
	}, nil
}

func (store *memoryTaskStore) Range(
	ctx context.Context,
	request RangeRequest,
) (*RangeResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	keys := make([]string, 0)
	for key := range store.history {
		if !strings.HasPrefix(key, request.Prefix) || key <= request.StartExclusive ||
			store.valueAtLocked(key, revision) == nil {
			continue
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	more := int64(len(keys)) > request.Limit
	if more {
		keys = keys[:request.Limit]
	}
	values := make([]KeyValue, 0, len(keys))
	for _, key := range keys {
		values = append(values, *store.valueAtLocked(key, revision))
	}
	return &RangeResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision, More: more,
	}, nil
}

func (store *memoryTaskStore) Transact(
	ctx context.Context,
	conditions []Condition,
	mutations []Mutation,
) (TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.conflictsRemaining > 0 {
		store.conflictsRemaining--
		return TransactionResult{Revision: store.revision}, nil
	}
	for _, condition := range conditions {
		value := store.valueAtLocked(condition.Key, store.revision)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failureReads := make([]*KeyValue, len(conditions))
			for index, failedCondition := range conditions {
				failureReads[index] = store.valueAtLocked(failedCondition.Key, store.revision)
			}
			return TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		version := memoryTaskVersion{revision: store.revision}
		switch mutation.Type {
		case MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
		case MutationDelete:
		default:
			return TransactionResult{}, errs.New(errs.KindInternal, "fake task store received invalid mutation")
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
	}
	if store.failAfterCommitOnce != nil {
		err := store.failAfterCommitOnce
		store.failAfterCommitOnce = nil
		return TransactionResult{}, err
	}
	return TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryTaskStore) valueAtLocked(key string, revision int64) *KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.revision > revision {
			continue
		}
		if !version.present {
			return nil
		}
		return &KeyValue{
			Key: key, Value: append([]byte(nil), version.value...), ModRevision: version.revision,
		}
	}
	return nil
}
