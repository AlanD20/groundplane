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
	testdeletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskRepositoryKeysMatchTheApprovedLayout(t *testing.T) {
	// Rationale: a path spelling or padding drift creates a second durable
	// schema. The accepted v1 keys have exactly one canonical representation.
	now := taskJournalTime()
	taskID := ids.NewAt(ids.KindTask, now, 21)
	operationID := ids.NewAt(ids.KindOperation, now, 22)
	stepID := ids.NewAt(ids.KindStep, now, 23)
	identity := testtaskjournal.TaskEventIdentity{
		AssignmentID: taskEventTestAssignmentID,
		AgentID:      taskEventTestAgentID, AgentGeneration: 1,
		TaskID: taskID, StepID: stepID, Attempt: 3, Ordinal: 17,
	}
	environmentID := ids.NewAt(ids.KindEnvironment, now, 24)
	tenantID := ids.NewAt(ids.KindTenant, now, 25)
	tenantWorkspacePath := "/v1/indexes/tasks/by-workspace/tenant/" + tenantID + "/" + taskID
	environmentTaskPath := "/v1/indexes/tasks/by-environment/" + environmentID + "/" + taskID

	wants := map[string]string{
		testtaskjournal.TaskStorageKey(taskID):                     "/v1/tasks/" + taskID,
		testtaskjournal.TaskOperationIndexKey(operationID, taskID): "/v1/indexes/tasks/operation/" + operationID + "/" + taskID,
		testtaskjournal.TaskActiveOperationKey(operationID):        "/v1/indexes/tasks/active-operation/" + operationID,
		testtaskjournal.TaskEventKey(taskID, 42):                   "/v1/runtime/task-events/" + taskID + "/00000000000000000042",
		testtaskjournal.TaskEventDedupKey(identity): "/v1/runtime/task-event-dedup/" + taskID + "/" +
			taskEventTestAssignmentID + "/" + stepID + "/3/17",
		testdeletions.TombstoneKey("environment", environmentID):       "/v1/runtime/deletions/environment/" + environmentID,
		testtaskjournal.TaskWorkspacePlatformIndexKey(taskID):          "/v1/indexes/tasks/by-workspace/platform/" + taskID,
		testtaskjournal.TaskWorkspaceTenantIndexKey(tenantID, taskID):  tenantWorkspacePath,
		testtaskjournal.TaskEnvironmentIndexKey(environmentID, taskID): environmentTaskPath,
	}
	for got, want := range wants {
		if got != want {
			t.Fatalf("key = %q, want %q", got, want)
		}
	}
	if sequence, err := testtaskjournal.TaskEventSequenceFromKey(taskID, testtaskjournal.TaskEventKey(taskID, 42)); err != nil ||
		sequence != 42 {
		t.Fatalf("taskEventSequenceFromKey() = %d, %v, want 42", sequence, err)
	}
}

func TestTaskRepositoryAppendIsRestartSafeAndDeduplicated(t *testing.T) {
	// Rationale: Agent reconnects replay the same stable event identity. A new
	// Controller process must return the original sequence without a new write.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	input := taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning)
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
	seedTaskRepositoryRunningTask(t, store, task)
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
				taskEventInput(task.ID, uint64(ordinal), testtaskjournal.TaskEventStateRunning),
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
	seedTaskRepositoryRunningTask(t, store, task)
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
		taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning),
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
	seedTaskRepositoryRunningTask(t, store, task)
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
		taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning),
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
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	store.failAfterCommit(errs.New(errs.KindStorageUnavailable, "unknown write outcome"))
	input := taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning)
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
	seedTaskRepositoryRunningTask(t, store, task)
	collidingInput := taskEventInput(task.ID, 99, testtaskjournal.TaskEventStateRunning)
	colliding, err := prepareTaskEvent(task, collidingInput, nil, taskJournalTime().Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent(collision) error = %v", err)
	}
	encodedCollision, err := testtaskjournal.EncodeTaskEventRecord(colliding.Event)
	if err != nil {
		t.Fatalf("encodeTaskEventRecord(collision) error = %v", err)
	}
	seedTaskRepositoryValue(t, store, testtaskjournal.TaskEventKey(task.ID, 1), encodedCollision)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	if _, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning),
		taskJournalTime().Add(2*time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("AppendTaskEvent(collision) error = %v, want internal", err)
	}

	cappedStore := newMemoryTaskStore()
	capped := validTaskRecord(taskJournalTime())
	capped.EventCount = testtaskjournal.MaximumTaskEvents
	capped.NextEventSequence = testtaskjournal.MaximumTaskEvents + 1
	seedTaskRepositoryRunningTask(t, cappedStore, capped)
	cappedRepository, err := newTaskRepository(cappedStore)
	if err != nil {
		t.Fatalf("newTaskRepository(capped) error = %v", err)
	}
	if _, err := cappedRepository.AppendTaskEvent(
		ctx,
		taskEventInput(capped.ID, testtaskjournal.MaximumTaskEvents+1, testtaskjournal.TaskEventStateRunning),
		taskJournalTime().Add(time.Second),
	); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("AppendTaskEvent(missing oldest event) error = %v, want internal", err)
	}
}

func TestTaskRepositoryTerminalReplayDoesNotPermitNewWrites(t *testing.T) {
	// Rationale: replay lookup precedes the terminal guard, while a new
	// identity must fail before the repository opens a transaction.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	input := taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning)
	if _, err := repository.AppendTaskEvent(ctx, input, taskJournalTime().Add(time.Second)); err != nil {
		t.Fatalf("AppendTaskEvent(first) error = %v", err)
	}
	current, err := repository.GetTask(ctx, task.ID)
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	running := current.Record
	terminal, err := TransitionTaskStatus(
		running,
		testtaskjournal.TaskStatusRunning,
		testtaskjournal.TaskStatusCompleted,
		taskJournalTime().Add(3*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(terminal) error = %v", err)
	}
	encoded, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatalf("encodeTaskRecord(terminal) error = %v", err)
	}
	transition, err := store.Transact(ctx, []testkeyvalue.Condition{{
		Key: testtaskjournal.TaskStorageKey(task.ID), ModRevision: current.Revision,
	}}, []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: encoded},
		{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskAssignmentKey(taskEventTestAgentID, task.ID)},
		{Type: testkeyvalue.MutationDelete, Key: testtaskjournal.TaskAssignmentIndexKey(task.ID)},
	})
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
	changedPayload := input
	changedPayload.Payload = json.RawMessage(`{"message":"changed"}`)
	changedAssignment := input
	changedAssignment.Identity.AssignmentID = "asgn_01ARZ3NDEKTSV4RRFFQ69G5FAW"
	for _, changed := range []testtaskjournal.TaskEventInput{changedPayload, changedAssignment} {
		if _, replayErr := repository.AppendTaskEvent(ctx, changed, taskJournalTime().Add(5*time.Second)); !errors.Is(
			replayErr,
			errs.New(errs.KindInternal, ""),
		) {
			t.Fatalf("AppendTaskEvent(changed terminal replay) error = %v, want internal", replayErr)
		}
	}
	if _, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(task.ID, 2, testtaskjournal.TaskEventStateRunning),
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
	seedTaskRepositoryRunningTask(t, store, first)
	seedTaskRepositoryRunningTask(t, store, second)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	page, err := repository.ListTasks(ctx, testkeyvalue.PageRequest{Limit: 1})
	if err != nil {
		t.Fatalf("ListTasks(first page) error = %v", err)
	}
	if len(page.Items) != 1 || page.NextCursor == "" {
		t.Fatalf("ListTasks(first page) = %#v", page)
	}
	third := validTaskRecord(taskJournalTime())
	third.ID = ids.NewAt(ids.KindTask, taskJournalTime().Add(2*time.Second), 33)
	third.OperationID = ids.NewAt(ids.KindOperation, taskJournalTime(), 34)
	seedTaskRepositoryRunningTask(t, store, third)
	secondPage, err := repository.ListTasks(ctx, testkeyvalue.PageRequest{Limit: 1, Cursor: page.NextCursor})
	if err != nil {
		t.Fatalf("ListTasks(second page) error = %v", err)
	}
	if len(secondPage.Items) != 1 || secondPage.Items[0].Record.ID != second.ID ||
		secondPage.Revision != page.Revision {
		t.Fatalf("ListTasks(second page) = %#v, want fixed-revision second Task", secondPage)
	}

	one, err := repository.AppendTaskEvent(
		ctx,
		taskEventInput(first.ID, 1, testtaskjournal.TaskEventStateRunning),
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
		taskEventInput(first.ID, 2, testtaskjournal.TaskEventStateRunning),
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
	value, err := EncodeTaskRecord(task)
	if err != nil {
		t.Fatalf("encodeTaskRecord(seed) error = %v", err)
	}
	keys, err := taskOwnerIndexKeys(task.Owner, task.ID)
	if err != nil {
		t.Fatalf("taskOwnerIndexKeys(seed) error = %v", err)
	}
	conditions := []testkeyvalue.Condition{{Key: testtaskjournal.TaskStorageKey(task.ID)}}
	mutations := []testkeyvalue.Mutation{
		{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(task.ID), Value: value},
	}
	for _, key := range keys {
		conditions = append(conditions, testkeyvalue.Condition{Key: key})
		mutations = append(
			mutations,
			testkeyvalue.Mutation{Type: testkeyvalue.MutationPut, Key: key, Value: []byte(task.ID)},
		)
	}
	result, err := store.Transact(context.Background(), conditions, mutations)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed Task with owner indexes = %#v, %v", result, err)
	}
}

func seedTaskRepositoryValue(t *testing.T, store *memoryTaskStore, key string, value []byte) {
	t.Helper()
	if _, err := store.Transact(context.Background(), []testkeyvalue.Condition{{Key: key}}, []testkeyvalue.Mutation{{
		Type: testkeyvalue.MutationPut, Key: key, Value: value,
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
	nextWatcherID       int
	watchers            map[int]*memoryTaskWatcher
	watchStarts         []memoryTaskWatchStart
}

type memoryTaskWatcher struct {
	prefix        string
	startRevision int64
	events        chan testkeyvalue.Event
	errors        chan error
}

type memoryTaskWatchStart struct {
	prefix   string
	revision int64
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

func (store *memoryTaskStore) Get(ctx context.Context, key string) (*testkeyvalue.GetResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return &testkeyvalue.GetResult{Entry: store.valueAtLocked(key, store.revision), ReadRevision: store.revision}, nil
}

func (store *memoryTaskStore) GetMany(
	ctx context.Context,
	request testkeyvalue.GetManyRequest,
) (*testkeyvalue.GetManyResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	revision := request.Revision
	if revision == 0 {
		revision = store.revision
	}
	values := make([]*testkeyvalue.KeyValue, len(request.Keys))
	for index, key := range request.Keys {
		values[index] = store.valueAtLocked(key, revision)
	}
	return &testkeyvalue.GetManyResult{
		Values: values, ReadRevision: revision, ResponseRevision: store.revision,
	}, nil
}

func (store *memoryTaskStore) Watch(
	ctx context.Context,
	prefix string,
	startRevision int64,
) (*testkeyvalue.WatchStream, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	store.mu.Lock()
	if startRevision == 0 {
		startRevision = store.revision + 1
	}
	events := make(chan testkeyvalue.Event, 4096)
	errorsFound := make(chan error, 1)
	history := make([]testkeyvalue.Event, 0)
	for key, versions := range store.history {
		if !strings.HasPrefix(key, prefix) {
			continue
		}
		for _, version := range versions {
			if version.revision < startRevision {
				continue
			}
			event := testkeyvalue.Event{Key: key, ModRevision: version.revision, Type: testkeyvalue.EventDelete}
			if version.present {
				event.Type = testkeyvalue.EventPut
				event.Value = append([]byte(nil), version.value...)
			}
			history = append(history, event)
		}
	}
	sort.Slice(history, func(left int, right int) bool {
		if history[left].ModRevision != history[right].ModRevision {
			return history[left].ModRevision < history[right].ModRevision
		}
		return history[left].Key < history[right].Key
	})
	for _, event := range history {
		events <- event
	}
	store.nextWatcherID++
	id := store.nextWatcherID
	if store.watchers == nil {
		store.watchers = make(map[int]*memoryTaskWatcher)
	}
	store.watchers[id] = &memoryTaskWatcher{
		prefix: prefix, startRevision: startRevision, events: events, errors: errorsFound,
	}
	store.watchStarts = append(store.watchStarts, memoryTaskWatchStart{prefix: prefix, revision: startRevision})
	store.mu.Unlock()

	go func() {
		<-ctx.Done()
		store.mu.Lock()
		watcher, exists := store.watchers[id]
		if exists {
			delete(store.watchers, id)
			close(watcher.errors)
			close(watcher.events)
		}
		store.mu.Unlock()
	}()
	return &testkeyvalue.WatchStream{Events: events, Errors: errorsFound}, nil
}

func (store *memoryTaskStore) Transact(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionResult, error) {
	if err := ctx.Err(); err != nil {
		return testkeyvalue.TransactionResult{}, err
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.conflictsRemaining > 0 {
		store.conflictsRemaining--
		return testkeyvalue.TransactionResult{Revision: store.revision}, nil
	}
	for _, condition := range conditions {
		value := store.conditionValueLocked(condition, store.revision)
		actual := int64(0)
		if value != nil {
			actual = value.ModRevision
		}
		if actual != condition.ModRevision {
			failureReads := make([]*testkeyvalue.KeyValue, len(conditions))
			for index, failedCondition := range conditions {
				failureReads[index] = store.conditionValueLocked(failedCondition, store.revision)
			}
			return testkeyvalue.TransactionResult{Revision: store.revision, FailureReads: failureReads}, nil
		}
	}
	store.revision++
	for _, mutation := range mutations {
		version := memoryTaskVersion{revision: store.revision}
		event := testkeyvalue.Event{Key: mutation.Key, ModRevision: store.revision, Type: testkeyvalue.EventDelete}
		switch mutation.Type {
		case testkeyvalue.MutationPut:
			version.present = true
			version.value = append([]byte(nil), mutation.Value...)
			event.Type = testkeyvalue.EventPut
			event.Value = append([]byte(nil), mutation.Value...)
		case testkeyvalue.MutationDelete:
		default:
			return testkeyvalue.TransactionResult{}, errs.New(
				errs.KindInternal,
				"fake task store received invalid mutation",
			)
		}
		store.history[mutation.Key] = append(store.history[mutation.Key], version)
		for _, watcher := range store.watchers {
			if store.revision >= watcher.startRevision && strings.HasPrefix(mutation.Key, watcher.prefix) {
				watcher.events <- event
			}
		}
	}
	if store.failAfterCommitOnce != nil {
		err := store.failAfterCommitOnce
		store.failAfterCommitOnce = nil
		return testkeyvalue.TransactionResult{}, err
	}
	return testkeyvalue.TransactionResult{Succeeded: true, Revision: store.revision}, nil
}

func (store *memoryTaskStore) MeasureTransaction(
	ctx context.Context,
	conditions []testkeyvalue.Condition,
	mutations []testkeyvalue.Mutation,
) (testkeyvalue.TransactionBudget, error) {
	return MeasureTransactionBudget(ctx, "/groundplane/", conditions, mutations)
}

func (store *memoryTaskStore) conditionValueLocked(
	condition testkeyvalue.Condition,
	revision int64,
) *testkeyvalue.KeyValue {
	if !condition.Prefix {
		return store.valueAtLocked(condition.Key, revision)
	}
	var first *testkeyvalue.KeyValue
	for key := range store.history {
		if !strings.HasPrefix(key, condition.Key) {
			continue
		}
		value := store.valueAtLocked(key, revision)
		if value != nil && (first == nil || value.Key < first.Key) {
			first = value
		}
	}
	return first
}

func (store *memoryTaskStore) failWatch(prefix string, err error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	for id, watcher := range store.watchers {
		if watcher.prefix != prefix {
			continue
		}
		delete(store.watchers, id)
		watcher.errors <- err
		close(watcher.errors)
		close(watcher.events)
	}
}

func (store *memoryTaskStore) watcherCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.watchers)
}

func (store *memoryTaskStore) watchStartCount() int {
	store.mu.Lock()
	defer store.mu.Unlock()
	return len(store.watchStarts)
}

func (store *memoryTaskStore) valueAtLocked(key string, revision int64) *testkeyvalue.KeyValue {
	versions := store.history[key]
	for index := len(versions) - 1; index >= 0; index-- {
		version := versions[index]
		if version.revision > revision {
			continue
		}
		if !version.present {
			return nil
		}
		keyVersion := int64(0)
		for previous := index; previous >= 0 && versions[previous].present; previous-- {
			keyVersion++
		}
		return &testkeyvalue.KeyValue{
			Key: key, Value: append([]byte(nil), version.value...),
			Version: keyVersion, ModRevision: version.revision,
		}
	}
	return nil
}
