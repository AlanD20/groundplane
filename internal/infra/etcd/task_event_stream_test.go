package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	testkeyvalue "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	testtaskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskEventStreamReplaysSuffixAndFinalDrains(t *testing.T) {
	// QA: TASK-06 (L1 in-memory repository; no real etcd watch scheduling or HTTP SSE framing).
	// Rationale: resume must skip seen sequence 1, watch both exact selectors from the snapshot successor,
	// emit each later durable sequence once, and close only after authoritative terminal Task state.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	appendTaskStreamEvent(t, repository, task.ID, 1)
	appendTaskStreamEvent(t, repository, task.ID, 2)
	snapshotRevision := store.currentRevision()
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 1)
	if err != nil {
		t.Fatalf("OpenTaskEventStream() error = %v", err)
	}
	assertTaskStreamWatchStarts(t, store,
		memoryTaskWatchStart{prefix: testtaskjournal.TaskStorageKey(task.ID), revision: snapshotRevision + 1},
		memoryTaskWatchStart{prefix: testtaskjournal.TaskEventScopePrefix(task.ID), revision: snapshotRevision + 1},
	)
	emitted := make(chan testtaskjournal.TaskEventRecord, 4)
	done := runTaskEventStream(ctx, stream, emitted)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 2 {
		t.Fatalf("initial suffix sequence = %d, want 2", event.Sequence)
	}
	appendTaskStreamEvent(t, repository, task.ID, 3)
	terminalizeTaskStream(t, store, repository, task.ID)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 3 {
		t.Fatalf("final sequence = %d, want 3", event.Sequence)
	}
	if err := receiveTaskStreamDone(t, done); err != nil {
		t.Fatalf("TaskEventStream.Run() error = %v", err)
	}
	assertNoTaskStreamEvent(t, emitted)
	if count := store.watcherCount(); count != 0 {
		t.Fatalf("watcher count after terminal close = %d, want 0", count)
	}
}

func TestTaskEventStreamValidatesResumeBeforeOpeningWatches(t *testing.T) {
	// QA: TASK-06 (L1 in-memory repository; HTTP 400 mapping and pre-header response timing are not proved).
	// Rationale: an ahead resume sequence must fail as a malformed request before any watch opens, while an
	// initially terminal Task must replay its durable event and close without creating live subscriptions.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	appendTaskStreamEvent(t, repository, task.ID, 1)
	terminalizeTaskStream(t, store, repository, task.ID)
	if _, err := repository.OpenTaskEventStream(ctx, task.ID, 2); !errors.Is(
		err,
		errs.New(errs.KindMalformedRequest, ""),
	) {
		t.Fatalf("OpenTaskEventStream(ahead) error = %v, want malformed request", err)
	}
	if starts := store.watchStartCount(); starts != 0 {
		t.Fatalf("ahead resume watch starts = %d, want 0", starts)
	}
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
	if err != nil {
		t.Fatalf("OpenTaskEventStream(terminal) error = %v", err)
	}
	var emitted []testtaskjournal.TaskEventRecord
	if err := stream.Run(ctx, func(event testtaskjournal.TaskEventRecord) error {
		emitted = append(emitted, event)
		return nil
	}); err != nil {
		t.Fatalf("TaskEventStream.Run(terminal) error = %v", err)
	}
	if len(emitted) != 1 || emitted[0].Sequence != 1 {
		t.Fatalf("terminal replay = %#v, want sequence 1", emitted)
	}
	if emitted[0].Identity.Ordinal != 1 || emitted[0].State != testtaskjournal.TaskEventStateRunning {
		t.Fatalf("terminal replay event = %#v, want original ordinal 1 running event", emitted[0])
	}
	if starts := store.watchStartCount(); starts != 0 {
		t.Fatalf("terminal stream watch starts = %d, want 0", starts)
	}
}

func TestTaskEventStreamRecoversCompactionBySequence(t *testing.T) {
	// QA: TASK-06 (L1 fake cursor-expiry injection; real etcd compaction and revision retention are not proved).
	// Rationale: compaction recovery must resnapshot from the last public sequence, filter any already observed
	// event, reopen both exact watches at the new snapshot successor, and continue without loss or duplication.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	appendTaskStreamEvent(t, repository, task.ID, 1)
	initialRevision := store.currentRevision()
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
	if err != nil {
		t.Fatalf("OpenTaskEventStream() error = %v", err)
	}
	emitted := make(chan testtaskjournal.TaskEventRecord, 4)
	done := runTaskEventStream(ctx, stream, emitted)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 1 {
		t.Fatalf("initial sequence = %d, want 1", event.Sequence)
	}
	appendTaskStreamEvent(t, repository, task.ID, 2)
	recoveryRevision := store.currentRevision()
	store.failWatch(testtaskjournal.TaskEventScopePrefix(task.ID), errs.New(errs.KindCursorExpired, "compacted"))
	waitForTaskWatchStarts(t, store, 4, done)
	assertTaskStreamWatchStarts(t, store,
		memoryTaskWatchStart{prefix: testtaskjournal.TaskStorageKey(task.ID), revision: initialRevision + 1},
		memoryTaskWatchStart{prefix: testtaskjournal.TaskEventScopePrefix(task.ID), revision: initialRevision + 1},
		memoryTaskWatchStart{prefix: testtaskjournal.TaskStorageKey(task.ID), revision: recoveryRevision + 1},
		memoryTaskWatchStart{prefix: testtaskjournal.TaskEventScopePrefix(task.ID), revision: recoveryRevision + 1},
	)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 2 {
		t.Fatalf("post-compaction sequence = %d, want 2", event.Sequence)
	}
	appendTaskStreamEvent(t, repository, task.ID, 3)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 3 {
		t.Fatalf("post-restart sequence = %d, want 3", event.Sequence)
	}
	terminalizeTaskStream(t, store, repository, task.ID)
	if err := receiveTaskStreamDone(t, done); err != nil {
		t.Fatalf("TaskEventStream.Run() error = %v", err)
	}
	assertNoTaskStreamEvent(t, emitted)
	if count := store.watcherCount(); count != 0 {
		t.Fatalf("watcher count after recovered stream close = %d, want 0", count)
	}
}

// QA: TASK-06; controlled L1 watch completion, not real etcd transport or HTTP SSE.
// Rationale: an error queued before following must survive either channel being
// selected first; recovery must replay the missed event and resume both watches.
func TestTaskEventStreamRecoversClosedWatchBeforeFollow(t *testing.T) {
	for _, primary := range []bool{false, true} {
		name := "events"
		if primary {
			name = "primary"
		}
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			store := newMemoryTaskStore()
			task := validTaskRecord(taskJournalTime())
			seedTaskRepositoryRunningTask(t, store, task)
			repository, err := newTaskRepository(store)
			if err != nil {
				t.Fatal(err)
			}
			appendTaskStreamEvent(t, repository, task.ID, 1)
			stream, err := repository.OpenTaskEventStream(ctx, task.ID, 1)
			if err != nil {
				t.Fatal(err)
			}
			prefix := testtaskjournal.TaskEventScopePrefix(task.ID)
			if primary {
				prefix = testtaskjournal.TaskStorageKey(task.ID)
			}
			store.failWatch(prefix, errs.New(errs.KindCursorExpired, "compacted"))
			appendTaskStreamEvent(t, repository, task.ID, 2)
			emitted := make(chan testtaskjournal.TaskEventRecord, 4)
			done := runTaskEventStream(ctx, stream, emitted)
			waitForTaskWatchStarts(t, store, 4, done)
			if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 2 {
				t.Fatalf("recovered sequence = %d, want 2", event.Sequence)
			}
			appendTaskStreamEvent(t, repository, task.ID, 3)
			terminalizeTaskStream(t, store, repository, task.ID)
			if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 3 {
				t.Fatalf("final sequence = %d, want 3", event.Sequence)
			}
			if err := receiveTaskStreamDone(t, done); err != nil {
				t.Fatalf("recovered stream = %v", err)
			}
			assertNoTaskStreamEvent(t, emitted)
			if count := store.watcherCount(); count != 0 {
				t.Fatalf("watchers after recovery and terminal drain = %d", count)
			}
		})
	}
}

// QA: TASK-06; L1 watch completion classification and cleanup, not HTTP error mapping.
// Rationale: preserving compaction recovery must not turn storage failures into
// retries, hide missing terminal errors, or classify caller cancellation as corruption.
func TestTaskEventStreamClosedWatchPreservesFailureAndCancellation(t *testing.T) {
	for _, primary := range []bool{false, true} {
		for _, test := range []struct {
			name     string
			watchErr error
			wantErr  error
			cancel   bool
		}{
			{"storage", errs.New(errs.KindStorageUnavailable, "offline"), errs.New(errs.KindStorageUnavailable, ""), false},
			{"missing-error", nil, errs.New(errs.KindInternal, ""), false},
			{"canceled", nil, context.Canceled, true},
		} {
			name := "events/" + test.name
			if primary {
				name = "primary/" + test.name
			}
			t.Run(name, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				store := newMemoryTaskStore()
				task := validTaskRecord(taskJournalTime())
				seedTaskRepositoryRunningTask(t, store, task)
				repository, err := newTaskRepository(store)
				if err != nil {
					t.Fatal(err)
				}
				appendTaskStreamEvent(t, repository, task.ID, 1)
				stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
				if err != nil {
					t.Fatal(err)
				}
				prefix := testtaskjournal.TaskEventScopePrefix(task.ID)
				if primary {
					prefix = testtaskjournal.TaskStorageKey(task.ID)
				}
				finishTaskStreamWatch(t, store, prefix, test.watchErr)
				var emitted []uint64
				err = stream.Run(ctx, func(event testtaskjournal.TaskEventRecord) error {
					emitted = append(emitted, event.Sequence)
					if test.cancel {
						cancel()
					}
					return nil
				})
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("closed watch = %v, want %v", err, test.wantErr)
				}
				if len(emitted) != 1 || emitted[0] != 1 {
					t.Fatalf("emitted sequences = %v, want [1]", emitted)
				}
				if store.watchStartCount() != 2 || store.watcherCount() != 0 {
					t.Fatalf("watch starts/remaining = %d/%d, want 2/0", store.watchStartCount(), store.watcherCount())
				}
			})
		}
	}
}

func finishTaskStreamWatch(t *testing.T, store *memoryTaskStore, prefix string, watchErr error) {
	t.Helper()
	store.mu.Lock()
	defer store.mu.Unlock()
	for id, watcher := range store.watchers {
		if watcher.prefix != prefix {
			continue
		}
		delete(store.watchers, id)
		if watchErr != nil {
			watcher.errors <- watchErr
		}
		close(watcher.errors)
		close(watcher.events)
		return
	}
	t.Fatal("watch to finish was not registered")
}

func TestTaskEventStreamDisconnectsOnSequenceGap(t *testing.T) {
	// QA: TASK-06 (L1 injected journal corruption; HTTP disconnect logging and real etcd delivery are not proved).
	// Rationale: a watched sequence gap must emit neither the gapped record nor a synthetic replacement, return
	// an internal corruption error, and release both subscriptions instead of skipping or resetting the resume point.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
	if err != nil {
		t.Fatalf("OpenTaskEventStream() error = %v", err)
	}
	emitted := make(chan testtaskjournal.TaskEventRecord, 1)
	done := runTaskEventStream(ctx, stream, emitted)
	input := taskEventInput(task.ID, 1, testtaskjournal.TaskEventStateRunning)
	prepared, err := prepareTaskEvent(task, input, nil, taskJournalTime().Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent() error = %v", err)
	}
	prepared.Event.Sequence = 2
	encoded, err := testtaskjournal.EncodeTaskEventRecord(prepared.Event)
	if err != nil {
		t.Fatalf("encodeTaskEventRecord() error = %v", err)
	}
	result, err := store.Transact(
		ctx,
		[]testkeyvalue.Condition{{Key: testtaskjournal.TaskEventKey(task.ID, 2)}},
		[]testkeyvalue.Mutation{{
			Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskEventKey(task.ID, 2), Value: encoded,
		}},
	)
	if err != nil || !result.Succeeded {
		t.Fatalf("seed gapped event = %#v, %v", result, err)
	}
	if err := receiveTaskStreamDone(t, done); !errors.Is(err, errs.New(errs.KindInternal, "")) {
		t.Fatalf("TaskEventStream.Run(gap) error = %v, want internal", err)
	}
	select {
	case event := <-emitted:
		t.Fatalf("gapped stream emitted %#v", event)
	default:
	}
	if count := store.watcherCount(); count != 0 {
		t.Fatalf("watcher count after sequence-gap disconnect = %d, want 0", count)
	}
}

func TestTaskEventStreamCancellationJoinsWatches(t *testing.T) {
	// QA: TASK-06 (L1 in-memory watches; real etcd transport and end-to-end HTTP disconnect are not proved).
	// Rationale: cancellation while delivery is blocked on a slow consumer must return context cancellation and
	// join both Task and event-watch goroutines before Run returns, preventing an abandoned subscription leak.
	ctx, cancel := context.WithCancel(context.Background())
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
	if err != nil {
		t.Fatalf("OpenTaskEventStream() error = %v", err)
	}
	if count := store.watcherCount(); count != 2 {
		t.Fatalf("watcher count before cancellation = %d, want 2", count)
	}
	emitStarted := make(chan testtaskjournal.TaskEventRecord, 1)
	done := make(chan error, 1)
	go func() {
		done <- stream.Run(ctx, func(event testtaskjournal.TaskEventRecord) error {
			emitStarted <- event
			<-ctx.Done()
			return ctx.Err()
		})
	}()
	appendTaskStreamEvent(t, repository, task.ID, 1)
	if event := receiveTaskStreamEvent(t, emitStarted); event.Sequence != 1 {
		t.Fatalf("blocked delivery sequence = %d, want 1", event.Sequence)
	}
	cancel()
	if err := receiveTaskStreamDone(t, done); !errors.Is(err, context.Canceled) {
		t.Fatalf("TaskEventStream.Run(canceled) error = %v, want context canceled", err)
	}
	if count := store.watcherCount(); count != 0 {
		t.Fatalf("watcher count after cancellation = %d, want 0", count)
	}
}

func appendTaskStreamEvent(t *testing.T, repository *TaskRepository, taskID string, ordinal uint64) {
	t.Helper()
	if _, err := repository.AppendTaskEvent(
		context.Background(),
		taskEventInput(taskID, ordinal, testtaskjournal.TaskEventStateRunning),
		taskJournalTime().Add(time.Duration(ordinal)*time.Second),
	); err != nil {
		t.Fatalf("AppendTaskEvent(%d) error = %v", ordinal, err)
	}
}

func terminalizeTaskStream(
	t *testing.T,
	store *memoryTaskStore,
	repository *TaskRepository,
	taskID string,
) {
	t.Helper()
	current, err := repository.GetTask(context.Background(), taskID)
	if err != nil {
		t.Fatalf("GetTask(terminalize) error = %v", err)
	}
	running := current.Record
	terminal, err := TransitionTaskStatus(
		running,
		testtaskjournal.TaskStatusRunning,
		testtaskjournal.TaskStatusCompleted,
		taskJournalTime().Add(21*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(completed) error = %v", err)
	}
	encoded, err := EncodeTaskRecord(terminal)
	if err != nil {
		t.Fatalf("encodeTaskRecord(terminal) error = %v", err)
	}
	result, err := store.Transact(context.Background(), []testkeyvalue.Condition{{
		Key: testtaskjournal.TaskStorageKey(taskID), ModRevision: current.Revision,
	}}, []testkeyvalue.Mutation{{Type: testkeyvalue.MutationPut, Key: testtaskjournal.TaskStorageKey(taskID), Value: encoded}})
	if err != nil || !result.Succeeded {
		t.Fatalf("persist terminal Task = %#v, %v", result, err)
	}
}

func runTaskEventStream(
	ctx context.Context,
	stream *TaskEventStream,
	emitted chan<- testtaskjournal.TaskEventRecord,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- stream.Run(ctx, func(event testtaskjournal.TaskEventRecord) error {
			select {
			case emitted <- event:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}()
	return done
}

func receiveTaskStreamEvent(
	t *testing.T,
	emitted <-chan testtaskjournal.TaskEventRecord,
) testtaskjournal.TaskEventRecord {
	t.Helper()
	select {
	case event := <-emitted:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Task event")
		return testtaskjournal.TaskEventRecord{}
	}
}

func receiveTaskStreamDone(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Task event stream to close")
		return nil
	}
}

func assertNoTaskStreamEvent(t *testing.T, emitted <-chan testtaskjournal.TaskEventRecord) {
	t.Helper()
	select {
	case event := <-emitted:
		t.Fatalf("unexpected additional Task event = %#v", event)
	default:
	}
}

func assertTaskStreamWatchStarts(
	t *testing.T,
	store *memoryTaskStore,
	want ...memoryTaskWatchStart,
) {
	t.Helper()
	store.mu.Lock()
	got := append([]memoryTaskWatchStart(nil), store.watchStarts...)
	store.mu.Unlock()
	if len(got) != len(want) {
		t.Fatalf("watch starts = %#v, want %#v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("watch start %d = %#v, want %#v", index, got[index], want[index])
		}
	}
}

func waitForTaskWatchStarts(t *testing.T, store *memoryTaskStore, count int, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.watchStartCount() >= count {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("stream exited before watches reopened: %v", err)
		case <-time.After(time.Millisecond):
		}
	}
	t.Fatalf("watch start count = %d, want at least %d", store.watchStartCount(), count)
}
