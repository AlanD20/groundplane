package etcd

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

func TestTaskEventStreamReplaysSuffixAndFinalDrains(t *testing.T) {
	// Rationale: reconnect must replay only unseen durable sequences, follow
	// new commits without a read-to-watch gap, and close only after Task state.
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
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 1)
	if err != nil {
		t.Fatalf("OpenTaskEventStream() error = %v", err)
	}
	emitted := make(chan TaskEventRecord, 4)
	done := runTaskEventStream(ctx, stream, emitted)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 2 {
		t.Fatalf("initial suffix sequence = %d, want 2", event.Sequence)
	}
	appendTaskStreamEvent(t, repository, task.ID, 3)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 3 {
		t.Fatalf("live sequence = %d, want 3", event.Sequence)
	}
	terminalizeTaskStream(t, store, repository, task.ID)
	if err := receiveTaskStreamDone(t, done); err != nil {
		t.Fatalf("TaskEventStream.Run() error = %v", err)
	}
	if count := store.watcherCount(); count != 0 {
		t.Fatalf("watcher count after terminal close = %d, want 0", count)
	}
}

func TestTaskEventStreamValidatesResumeBeforeOpeningWatches(t *testing.T) {
	// Rationale: an ahead resume id is a deterministic 400-class request error,
	// while an initially terminal Task must replay and close without live watches.
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
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
	if err != nil {
		t.Fatalf("OpenTaskEventStream(terminal) error = %v", err)
	}
	var emitted []TaskEventRecord
	if err := stream.Run(ctx, func(event TaskEventRecord) error {
		emitted = append(emitted, event)
		return nil
	}); err != nil {
		t.Fatalf("TaskEventStream.Run(terminal) error = %v", err)
	}
	if len(emitted) != 1 || emitted[0].Sequence != 1 {
		t.Fatalf("terminal replay = %#v, want sequence 1", emitted)
	}
	if starts := store.watchStartCount(); starts != 0 {
		t.Fatalf("terminal stream watch starts = %d, want 0", starts)
	}
}

func TestTaskEventStreamRecoversCompactionBySequence(t *testing.T) {
	// Rationale: etcd compaction is private infrastructure state. A live client
	// must continue from its last public sequence without cursor exposure.
	ctx := context.Background()
	store := newMemoryTaskStore()
	task := validTaskRecord(taskJournalTime())
	seedTaskRepositoryRunningTask(t, store, task)
	repository, err := newTaskRepository(store)
	if err != nil {
		t.Fatalf("newTaskRepository() error = %v", err)
	}
	appendTaskStreamEvent(t, repository, task.ID, 1)
	stream, err := repository.OpenTaskEventStream(ctx, task.ID, 0)
	if err != nil {
		t.Fatalf("OpenTaskEventStream() error = %v", err)
	}
	emitted := make(chan TaskEventRecord, 4)
	done := runTaskEventStream(ctx, stream, emitted)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 1 {
		t.Fatalf("initial sequence = %d, want 1", event.Sequence)
	}
	store.failWatch(taskEventScopePrefix(task.ID), errs.New(errs.KindCursorExpired, "compacted"))
	waitForTaskWatchStarts(t, store, 4)
	appendTaskStreamEvent(t, repository, task.ID, 2)
	if event := receiveTaskStreamEvent(t, emitted); event.Sequence != 2 {
		t.Fatalf("post-compaction sequence = %d, want 2", event.Sequence)
	}
	terminalizeTaskStream(t, store, repository, task.ID)
	if err := receiveTaskStreamDone(t, done); err != nil {
		t.Fatalf("TaskEventStream.Run() error = %v", err)
	}
}

func TestTaskEventStreamDisconnectsOnSequenceGap(t *testing.T) {
	// Rationale: a watched gap is durable corruption, not permission to skip a
	// sequence or reset the public resume point.
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
	emitted := make(chan TaskEventRecord, 1)
	done := runTaskEventStream(ctx, stream, emitted)
	input := taskEventInput(task.ID, 1, TaskEventStateRunning)
	prepared, err := prepareTaskEvent(task, input, nil, taskJournalTime().Add(time.Second))
	if err != nil {
		t.Fatalf("prepareTaskEvent() error = %v", err)
	}
	prepared.Event.Sequence = 2
	encoded, err := encodeTaskEventRecord(prepared.Event)
	if err != nil {
		t.Fatalf("encodeTaskEventRecord() error = %v", err)
	}
	result, err := store.Transact(ctx, []Condition{{Key: taskEventKey(task.ID, 2)}}, []Mutation{{
		Type: MutationPut, Key: taskEventKey(task.ID, 2), Value: encoded,
	}})
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
}

func TestTaskEventStreamCancellationJoinsWatches(t *testing.T) {
	// Rationale: abandoned and slow HTTP clients must not leave either etcd
	// watch goroutine alive after their request context is canceled.
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
	done := runTaskEventStream(ctx, stream, make(chan TaskEventRecord))
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
		taskEventInput(taskID, ordinal, TaskEventStateRunning),
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
	terminal, err := transitionTaskStatus(
		running,
		TaskStatusRunning,
		TaskStatusCompleted,
		taskJournalTime().Add(21*time.Second),
	)
	if err != nil {
		t.Fatalf("transitionTaskStatus(completed) error = %v", err)
	}
	encoded, err := encodeTaskRecord(terminal)
	if err != nil {
		t.Fatalf("encodeTaskRecord(terminal) error = %v", err)
	}
	result, err := store.Transact(context.Background(), []Condition{{
		Key: taskKey(taskID), ModRevision: current.Revision,
	}}, []Mutation{{Type: MutationPut, Key: taskKey(taskID), Value: encoded}})
	if err != nil || !result.Succeeded {
		t.Fatalf("persist terminal Task = %#v, %v", result, err)
	}
}

func runTaskEventStream(
	ctx context.Context,
	stream *TaskEventStream,
	emitted chan<- TaskEventRecord,
) <-chan error {
	done := make(chan error, 1)
	go func() {
		done <- stream.Run(ctx, func(event TaskEventRecord) error {
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

func receiveTaskStreamEvent(t *testing.T, emitted <-chan TaskEventRecord) TaskEventRecord {
	t.Helper()
	select {
	case event := <-emitted:
		return event
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Task event")
		return TaskEventRecord{}
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

func waitForTaskWatchStarts(t *testing.T, store *memoryTaskStore, count int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if store.watchStartCount() >= count {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("watch start count = %d, want at least %d", store.watchStartCount(), count)
}
