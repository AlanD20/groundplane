package etcd

import (
	"context"
	"errors"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskEventWatchStore interface {
	Watch(context.Context, string, int64) (*WatchStream, error)
}

// TaskEventStream owns one gap-free Task primary and event-journal follow.
// Call Run exactly once. Run joins both Store watches before returning.
type TaskEventStream struct {
	repository *TaskRepository
	store      taskEventWatchStore
	taskID     string
	after      uint64
	snapshot   TaskEventSnapshot
	watches    *taskEventWatches
}

type taskEventWatches struct {
	cancel context.CancelFunc
	task   *WatchStream
	events *WatchStream
	once   sync.Once
}

// OpenTaskEventStream validates the public sequence resume point against one
// fixed Task-and-journal snapshot and opens both watches before returning.
func (repository *TaskRepository) OpenTaskEventStream(
	ctx context.Context,
	taskID string,
	after uint64,
) (*TaskEventStream, error) {
	if err := validateContext(ctx); err != nil {
		return nil, err
	}
	snapshot, err := repository.ListTaskEvents(ctx, taskID, 0)
	if err != nil {
		return nil, err
	}
	if after > uint64(len(snapshot.Events)) {
		return nil, errs.New(errs.KindMalformedRequest, "Last-Event-ID is ahead of the task journal")
	}
	stream := &TaskEventStream{
		repository: repository,
		taskID:     taskID,
		after:      after,
		snapshot:   snapshot,
	}
	if isTerminalTaskStatus(snapshot.Task.Status) {
		return stream, nil
	}
	store, ok := repository.store.(taskEventWatchStore)
	if !ok {
		return nil, errs.New(errs.KindInternal, "task event watch store is unavailable")
	}
	stream.store = store
	stream.watches, err = openTaskEventWatches(ctx, store, taskID, snapshot.Revision+1)
	if err != nil {
		return nil, err
	}
	return stream, nil
}

// Run emits the requested snapshot suffix followed by live durable events.
// It returns nil only after an authoritative terminal Task drain.
func (stream *TaskEventStream) Run(
	ctx context.Context,
	emit func(TaskEventRecord) error,
) error {
	if stream == nil || stream.repository == nil || stream.taskID == "" || emit == nil {
		return errs.New(errs.KindInternal, "task event stream is invalid")
	}
	if stream.watches != nil {
		defer func() { stream.watches.stop() }()
	}
	if err := validateContext(ctx); err != nil {
		return err
	}

	last, err := emitTaskEventSuffix(ctx, stream.snapshot, stream.after, emit)
	if err != nil {
		return err
	}
	if isTerminalTaskStatus(stream.snapshot.Task.Status) {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-stream.watches.events.Events:
			if !ok {
				return errs.New(errs.KindInternal, "task event watch closed unexpectedly")
			}
			last, err = stream.consumeEvent(ctx, event, last, emit)
			if err != nil {
				return err
			}
		case watchErr, ok := <-stream.watches.events.Errors:
			if !ok {
				return errs.New(errs.KindInternal, "task event watch failed without an error")
			}
			var done bool
			last, done, err = stream.recoverWatch(ctx, watchErr, last, emit)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		case event, ok := <-stream.watches.task.Events:
			if !ok {
				return errs.New(errs.KindInternal, "Task primary watch closed unexpectedly")
			}
			terminalRevision, terminal, consumeErr := stream.consumeTask(event)
			if consumeErr != nil {
				return consumeErr
			}
			if terminal {
				return stream.finalDrain(ctx, terminalRevision, last, emit)
			}
		case watchErr, ok := <-stream.watches.task.Errors:
			if !ok {
				return errs.New(errs.KindInternal, "Task primary watch failed without an error")
			}
			var done bool
			last, done, err = stream.recoverWatch(ctx, watchErr, last, emit)
			if err != nil {
				return err
			}
			if done {
				return nil
			}
		}
	}
}

func (stream *TaskEventStream) consumeEvent(
	ctx context.Context,
	event Event,
	last uint64,
	emit func(TaskEventRecord) error,
) (uint64, error) {
	if event.Type != EventPut {
		return last, errs.New(errs.KindInternal, "task event journal was deleted during streaming")
	}
	sequence, err := taskEventSequenceFromKey(stream.taskID, event.Key)
	if err != nil {
		return last, err
	}
	record, err := decodeTaskEventRecord(event.Value)
	if err != nil {
		return last, err
	}
	if record.Sequence != sequence || record.Identity.TaskID != stream.taskID {
		return last, errs.New(errs.KindInternal, "watched task event does not match its key")
	}
	if sequence <= last {
		return last, nil
	}
	if sequence != last+1 {
		return last, errs.New(errs.KindInternal, "watched task event has a sequence gap")
	}
	if err := emitTaskEvent(ctx, emit, record); err != nil {
		return last, err
	}
	return sequence, nil
}

func (stream *TaskEventStream) consumeTask(event Event) (int64, bool, error) {
	if event.Type != EventPut {
		return 0, false, errs.New(errs.KindTaskNotFound, "Task disappeared during event streaming")
	}
	if event.Key != taskKey(stream.taskID) {
		return 0, false, errs.New(errs.KindInternal, "watched Task primary does not match its key")
	}
	record, err := decodeTaskRecord(event.Value)
	if err != nil {
		return 0, false, err
	}
	if record.ID != stream.taskID || event.ModRevision <= 0 {
		return 0, false, errs.New(errs.KindInternal, "watched Task primary is inconsistent")
	}
	return event.ModRevision, isTerminalTaskStatus(record.Status), nil
}

func (stream *TaskEventStream) recoverWatch(
	ctx context.Context,
	watchErr error,
	last uint64,
	emit func(TaskEventRecord) error,
) (uint64, bool, error) {
	if !errors.Is(watchErr, errs.New(errs.KindCursorExpired, "")) {
		return last, false, watchErr
	}
	stream.watches.stop()
	snapshot, err := stream.repository.ListTaskEvents(ctx, stream.taskID, 0)
	if err != nil {
		return last, false, err
	}
	if uint64(len(snapshot.Events)) < last {
		return last, false, errs.New(errs.KindInternal, "task event journal regressed during compaction recovery")
	}
	if isTerminalTaskStatus(snapshot.Task.Status) {
		last, err = emitTaskEventSuffix(ctx, snapshot, last, emit)
		return last, err == nil, err
	}
	watches, err := openTaskEventWatches(ctx, stream.store, stream.taskID, snapshot.Revision+1)
	if err != nil {
		return last, false, err
	}
	stream.watches = watches
	last, err = emitTaskEventSuffix(ctx, snapshot, last, emit)
	return last, false, err
}

func (stream *TaskEventStream) finalDrain(
	ctx context.Context,
	revision int64,
	last uint64,
	emit func(TaskEventRecord) error,
) error {
	stream.watches.stop()
	snapshot, err := stream.repository.ListTaskEvents(ctx, stream.taskID, revision)
	if errors.Is(err, errs.New(errs.KindCursorExpired, "")) {
		snapshot, err = stream.repository.ListTaskEvents(ctx, stream.taskID, 0)
	}
	if err != nil {
		return err
	}
	if !isTerminalTaskStatus(snapshot.Task.Status) || uint64(len(snapshot.Events)) < last {
		return errs.New(errs.KindInternal, "terminal Task event drain is inconsistent")
	}
	_, err = emitTaskEventSuffix(ctx, snapshot, last, emit)
	return err
}

func emitTaskEventSuffix(
	ctx context.Context,
	snapshot TaskEventSnapshot,
	last uint64,
	emit func(TaskEventRecord) error,
) (uint64, error) {
	if uint64(len(snapshot.Events)) < last {
		return last, errs.New(errs.KindInternal, "task event snapshot is behind the stream")
	}
	for index := last; index < uint64(len(snapshot.Events)); index++ {
		event := snapshot.Events[index]
		if event.Sequence != index+1 {
			return last, errs.New(errs.KindInternal, "task event snapshot suffix has a sequence gap")
		}
		if err := emitTaskEvent(ctx, emit, event); err != nil {
			return last, err
		}
		last = event.Sequence
	}
	return last, nil
}

func emitTaskEvent(
	ctx context.Context,
	emit func(TaskEventRecord) error,
	event TaskEventRecord,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := emit(event); err != nil {
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		return err
	}
	return nil
}

func openTaskEventWatches(
	ctx context.Context,
	store taskEventWatchStore,
	taskID string,
	revision int64,
) (*taskEventWatches, error) {
	watchContext, cancel := context.WithCancel(ctx)
	taskWatch, err := store.Watch(watchContext, taskKey(taskID), revision)
	if err != nil {
		cancel()
		return nil, err
	}
	eventWatch, err := store.Watch(watchContext, taskEventScopePrefix(taskID), revision)
	if err != nil {
		cancel()
		drainWatchStream(taskWatch)
		return nil, err
	}
	return &taskEventWatches{cancel: cancel, task: taskWatch, events: eventWatch}, nil
}

func (watches *taskEventWatches) stop() {
	if watches == nil {
		return
	}
	watches.once.Do(func() {
		watches.cancel()
		drainWatchStream(watches.task)
		drainWatchStream(watches.events)
	})
}

func drainWatchStream(stream *WatchStream) {
	if stream == nil {
		return
	}
	events := stream.Events
	errorsFound := stream.Errors
	for events != nil || errorsFound != nil {
		select {
		case _, ok := <-events:
			if !ok {
				events = nil
			}
		case _, ok := <-errorsFound:
			if !ok {
				errorsFound = nil
			}
		}
	}
}
