package etcd

import (
	"context"
	"errors"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"sync"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskEventWatchStore interface {
	Watch(context.Context, string, int64) (*etcdstore.WatchStream, error)
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
	task   *etcdstore.WatchStream
	events *etcdstore.WatchStream
	once   sync.Once
}

// OpenTaskEventStream validates the public sequence resume point against one
// fixed Task-and-journal snapshot and opens both watches before returning.
func (repository *TaskRepository) OpenTaskEventStream(
	ctx context.Context,
	taskID string,
	after uint64,
) (*TaskEventStream, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	snapshot, err := repository.ListTaskEvents(ctx, taskID, 0)
	if err != nil {
		return nil, err
	}
	if after >= snapshot.Task.NextEventSequence {
		return nil, errs.New(errs.KindMalformedRequest, "Last-Event-ID is ahead of the task journal")
	}
	if after != 0 && after < firstTaskEventSequence(snapshot.Task)-1 {
		return nil, errs.New(errs.KindCursorExpired, "Task event history was trimmed; request a fresh snapshot")
	}
	stream := &TaskEventStream{
		repository: repository,
		taskID:     taskID,
		after:      after,
		snapshot:   snapshot,
	}
	if taskjournal.IsTerminalTaskStatus(snapshot.Task.Status) {
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
	emit func(taskjournal.TaskEventRecord) error,
) error {
	if stream == nil || stream.repository == nil || stream.taskID == "" || emit == nil {
		return errs.New(errs.KindInternal, "task event stream is invalid")
	}
	if stream.watches != nil {
		defer func() { stream.watches.stop() }()
	}
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}

	last, err := emitTaskEventSuffix(ctx, stream.snapshot, stream.after, emit)
	if err != nil {
		return err
	}
	if taskjournal.IsTerminalTaskStatus(stream.snapshot.Task.Status) {
		return nil
	}

	taskEvents, journalEvents := stream.watches.task.Events, stream.watches.events.Events
	for {
		var watchErr error
		var ok bool
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-journalEvents:
			if !ok {
				// Closure cannot supersede a terminal error queued on the other channel.
				journalEvents = nil
				continue
			}
			last, err = stream.consumeEvent(ctx, event, last, emit)
			if err != nil {
				return err
			}
			continue
		case watchErr, ok = <-stream.watches.events.Errors:
		case event, ok := <-taskEvents:
			if !ok {
				taskEvents = nil
				continue
			}
			terminalRevision, terminal, consumeErr := stream.consumeTask(event)
			if consumeErr != nil {
				return consumeErr
			}
			if terminal {
				return stream.finalDrain(ctx, terminalRevision, last, emit)
			}
			continue
		case watchErr, ok = <-stream.watches.task.Errors:
		}
		if contextErr := ctx.Err(); contextErr != nil {
			return contextErr
		}
		if !ok || watchErr == nil {
			return errs.New(errs.KindInternal, "task stream watch failed without an error")
		}
		var done bool
		last, done, err = stream.recoverWatch(ctx, watchErr, last, emit)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		taskEvents, journalEvents = stream.watches.task.Events, stream.watches.events.Events
	}
}

func (stream *TaskEventStream) consumeEvent(
	ctx context.Context,
	event etcdstore.Event,
	last uint64,
	emit func(taskjournal.TaskEventRecord) error,
) (uint64, error) {
	if event.Type == etcdstore.EventDelete {
		sequence, err := taskjournal.TaskEventSequenceFromKey(stream.taskID, event.Key)
		if err != nil {
			return last, err
		}
		if sequence <= last {
			return last, nil
		}
		return last, errs.New(errs.KindCursorExpired, "Task event history overtook its reader")
	}
	if event.Type != etcdstore.EventPut {
		return last, errs.New(errs.KindInternal, "task event journal was deleted during streaming")
	}
	sequence, err := taskjournal.TaskEventSequenceFromKey(stream.taskID, event.Key)
	if err != nil {
		return last, err
	}
	record, err := taskjournal.DecodeTaskEventRecord(event.Value)
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

func (stream *TaskEventStream) consumeTask(event etcdstore.Event) (int64, bool, error) {
	if event.Type != etcdstore.EventPut {
		return 0, false, errs.New(errs.KindTaskNotFound, "Task disappeared during event streaming")
	}
	if event.Key != taskjournal.TaskStorageKey(stream.taskID) {
		return 0, false, errs.New(errs.KindInternal, "watched Task primary does not match its key")
	}
	record, err := DecodeTaskRecord(event.Value)
	if err != nil {
		return 0, false, err
	}
	if record.ID != stream.taskID || event.ModRevision <= 0 {
		return 0, false, errs.New(errs.KindInternal, "watched Task primary is inconsistent")
	}
	return event.ModRevision, taskjournal.IsTerminalTaskStatus(record.Status), nil
}

func (stream *TaskEventStream) recoverWatch(
	ctx context.Context,
	watchErr error,
	last uint64,
	emit func(taskjournal.TaskEventRecord) error,
) (uint64, bool, error) {
	if !errors.Is(watchErr, errs.New(errs.KindCursorExpired, "")) {
		return last, false, watchErr
	}
	stream.watches.stop()
	snapshot, err := stream.repository.ListTaskEvents(ctx, stream.taskID, 0)
	if err != nil {
		return last, false, err
	}
	if snapshot.Task.NextEventSequence <= last {
		return last, false, errs.New(errs.KindInternal, "task event journal regressed during compaction recovery")
	}
	if taskjournal.IsTerminalTaskStatus(snapshot.Task.Status) {
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
	emit func(taskjournal.TaskEventRecord) error,
) error {
	stream.watches.stop()
	snapshot, err := stream.repository.ListTaskEvents(ctx, stream.taskID, revision)
	if errors.Is(err, errs.New(errs.KindCursorExpired, "")) {
		snapshot, err = stream.repository.ListTaskEvents(ctx, stream.taskID, 0)
	}
	if err != nil {
		return err
	}
	if !taskjournal.IsTerminalTaskStatus(snapshot.Task.Status) || snapshot.Task.NextEventSequence <= last {
		return errs.New(errs.KindInternal, "terminal Task event drain is inconsistent")
	}
	_, err = emitTaskEventSuffix(ctx, snapshot, last, emit)
	return err
}

func emitTaskEventSuffix(
	ctx context.Context,
	snapshot TaskEventSnapshot,
	last uint64,
	emit func(taskjournal.TaskEventRecord) error,
) (uint64, error) {
	if snapshot.Task.NextEventSequence <= last {
		return last, errs.New(errs.KindInternal, "task event snapshot is behind the stream")
	}
	first := firstTaskEventSequence(snapshot.Task)
	if last == 0 {
		last = first - 1
	}
	if last < first-1 {
		return last, errs.New(errs.KindCursorExpired, "Task event history was trimmed; request a fresh snapshot")
	}
	for _, event := range snapshot.Events {
		if event.Sequence <= last {
			continue
		}
		if event.Sequence != last+1 {
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
	emit func(taskjournal.TaskEventRecord) error,
	event taskjournal.TaskEventRecord,
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
	taskWatch, err := store.Watch(watchContext, taskjournal.TaskStorageKey(taskID), revision)
	if err != nil {
		cancel()
		return nil, err
	}
	eventWatch, err := store.Watch(watchContext, taskjournal.TaskEventScopePrefix(taskID), revision)
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

func drainWatchStream(stream *etcdstore.WatchStream) {
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
