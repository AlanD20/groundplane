package etcd

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	initialTaskCASDelay = 2 * time.Millisecond
	maximumTaskCASDelay = 128 * time.Millisecond
)

type taskRepositoryStore interface {
	Get(context.Context, string) (*GetResult, error)
	GetMany(context.Context, GetManyRequest) (*GetManyResult, error)
	Range(context.Context, RangeRequest) (*RangeResult, error)
	Transact(context.Context, []Condition, []Mutation) (TransactionResult, error)
}

// TaskEventAppend reports the durable sequence allocated by the Controller.
// Duplicate is true only when the same Agent event was already committed.
type TaskEventAppend struct {
	Sequence  uint64
	Revision  int64
	Duplicate bool
}

// TaskEventSnapshot is one complete, fixed-revision event journal. A Task has
// a hard limit of MaximumTaskEvents, so the repository never needs an
// unbounded read or a second revision to return the complete journal.
type TaskEventSnapshot struct {
	Task     TaskRecord
	Events   []TaskEventRecord
	Revision int64
}

// TaskListScope selects one immutable Task journal. ID is empty for global
// and platform scope, a Tenant id for tenant scope, and an Environment id for
// environment scope.
type TaskListScope struct {
	Kind TaskListScopeKind
	ID   string
}

type TaskListScopeKind string

const (
	TaskListScopeGlobal            TaskListScopeKind = "global"
	TaskListScopePlatformWorkspace TaskListScopeKind = "platform_workspace"
	TaskListScopeTenantWorkspace   TaskListScopeKind = "tenant_workspace"
	TaskListScopeEnvironment       TaskListScopeKind = "environment"
)

type taskCASRetryPolicy struct {
	initialDelay time.Duration
	maximumDelay time.Duration
	jitter       func(time.Duration) time.Duration
	wait         func(context.Context, time.Duration) error
}

// TaskRepository owns the accepted Task primary, durable queue/assignment
// lifecycle, and event-journal mechanics. Retention metadata is persisted by
// the journal codec; the daily Task pruning collector remains separate.
type TaskRepository struct {
	store       taskRepositoryStore
	retryPolicy taskCASRetryPolicy
}

func NewTaskRepository(store Store) (*TaskRepository, error) {
	return newTaskRepository(store)
}

func newTaskRepository(store taskRepositoryStore) (*TaskRepository, error) {
	return newTaskRepositoryWithRetryPolicy(store, defaultTaskCASRetryPolicy())
}

func newTaskRepositoryWithRetryPolicy(
	store taskRepositoryStore,
	retryPolicy taskCASRetryPolicy,
) (*TaskRepository, error) {
	if store == nil {
		return nil, errs.New(errs.KindInternal, "task store is required")
	}
	if err := validateTaskCASRetryPolicy(retryPolicy); err != nil {
		return nil, err
	}
	return &TaskRepository{store: store, retryPolicy: retryPolicy}, nil
}

func (repository *TaskRepository) GetTask(
	ctx context.Context,
	taskID string,
) (Versioned[TaskRecord], error) {
	if err := validateContext(ctx); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if err := validateStableID(ids.KindTask, taskID); err != nil {
		return Versioned[TaskRecord]{}, err
	}
	result, err := repository.store.Get(ctx, taskKey(taskID))
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if result.Entry == nil {
		return Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	record, err := decodeTaskRecord(result.Entry.Value)
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if record.ID != taskID || result.Entry.Key != taskKey(taskID) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task primary does not match its key")
	}
	indexKeys, err := taskOwnerIndexKeys(record.Owner, taskID)
	if err != nil {
		return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task owner indexes are corrupt")
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: indexKeys, Revision: result.ReadRevision,
	})
	if err != nil {
		return Versioned[TaskRecord]{}, err
	}
	if indexes == nil || indexes.ReadRevision != result.ReadRevision || len(indexes.Values) != len(indexKeys) {
		return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index read is incomplete")
	}
	for index, value := range indexes.Values {
		if value == nil || value.Key != indexKeys[index] || string(value.Value) != taskID {
			return Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index membership is corrupt")
		}
	}
	return Versioned[TaskRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

// EnsureTaskJournalSchema is the clean-start gate for the ownership journal.
// A missing marker can be initialized only when the Task primary collection is
// empty. Any incompatible state or lost initialization CAS fails closed.
func (repository *TaskRepository) EnsureTaskJournalSchema(ctx context.Context) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	primary, err := repository.store.Range(ctx, RangeRequest{Prefix: taskPrefix, Limit: 1})
	if err != nil {
		return err
	}
	if primary == nil {
		return errs.New(errs.KindInternal, "task journal schema primary read is missing")
	}
	marker, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskJournalSchemaKey}, Revision: primary.ReadRevision,
	})
	if err != nil {
		return err
	}
	if marker == nil || marker.ReadRevision != primary.ReadRevision || len(marker.Values) != 1 {
		return errs.New(errs.KindInternal, "task journal schema marker read is incomplete")
	}
	if marker.Values[0] != nil {
		if marker.Values[0].Key != taskJournalSchemaKey || string(marker.Values[0].Value) != taskJournalSchemaValue {
			return errs.New(errs.KindInternal, "task journal schema marker is incompatible")
		}
		return nil
	}
	if len(primary.Values) != 0 {
		return errs.New(errs.KindInternal, "task journal schema marker is missing for existing tasks")
	}
	initialized, err := repository.store.Transact(
		ctx,
		[]Condition{{Key: taskJournalSchemaKey}, {Key: taskPrefix, Prefix: true}},
		[]Mutation{{Type: MutationPut, Key: taskJournalSchemaKey, Value: []byte(taskJournalSchemaValue)}},
	)
	if err != nil {
		return err
	}
	if !initialized.Succeeded {
		return errs.New(errs.KindInternal, "task journal schema initialization compare failed")
	}
	return nil
}

// TaskRetryScope is the immutable durable owner used to domain-separate a
// human retry intent from the same idempotency key under another owner.
type TaskRetryScope struct {
	Kind IdempotencyScopeKind
	ID   string
}

func (repository *TaskRepository) GetTaskRetryScope(
	ctx context.Context,
	taskID string,
) (TaskRetryScope, error) {
	task, err := repository.GetTask(ctx, taskID)
	if err != nil {
		return TaskRetryScope{}, err
	}
	return taskOwnerRetryScope(task.Record.Owner)
}

func (repository *TaskRepository) GetSystemTaskInitiation(
	ctx context.Context,
	taskID string,
) (TaskInitiation, error) {
	parent, err := repository.GetTask(ctx, taskID)
	if err != nil {
		return TaskInitiation{}, err
	}
	if parent.Record.Executor != TaskExecutorController || parent.Record.Status != TaskStatusRunning {
		return TaskInitiation{}, errs.New(
			errs.KindStateConflict,
			"system task initiation parent is not a running controller task",
		)
	}
	return newInheritedTaskInitiation(parent, TaskActorSystem)
}

func (repository *TaskRepository) ListTasks(
	ctx context.Context,
	request PageRequest,
) (Page[TaskRecord], error) {
	return repository.ListTasksByScope(ctx, TaskListScope{Kind: TaskListScopeGlobal}, request)
}

// ListTasksByScope returns the selected immutable journal at one fixed MVCC
// revision. Every scope uses the logical collection identity "tasks", so a
// cursor is route-agnostic between GET /tasks and GET /activity while still
// binding its exact owner scope.
func (repository *TaskRepository) ListTasksByScope(
	ctx context.Context,
	scope TaskListScope,
	request PageRequest,
) (Page[TaskRecord], error) {
	identity := func(record TaskRecord) string { return record.ID }
	switch scope.Kind {
	case TaskListScopeGlobal:
		if scope.ID != "" {
			return Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "global task scope cannot contain an id")
		}
		page, err := listPrimaryPage(
			ctx, repository.store, "tasks", "global", "-", taskPrefix, ids.KindTask,
			request, decodeTaskRecord, identity, func(TaskRecord) bool { return true },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopePlatformWorkspace:
		if scope.ID != "" {
			return Page[TaskRecord]{}, errs.New(
				errs.KindValidationFailed,
				"platform task workspace scope cannot contain an id",
			)
		}
		page, err := listIndexPage(
			ctx, repository.store, "tasks", "workspace", "platform", taskWorkspacePlatformPrefix,
			taskKey, ids.KindTask, request, decodeTaskRecord, identity,
			func(record TaskRecord) bool { return record.Owner.WorkspaceType == TaskWorkspacePlatform },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeTenantWorkspace:
		if ids.Validate(ids.KindTenant, scope.ID) != nil {
			return Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "tenant task workspace scope is invalid")
		}
		page, err := listIndexPage(
			ctx, repository.store, "tasks", "workspace", scope.ID,
			taskWorkspaceTenantPrefix+scope.ID+"/", taskKey, ids.KindTask, request,
			decodeTaskRecord, identity,
			func(record TaskRecord) bool {
				return record.Owner.WorkspaceType == TaskWorkspaceTenant && record.Owner.TenantID == scope.ID
			},
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeEnvironment:
		if ids.Validate(ids.KindEnvironment, scope.ID) != nil {
			return Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "environment task scope is invalid")
		}
		page, err := listIndexPage(
			ctx, repository.store, "tasks", "environment", scope.ID,
			taskEnvironmentIndexPrefix+scope.ID+"/", taskKey, ids.KindTask, request,
			decodeTaskRecord, identity,
			func(record TaskRecord) bool { return record.Owner.EnvironmentID == scope.ID },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	default:
		return Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "task list scope kind is invalid")
	}
}

func (repository *TaskRepository) verifyTaskOwnerPage(
	ctx context.Context,
	page Page[TaskRecord],
	listErr error,
) (Page[TaskRecord], error) {
	if listErr != nil {
		return Page[TaskRecord]{}, listErr
	}
	if len(page.Items) == 0 {
		return page, nil
	}
	if page.Revision <= 0 {
		return Page[TaskRecord]{}, errs.New(errs.KindInternal, "task list revision is invalid")
	}
	keys := make([]string, 0, len(page.Items)*2)
	expectedTaskIDs := make([]string, 0, len(page.Items)*2)
	for _, item := range page.Items {
		ownerKeys, err := taskOwnerIndexKeys(item.Record.Owner, item.Record.ID)
		if err != nil {
			return Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner indexes are corrupt")
		}
		keys = append(keys, ownerKeys...)
		for range ownerKeys {
			expectedTaskIDs = append(expectedTaskIDs, item.Record.ID)
		}
	}
	indexes, err := getManyBatchedAtRevision(ctx, repository.store, keys, page.Revision)
	if err != nil {
		return Page[TaskRecord]{}, err
	}
	defer clearKeyValues(indexes.Values)
	if indexes == nil || indexes.ReadRevision != page.Revision || len(indexes.Values) != len(keys) {
		return Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index page read is incomplete")
	}
	for index, value := range indexes.Values {
		if value == nil || value.Key != keys[index] || string(value.Value) != expectedTaskIDs[index] {
			return Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index membership is corrupt")
		}
	}
	return page, nil
}

// AppendTaskEvent persists the event, updated Task summary, and deduplication
// record in one CAS transaction. It never converts a transaction error into
// success by inspecting later key state: that requires the separate durable
// idempotency evidence that ADR 0013 still gates.
func (repository *TaskRepository) AppendTaskEvent(
	ctx context.Context,
	input TaskEventInput,
	receivedAt time.Time,
) (TaskEventAppend, error) {
	if err := validateContext(ctx); err != nil {
		return TaskEventAppend{}, err
	}
	if err := validateTaskEventIdentity(input.Identity); err != nil {
		return TaskEventAppend{}, err
	}

	conflicts := 0
	for {
		if err := ctx.Err(); err != nil {
			return TaskEventAppend{}, err
		}
		result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
			taskKey(input.Identity.TaskID), taskEventDedupKey(input.Identity),
		}})
		if err != nil {
			return TaskEventAppend{}, err
		}
		if len(result.Values) != 2 {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event read returned an invalid record count")
		}
		taskValue := result.Values[0]
		if taskValue == nil {
			return TaskEventAppend{}, errs.Newf(
				errs.KindTaskNotFound,
				"task not found: %s",
				input.Identity.TaskID,
			)
		}
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return TaskEventAppend{}, err
		}
		if task.ID != input.Identity.TaskID || taskValue.Key != taskKey(task.ID) {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event read returned a mismatched task")
		}

		var existing *TaskEventDedupRecord
		if result.Values[1] != nil {
			dedup, err := decodeTaskEventDedupRecord(result.Values[1].Value)
			if err != nil {
				return TaskEventAppend{}, err
			}
			existing = &dedup
		}
		prepared, err := prepareTaskEvent(task, input, existing, receivedAt)
		if err != nil {
			return TaskEventAppend{}, err
		}
		if prepared.Duplicate {
			if err := repository.verifyDuplicateEvent(ctx, result.ReadRevision, task, *existing); err != nil {
				return TaskEventAppend{}, err
			}
			return TaskEventAppend{
				Sequence: prepared.Sequence, Revision: result.ReadRevision, Duplicate: true,
			}, nil
		}

		eventKey := taskEventKey(task.ID, prepared.Sequence)
		eventAtRevision, err := repository.store.GetMany(ctx, GetManyRequest{
			Keys: []string{eventKey}, Revision: result.ReadRevision,
		})
		if err != nil {
			return TaskEventAppend{}, err
		}
		if len(eventAtRevision.Values) != 1 {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event collision read is invalid")
		}
		if eventAtRevision.Values[0] != nil {
			return TaskEventAppend{}, errs.New(errs.KindInternal, "task event sequence is already occupied")
		}

		encodedTask, err := encodeTaskRecord(prepared.Task)
		if err != nil {
			return TaskEventAppend{}, err
		}
		encodedEvent, err := encodeTaskEventRecord(prepared.Event)
		if err != nil {
			return TaskEventAppend{}, err
		}
		encodedDedup, err := encodeTaskEventDedupRecord(prepared.Dedup)
		if err != nil {
			return TaskEventAppend{}, err
		}
		transaction, err := repository.store.Transact(
			ctx,
			[]Condition{
				{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
				{Key: taskEventDedupKey(input.Identity)},
				{Key: eventKey},
			},
			[]Mutation{
				{Type: MutationPut, Key: taskKey(task.ID), Value: encodedTask},
				{Type: MutationPut, Key: eventKey, Value: encodedEvent},
				{Type: MutationPut, Key: taskEventDedupKey(input.Identity), Value: encodedDedup},
			},
		)
		if err != nil {
			return TaskEventAppend{}, err
		}
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskEventAppend{}, err
			}
			continue
		}
		return TaskEventAppend{Sequence: prepared.Sequence, Revision: transaction.Revision}, nil
	}
}

func defaultTaskCASRetryPolicy() taskCASRetryPolicy {
	return taskCASRetryPolicy{
		initialDelay: initialTaskCASDelay,
		maximumDelay: maximumTaskCASDelay,
		jitter: func(bound time.Duration) time.Duration {
			if bound <= 0 {
				return 0
			}
			return time.Duration(rand.Int64N(int64(bound) + 1))
		},
		wait: waitForTaskCASRetry,
	}
}

func validateTaskCASRetryPolicy(policy taskCASRetryPolicy) error {
	if policy.initialDelay <= 0 || policy.maximumDelay < policy.initialDelay ||
		policy.jitter == nil || policy.wait == nil {
		return errs.New(errs.KindInternal, "task CAS retry policy is invalid")
	}
	return nil
}

func (policy taskCASRetryPolicy) waitAfterConflict(ctx context.Context, conflicts int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	delay := policy.initialDelay
	for exponent := 1; exponent < conflicts && delay < policy.maximumDelay; exponent++ {
		if delay > policy.maximumDelay/2 {
			delay = policy.maximumDelay
			break
		}
		delay *= 2
	}
	jitterBound := delay / 2
	jitter := policy.jitter(jitterBound)
	if jitter < 0 || jitter > jitterBound {
		return errs.New(errs.KindInternal, "task CAS retry jitter is outside its bound")
	}
	if delay > policy.maximumDelay-jitter {
		delay = policy.maximumDelay
	} else {
		delay += jitter
	}
	return policy.wait(ctx, delay)
}

func waitForTaskCASRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ListTaskEvents returns the complete journal at revision. Revision zero
// captures a new linearizable view; a positive revision reproduces that fixed
// MVCC view. The Task summary and event set must agree exactly.
func (repository *TaskRepository) ListTaskEvents(
	ctx context.Context,
	taskID string,
	revision int64,
) (TaskEventSnapshot, error) {
	if err := validateContext(ctx); err != nil {
		return TaskEventSnapshot{}, err
	}
	if err := validateStableID(ids.KindTask, taskID); err != nil {
		return TaskEventSnapshot{}, err
	}
	if revision < 0 {
		return TaskEventSnapshot{}, errs.New(errs.KindValidationFailed, "task event revision must not be negative")
	}
	taskResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskKey(taskID)}, Revision: revision,
	})
	if err != nil {
		return TaskEventSnapshot{}, err
	}
	if len(taskResult.Values) != 1 {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot returned an invalid task count")
	}
	if taskResult.Values[0] == nil {
		return TaskEventSnapshot{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	task, err := decodeTaskRecord(taskResult.Values[0].Value)
	if err != nil {
		return TaskEventSnapshot{}, err
	}
	if task.ID != taskID {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot has a mismatched task")
	}
	eventsResult, err := repository.store.Range(ctx, RangeRequest{
		Prefix:   taskEventScopePrefix(taskID),
		Limit:    int64(MaximumTaskEvents) + 1,
		Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return TaskEventSnapshot{}, err
	}
	if eventsResult.ReadRevision != taskResult.ReadRevision || eventsResult.More ||
		len(eventsResult.Values) > MaximumTaskEvents {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot exceeds its durable bounds")
	}
	events := make([]TaskEventRecord, len(eventsResult.Values))
	for index, value := range eventsResult.Values {
		sequence, err := taskEventSequenceFromKey(taskID, value.Key)
		if err != nil {
			return TaskEventSnapshot{}, err
		}
		if sequence != uint64(index)+1 {
			return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot has a sequence gap")
		}
		event, err := decodeTaskEventRecord(value.Value)
		if err != nil {
			return TaskEventSnapshot{}, err
		}
		if event.Sequence != sequence || event.Identity.TaskID != taskID {
			return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event key does not match its record")
		}
		events[index] = event
	}
	if len(events) != int(task.EventCount) || task.NextEventSequence != uint64(len(events))+1 {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot does not match its task summary")
	}
	return TaskEventSnapshot{Task: task, Events: events, Revision: eventsResult.ReadRevision}, nil
}

func (repository *TaskRepository) verifyDuplicateEvent(
	ctx context.Context,
	revision int64,
	task TaskRecord,
	dedup TaskEventDedupRecord,
) error {
	if dedup.Sequence > uint64(task.EventCount) || dedup.Sequence >= task.NextEventSequence {
		return errs.New(errs.KindInternal, "task event dedupe sequence is outside its task summary")
	}
	result, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{taskEventKey(task.ID, dedup.Sequence)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return errs.New(errs.KindInternal, "task event dedupe references a missing event")
	}
	event, err := decodeTaskEventRecord(result.Values[0].Value)
	if err != nil {
		return err
	}
	if event.Sequence != dedup.Sequence || event.Identity != dedup.Identity ||
		event.PayloadSHA256 != dedup.PayloadSHA256 {
		return errs.New(errs.KindInternal, "task event dedupe does not match its event")
	}
	return nil
}
