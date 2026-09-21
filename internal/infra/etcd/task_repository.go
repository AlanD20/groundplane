package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	resolutionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hostresolution"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
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
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

// PlatformResolverTaskPreparer is the private composition-root seam for
// rendering a generic dns-resolver Task. The repository owns publication and
// fencing; the Controller owns the registered Component renderer.
type PlatformResolverTaskPreparer func(
	context.Context,
	etcdstore.Versioned[componentrecord.Record],
	resolutionrecord.HostResolutionProjectionRecord,
	TaskRecord,
	*ComponentObservationRecord,
) (PlatformComponentTaskRenderInput, error)

// PlatformResolverComponentSelector selects the platform Component that
// provides the registered dns-resolver capability. The repository supplies
// only the fixed-revision platform Component records; capability selection is
// kept at the composition root.
type PlatformResolverComponentSelector func(
	context.Context,
	[]etcdstore.Versioned[componentrecord.Record],
) (etcdstore.Versioned[componentrecord.Record], error)

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
	Events   []taskjournal.TaskEventRecord
	Revision int64
}

// TaskListScope selects one immutable Task journal. ID is empty for global
// and platform scope, or contains the stable Tenant, Project, or Environment
// id selected by the corresponding scope.
type TaskListScope struct {
	Kind TaskListScopeKind
	ID   string
}

type TaskListScopeKind string

const (
	TaskListScopeGlobal            TaskListScopeKind = "global"
	TaskListScopePlatformWorkspace TaskListScopeKind = "platform_workspace"
	TaskListScopeTenantWorkspace   TaskListScopeKind = "tenant_workspace"
	TaskListScopeProject           TaskListScopeKind = "project"
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
	store                        taskRepositoryStore
	blueprintTerminalStore       blueprintTaskTerminalStore
	retryPolicy                  taskCASRetryPolicy
	platformResolverTaskPreparer PlatformResolverTaskPreparer
	platformResolverSelector     PlatformResolverComponentSelector
}

// SetPlatformResolverTaskPreparer installs the private renderer used only by
// automatic host-resolution reconciliation. It must be configured before the
// Controller starts accepting lifecycle acknowledgements.
func (repository *TaskRepository) SetPlatformResolverTaskPreparer(
	preparer PlatformResolverTaskPreparer,
) error {
	if repository == nil || preparer == nil {
		return errs.New(errs.KindInternal, "platform resolver Task preparer is required")
	}
	repository.platformResolverTaskPreparer = preparer
	return nil
}

// SetPlatformResolverComponentSelector installs the private capability
// selector used by automatic host-resolution reconciliation.
func (repository *TaskRepository) SetPlatformResolverComponentSelector(
	selector PlatformResolverComponentSelector,
) error {
	if repository == nil || selector == nil {
		return errs.New(errs.KindInternal, "platform resolver Component selector is required")
	}
	repository.platformResolverSelector = selector
	return nil
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
) (etcdstore.Versioned[TaskRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, taskID); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	result, err := repository.store.Get(ctx, taskjournal.TaskStorageKey(taskID))
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if result.Entry == nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
	}
	record, err := decodeTaskRecord(result.Entry.Value)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if record.ID != taskID || result.Entry.Key != taskjournal.TaskStorageKey(taskID) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task primary does not match its key")
	}
	indexKeys, err := taskJournalIndexKeys(record)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task owner indexes are corrupt")
	}
	indexes, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: indexKeys, Revision: result.ReadRevision,
	})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if indexes == nil || indexes.ReadRevision != result.ReadRevision || len(indexes.Values) != len(indexKeys) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index read is incomplete")
	}
	for index, value := range indexes.Values {
		if value == nil || value.Key != indexKeys[index] || string(value.Value) != taskID {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index membership is corrupt")
		}
	}
	return etcdstore.Versioned[TaskRecord]{
		Record: record, Revision: result.Entry.ModRevision, ReadRevision: result.ReadRevision,
	}, nil
}

// EnsureTaskJournalSchema is the clean-start gate for the ownership journal.
// A missing marker can be initialized only when the Task primary collection is
// empty. Any incompatible state or lost initialization CAS fails closed.
func (repository *TaskRepository) EnsureTaskJournalSchema(ctx context.Context) error {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return err
	}
	primary, err := repository.store.Range(ctx, etcdstore.RangeRequest{Prefix: taskjournal.TaskPrefix, Limit: 1})
	if err != nil {
		return err
	}
	if primary == nil {
		return errs.New(errs.KindInternal, "task journal schema primary read is missing")
	}
	marker, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskJournalSchemaKey}, Revision: primary.ReadRevision,
	})
	if err != nil {
		return err
	}
	if marker == nil || marker.ReadRevision != primary.ReadRevision || len(marker.Values) != 1 {
		return errs.New(errs.KindInternal, "task journal schema marker read is incomplete")
	}
	if marker.Values[0] != nil {
		if marker.Values[0].Key != taskjournal.TaskJournalSchemaKey || string(marker.Values[0].Value) != taskjournal.TaskJournalSchemaValue {
			return errs.New(errs.KindInternal, "task journal schema marker is incompatible")
		}
		return nil
	}
	if len(primary.Values) != 0 {
		return errs.New(errs.KindInternal, "task journal schema marker is missing for existing tasks")
	}
	initialized, err := repository.store.Transact(
		ctx,
		[]etcdstore.Condition{{Key: taskjournal.TaskJournalSchemaKey}, {Key: taskjournal.TaskPrefix, Prefix: true}},
		[]etcdstore.Mutation{{Type: etcdstore.MutationPut, Key: taskjournal.TaskJournalSchemaKey, Value: []byte(taskjournal.TaskJournalSchemaValue)}},
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
	Kind idempotencyrecord.IdempotencyScopeKind
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
	if parent.Record.Executor != taskjournal.TaskExecutorController || parent.Record.Status != taskjournal.TaskStatusRunning {
		return TaskInitiation{}, errs.New(
			errs.KindStateConflict,
			"system task initiation parent is not a running controller task",
		)
	}
	return newInheritedTaskInitiation(parent, taskjournal.TaskActorSystem)
}

func (repository *TaskRepository) ListTasks(
	ctx context.Context,
	request etcdstore.PageRequest,
) (etcdstore.Page[TaskRecord], error) {
	return repository.ListTasksByScope(ctx, TaskListScope{Kind: TaskListScopeGlobal}, request)
}

// ListTasksByScope returns the selected immutable journal at one fixed MVCC
// revision. Every scope uses the logical collection identity "tasks", so a
// cursor is route-agnostic between GET /tasks and GET /activity while still
// binding its exact owner scope.
func (repository *TaskRepository) ListTasksByScope(
	ctx context.Context,
	scope TaskListScope,
	request etcdstore.PageRequest,
) (etcdstore.Page[TaskRecord], error) {
	identity := func(record TaskRecord) string { return record.ID }
	switch scope.Kind {
	case TaskListScopeGlobal:
		if scope.ID != "" {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "global task scope cannot contain an id")
		}
		page, err := listPrimaryPage(
			ctx, repository.store, "tasks", "global", "-", taskjournal.TaskPrefix, ids.KindTask,
			request, decodeTaskRecord, identity, func(TaskRecord) bool { return true },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopePlatformWorkspace:
		if scope.ID != "" {
			return etcdstore.Page[TaskRecord]{}, errs.New(
				errs.KindValidationFailed,
				"platform task workspace scope cannot contain an id",
			)
		}
		page, err := listIndexPage(
			ctx, repository.store, "tasks", "workspace", "platform", taskjournal.TaskWorkspacePlatformPrefix,
			taskjournal.TaskStorageKey, ids.KindTask, request, decodeTaskRecord, identity,
			func(record TaskRecord) bool { return record.Owner.WorkspaceType == taskjournal.TaskWorkspacePlatform },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeTenantWorkspace:
		if ids.Validate(ids.KindTenant, scope.ID) != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "tenant task workspace scope is invalid")
		}
		page, err := listIndexPage(
			ctx, repository.store, "tasks", "workspace", scope.ID,
			taskjournal.TaskWorkspaceTenantPrefix+scope.ID+"/", taskjournal.TaskStorageKey, ids.KindTask, request,
			decodeTaskRecord, identity,
			func(record TaskRecord) bool {
				return record.Owner.WorkspaceType == taskjournal.TaskWorkspaceTenant && record.Owner.TenantID == scope.ID
			},
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeProject:
		if ids.Validate(ids.KindProject, scope.ID) != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "project task scope is invalid")
		}
		page, err := listFilteredPrimaryPage(
			ctx,
			repository.store,
			"tasks",
			"project",
			scope.ID,
			taskjournal.TaskPrefix,
			ids.KindTask,
			request,
			decodeTaskRecord,
			identity,
			func(record TaskRecord) bool { return record.Owner.ProjectID == scope.ID },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeEnvironment:
		if ids.Validate(ids.KindEnvironment, scope.ID) != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "environment task scope is invalid")
		}
		page, err := listIndexPage(
			ctx, repository.store, "tasks", "environment", scope.ID,
			taskjournal.TaskEnvironmentIndexPrefix+scope.ID+"/", taskjournal.TaskStorageKey, ids.KindTask, request,
			decodeTaskRecord, identity,
			func(record TaskRecord) bool { return record.Owner.EnvironmentID == scope.ID },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	default:
		return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "task list scope kind is invalid")
	}
}

func (repository *TaskRepository) verifyTaskOwnerPage(
	ctx context.Context,
	page etcdstore.Page[TaskRecord],
	listErr error,
) (etcdstore.Page[TaskRecord], error) {
	if listErr != nil {
		return etcdstore.Page[TaskRecord]{}, listErr
	}
	if len(page.Items) == 0 {
		return page, nil
	}
	if page.Revision <= 0 {
		return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindInternal, "task list revision is invalid")
	}
	keys := make([]string, 0, len(page.Items)*2)
	expectedTaskIDs := make([]string, 0, len(page.Items)*2)
	for _, item := range page.Items {
		ownerKeys, err := taskJournalIndexKeys(item.Record)
		if err != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner indexes are corrupt")
		}
		keys = append(keys, ownerKeys...)
		for range ownerKeys {
			expectedTaskIDs = append(expectedTaskIDs, item.Record.ID)
		}
	}
	indexes, err := getManyBatchedAtRevision(ctx, repository.store, keys, page.Revision)
	if err != nil {
		return etcdstore.Page[TaskRecord]{}, err
	}
	if indexes == nil {
		return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index page read is incomplete")
	}
	defer etcdstore.ClearValues(indexes.Values)
	if indexes.ReadRevision != page.Revision || len(indexes.Values) != len(keys) {
		return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index page read is incomplete")
	}
	for index, value := range indexes.Values {
		if value == nil || value.Key != keys[index] || string(value.Value) != expectedTaskIDs[index] {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner index membership is corrupt")
		}
	}
	return page, nil
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
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return TaskEventSnapshot{}, err
	}
	if err := recordcodec.ValidateID(ids.KindTask, taskID); err != nil {
		return TaskEventSnapshot{}, err
	}
	if revision < 0 {
		return TaskEventSnapshot{}, errs.New(errs.KindValidationFailed, "task event revision must not be negative")
	}
	taskResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision,
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
	eventsResult, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix:   taskjournal.TaskEventScopePrefix(taskID),
		Limit:    int64(taskjournal.MaximumTaskEvents) + 1,
		Revision: taskResult.ReadRevision,
	})
	if err != nil {
		return TaskEventSnapshot{}, err
	}
	if eventsResult.ReadRevision != taskResult.ReadRevision || eventsResult.More ||
		len(eventsResult.Values) > taskjournal.MaximumTaskEvents {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot exceeds its durable bounds")
	}
	events := make([]taskjournal.TaskEventRecord, len(eventsResult.Values))
	for index, value := range eventsResult.Values {
		sequence, err := taskjournal.TaskEventSequenceFromKey(taskID, value.Key)
		if err != nil {
			return TaskEventSnapshot{}, err
		}
		if sequence != firstTaskEventSequence(task)+uint64(index) {
			return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot has a sequence gap")
		}
		event, err := taskjournal.DecodeTaskEventRecord(value.Value)
		if err != nil {
			return TaskEventSnapshot{}, err
		}
		if event.Sequence != sequence || event.Identity.TaskID != taskID {
			return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event key does not match its record")
		}
		events[index] = event
	}
	if len(events) != int(task.EventCount) || firstTaskEventSequence(task) > 1 && len(task.EventCheckpoints) == 0 {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot does not match its task summary")
	}
	return TaskEventSnapshot{Task: task, Events: events, Revision: eventsResult.ReadRevision}, nil
}

func (repository *TaskRepository) verifyDuplicateEvent(
	ctx context.Context,
	revision int64,
	task TaskRecord,
	dedup taskjournal.TaskEventDedupRecord,
) error {
	if dedup.Sequence < firstTaskEventSequence(task) {
		for _, checkpoint := range task.EventCheckpoints {
			if checkpoint.Identity == dedup.Identity && checkpoint.Sequence == dedup.Sequence &&
				checkpoint.PayloadSHA256 == dedup.PayloadSHA256 {
				return nil
			}
		}
		return errs.New(errs.KindStateConflict, "task replay is outside retained authority")
	}
	if dedup.Sequence >= task.NextEventSequence {
		return errs.New(errs.KindInternal, "task event dedupe sequence is outside its task summary")
	}
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskjournal.TaskEventKey(task.ID, dedup.Sequence)}, Revision: revision,
	})
	if err != nil {
		return err
	}
	if len(result.Values) != 1 || result.Values[0] == nil {
		return errs.New(errs.KindInternal, "task event dedupe references a missing event")
	}
	event, err := taskjournal.DecodeTaskEventRecord(result.Values[0].Value)
	if err != nil {
		return err
	}
	if event.Sequence != dedup.Sequence || event.Identity != dedup.Identity ||
		event.PayloadSHA256 != dedup.PayloadSHA256 {
		return errs.New(errs.KindInternal, "task event dedupe does not match its event")
	}
	return nil
}
