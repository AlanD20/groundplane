package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/rollingrelease"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	recordquery "github.com/AlanD20/groundplane/internal/infra/etcd/recordquery"
	groupstore "github.com/AlanD20/groundplane/internal/infra/etcd/releasegroups"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskRepositoryStore interface {
	Get(context.Context, string) (*etcdstore.GetResult, error)
	GetMany(context.Context, etcdstore.GetManyRequest) (*etcdstore.GetManyResult, error)
	Range(context.Context, etcdstore.RangeRequest) (*etcdstore.RangeResult, error)
	MeasureTransaction(
		context.Context,
		[]etcdstore.Condition,
		[]etcdstore.Mutation,
	) (etcdstore.TransactionBudget, error)
	Transact(context.Context, []etcdstore.Condition, []etcdstore.Mutation) (etcdstore.TransactionResult, error)
}

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
	StartDecision    taskjournal.StepStartDecision
	EventOrdinalBase uint64
	Sequence         uint64
	Revision         int64
	Duplicate        bool
	RollingBudget    *rollingrelease.StepBudget
}

// TaskEventSnapshot is one complete, fixed-revision event journal. A Task has
// a hard limit of MaximumTaskEvents, so the repository never needs an
// unbounded read or a second revision to return the complete journal.
type TaskEventSnapshot struct {
	Task                 TaskRecord
	Events               []taskjournal.TaskEventRecord
	Revision             int64
	DiagnosticsExpiredAt *time.Time
}

// TaskListScope selects one immutable Task journal. ID is empty for global
// and platform scope, or contains the stable Tenant, Project, or Environment
// id selected by the corresponding scope.
type TaskListScope struct {
	Kind         TaskListScopeKind
	ID           string
	Status       taskjournal.TaskStatus
	Type         taskjournal.TaskType
	ResourceKind string
}

type TaskListScopeKind string

const (
	TaskListScopeGlobal            TaskListScopeKind = "global"
	TaskListScopePlatformWorkspace TaskListScopeKind = "platform_workspace"
	TaskListScopeTenantWorkspace   TaskListScopeKind = "tenant_workspace"
	TaskListScopeProject           TaskListScopeKind = "project"
	TaskListScopeEnvironment       TaskListScopeKind = "environment"
)

// TaskRepository owns the accepted Task primary, durable queue/assignment
// lifecycle, and event-journal mechanics. Retention metadata is persisted by
// the journal codec; the daily Task pruning collector remains separate.
type TaskRepository struct {
	*groupstore.Preparer
	store                    taskRepositoryStore
	retryPolicy              taskCASRetryPolicy
	platformResolverSelector PlatformResolverComponentSelector
	dispatchClaims           chan struct{}
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

func NewTaskRepository(store taskRepositoryStore) (*TaskRepository, error) {
	return newTaskRepository(store)
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
	return &TaskRepository{Preparer: groupstore.NewPreparer(store), store: store, retryPolicy: retryPolicy,
		dispatchClaims: make(chan struct{}, 1)}, nil
}

// TaskRetryScope is the immutable durable owner used to domain-separate a
// human retry intent from the same idempotency key under another owner.
type TaskRetryScope struct {
	Kind idempotencyrecord.IdempotencyScopeKind
	ID   string
}

func visibleOperatorTask(record TaskRecord) bool {
	return !taskjournal.IsInternalChild(record.Params)
}

// ListTasksByScope returns the selected immutable journal at one fixed MVCC
// revision. The collection identity binds all filters, so a
// cursor is route-agnostic between GET /tasks and GET /activity while still
// binding its exact owner scope.
func (repository *TaskRepository) ListTasksByScope(
	ctx context.Context,
	scope TaskListScope,
	request etcdstore.PageRequest,
) (etcdstore.Page[TaskRecord], error) {
	filters := taskjournal.ListFilter{Status: scope.Status, Type: scope.Type, ResourceKind: scope.ResourceKind}
	if err := filters.Validate(); err != nil {
		return etcdstore.Page[TaskRecord]{}, err
	}
	collection := filters.Collection()
	visible := func(record TaskRecord) bool {
		return visibleOperatorTask(record) &&
			filters.Matches(record.Status, record.Type, record.Params[taskjournal.TaskResourceKindParam])
	}
	identity := func(record TaskRecord) string { return record.ID }
	switch scope.Kind {
	case TaskListScopeGlobal:
		if scope.ID != "" {
			return etcdstore.Page[TaskRecord]{}, errs.New(
				errs.KindValidationFailed,
				"global task scope cannot contain an id",
			)
		}
		page, err := recordquery.ListFilteredPrimary(
			ctx, repository.store, collection, "global", "-", taskjournal.TaskPrefix, ids.KindTask,
			request.NewestFirst(), DecodeTaskRecord, identity, visible,
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopePlatformWorkspace:
		if scope.ID != "" {
			return etcdstore.Page[TaskRecord]{}, errs.New(
				errs.KindValidationFailed,
				"platform task workspace scope cannot contain an id",
			)
		}
		page, err := recordquery.ListVisibleIndex(
			ctx, repository.store, collection, "workspace", "platform", taskjournal.TaskWorkspacePlatformPrefix,
			taskjournal.TaskStorageKey, ids.KindTask, request.NewestFirst(), DecodeTaskRecord, identity,
			func(record TaskRecord) bool { return record.Owner.WorkspaceType == taskjournal.TaskWorkspacePlatform },
			visible,
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeTenantWorkspace:
		if ids.Validate(ids.KindTenant, scope.ID) != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(
				errs.KindValidationFailed,
				"tenant task workspace scope is invalid",
			)
		}
		page, err := recordquery.ListVisibleIndex(
			ctx, repository.store, collection, "workspace", scope.ID,
			taskjournal.TaskWorkspaceTenantPrefix+scope.ID+"/", taskjournal.TaskStorageKey, ids.KindTask,
			request.NewestFirst(), DecodeTaskRecord, identity,
			func(record TaskRecord) bool {
				return record.Owner.WorkspaceType == taskjournal.TaskWorkspaceTenant &&
					record.Owner.TenantID == scope.ID
			},
			visible,
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeProject:
		if ids.Validate(ids.KindProject, scope.ID) != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindValidationFailed, "project task scope is invalid")
		}
		page, err := recordquery.ListFilteredPrimary(
			ctx,
			repository.store,
			collection,
			"project",
			scope.ID,
			taskjournal.TaskPrefix,
			ids.KindTask,
			request.NewestFirst(),
			DecodeTaskRecord,
			identity,
			func(record TaskRecord) bool { return visible(record) && record.Owner.ProjectID == scope.ID },
		)
		return repository.verifyTaskOwnerPage(ctx, page, err)
	case TaskListScopeEnvironment:
		if ids.Validate(ids.KindEnvironment, scope.ID) != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(
				errs.KindValidationFailed,
				"environment task scope is invalid",
			)
		}
		page, err := recordquery.ListVisibleIndex(
			ctx, repository.store, collection, "environment", scope.ID,
			taskjournal.TaskEnvironmentIndexPrefix+scope.ID+"/", taskjournal.TaskStorageKey, ids.KindTask,
			request.NewestFirst(), DecodeTaskRecord, identity,
			func(record TaskRecord) bool { return record.Owner.EnvironmentID == scope.ID },
			visible,
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
		ownerKeys, err := taskOwnerIndexKeys(item.Record.Owner, item.Record.ID)
		if err != nil {
			return etcdstore.Page[TaskRecord]{}, errs.New(errs.KindInternal, "task owner indexes are corrupt")
		}
		keys = append(keys, ownerKeys...)
		for range ownerKeys {
			expectedTaskIDs = append(expectedTaskIDs, item.Record.ID)
		}
	}
	indexes, err := recordquery.GetManyBatchedAtRevision(ctx, repository.store, keys, page.Revision)
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
	task, err := DecodeTaskRecord(taskResult.Values[0].Value)
	if err != nil {
		return TaskEventSnapshot{}, err
	}
	if task.ID != taskID {
		return TaskEventSnapshot{}, errs.New(errs.KindInternal, "task event snapshot has a mismatched task")
	}
	expiryRead, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{
			Keys:     []string{taskjournal.TaskDiagnosticExpiryKey(taskID)},
			Revision: taskResult.ReadRevision,
		},
	)
	if err != nil {
		return TaskEventSnapshot{}, err
	}
	if expiryRead == nil || len(expiryRead.Values) != 1 {
		return TaskEventSnapshot{}, recordcodec.CorruptRecord()
	}
	defer etcdstore.ClearValues(expiryRead.Values)
	if expiryRead.Values[0] != nil {
		expiry, err := taskjournal.DecodeDiagnosticExpiry(expiryRead.Values[0].Value)
		if err != nil || expiry.TaskID != task.ID || expiry.TaskRevision != taskResult.Values[0].ModRevision ||
			!taskjournal.IsTerminalTaskStatus(task.Status) || expiry.ThroughSequence != task.NextEventSequence-1 {
			return TaskEventSnapshot{}, recordcodec.CorruptRecord()
		}
		return TaskEventSnapshot{
			Task:                 task,
			Revision:             taskResult.ReadRevision,
			DiagnosticsExpiredAt: &expiry.ExpiredAt,
		}, nil
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
