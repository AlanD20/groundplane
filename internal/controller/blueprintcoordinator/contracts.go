package blueprintcoordinator

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Planner derives the current claimed parent's complete durable unit view from
// its exact immutable desired revision. Complete remains false while a unit in
// this Apply must produce facts before the plan can be sealed.
type Planner interface {
	Plan(context.Context, etcd.TaskRecord, blueprintunits.Snapshot) (blueprintunits.DesiredPlan, error)
}

// EntrySettler publishes one Controller-owned Entry value and its applied
// receipt atomically. It is separate from Agent child preparation.
type EntrySettler interface {
	SettleEntry(context.Context, etcd.TaskRecord, blueprintunits.Unit, blueprintunits.Snapshot) error
}

// DormantServiceSettler acknowledges desired configuration when a Service's
// separate runtime intent forbids a host Release.
type DormantServiceSettler interface {
	TrySettleDormantService(context.Context, etcd.TaskRecord, blueprintunits.Unit) (bool, error)
}

// ChildPreparer prepares one exact ready unit as a private Agent Task. It must
// use the parent's immutable desired revision rather than the current head as
// a substitute for that revision.
type ChildPreparer interface {
	Prepare(context.Context, etcd.TaskRecord, blueprintunits.Unit) (PreparedChild, error)
}

type PreparedChild struct {
	Task               etcd.TaskRecord
	ReleasePublication etcd.BlueprintReleasePublication
}

// Clear releases prepared Release bytes after publication.
func (prepared *PreparedChild) Clear() {
	if prepared == nil {
		return
	}
	prepared.ReleasePublication.Clear()
}

type taskStore interface {
	ListBlueprintParentClaims(context.Context) ([]keyvalue.Versioned[etcd.TaskRecord], error)
	ClaimNextBlueprintParent(context.Context, time.Time) (keyvalue.Versioned[etcd.TaskRecord], bool, error)
	PublishBlueprintDesiredPlan(
		context.Context,
		string,
		blueprintunits.DesiredPlan,
	) (keyvalue.Versioned[blueprintunits.DesiredPlan], error)
	PublishBlueprintChild(
		context.Context,
		string,
		etcd.TaskRecord,
		blueprintunits.Unit,
		etcd.BlueprintReleasePublication,
	) (keyvalue.Versioned[etcd.TaskRecord], error)
	AbortPendingTask(context.Context, string, time.Time) (keyvalue.Versioned[etcd.TaskRecord], error)
	GetTask(context.Context, string) (keyvalue.Versioned[etcd.TaskRecord], error)
	GetTaskAssignment(context.Context, string) (etcd.TaskAssignment, error)
	RequestBlueprintParentAbort(context.Context, string, time.Time) error
	BlueprintParentFailureRequested(context.Context, string) (bool, error)
	BlueprintParentAbortRequested(context.Context, string) (bool, error)
	FailBlueprintParent(
		context.Context,
		string,
		string,
		time.Time,
	) (keyvalue.Versioned[etcd.TaskRecord], error)
	AbortBlueprintParent(
		context.Context,
		string,
		string,
		time.Time,
	) (keyvalue.Versioned[etcd.TaskRecord], error)
	CompleteBlueprintParent(
		context.Context,
		string,
		string,
		time.Time,
	) (keyvalue.Versioned[etcd.TaskRecord], error)
	RetireSupersededBlueprintParent(
		context.Context,
		string,
		string,
		time.Time,
	) (keyvalue.Versioned[etcd.TaskRecord], error)
}

type ledger interface {
	Load(context.Context, string) (blueprintunits.Snapshot, error)
}

type agentChannel interface {
	TaskTerminal(context.Context, string, uint64, string, string) (<-chan error, error)
	AbortTask(context.Context, string, uint64, string, string, string) error
	WakeTaskDispatch()
}

type Runner struct {
	tasks    taskStore
	ledger   ledger
	planner  Planner
	children ChildPreparer
	agents   agentChannel
	interval time.Duration
	logger   *slog.Logger
	now      func() time.Time
	wake     chan struct{}
}

func New(
	tasks taskStore,
	ledger ledger,
	planner Planner,
	children ChildPreparer,
	agents agentChannel,
	interval time.Duration,
	logger *slog.Logger,
) (*Runner, error) {
	if tasks == nil || ledger == nil || planner == nil || children == nil || agents == nil ||
		interval <= 0 || logger == nil {
		return nil, errs.New(errs.KindInternal, "blueprint coordinator dependencies are invalid")
	}
	return &Runner{
		tasks: tasks, ledger: ledger, planner: planner, children: children, agents: agents,
		interval: interval, logger: logger, now: time.Now, wake: make(chan struct{}, 1),
	}, nil
}

// Wake requests an immediate durable scan. The interval remains the recovery
// path when publication and this process do not share an in-memory signal.
func (runner *Runner) Wake() {
	if runner == nil || runner.wake == nil {
		return
	}
	select {
	case runner.wake <- struct{}{}:
	default:
	}
}

func validParent(task etcd.TaskRecord) bool {
	return etcd.ValidateTaskRecord(task) == nil && task.Executor == taskjournal.TaskExecutorBlueprint &&
		task.Status == taskjournal.TaskStatusRunning
}
