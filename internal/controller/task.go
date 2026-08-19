// task.go is the ONE task pipeline: deploy, rollback, backup, restore,
// attach, run, script all run through it as parameterized step
// sequences. See mvp.md, "Baked-in actions become Tasks", and
// api-cli.md, "Retry = a new task with the same operation identity."
package controller

import (
	"context"
	"sync"
	"time"

	"github.com/sample-tenant/groundplane/internal/adapters"
	"github.com/sample-tenant/groundplane/internal/common/ids"
	"github.com/sample-tenant/groundplane/pkg/errs"
)

type TaskType string

const (
	TaskDeploy    TaskType = "deploy"
	TaskRollback  TaskType = "rollback"
	TaskBackup    TaskType = "backup"
	TaskRestore   TaskType = "restore"
	TaskAttach    TaskType = "attach"
	TaskDetach    TaskType = "detach"
	TaskRun       TaskType = "run"
	TaskScript    TaskType = "script"
	TaskProvision TaskType = "provision"
	TaskCreate    TaskType = "create"
	TaskStart     TaskType = "start"
	TaskStop      TaskType = "stop"
	TaskDestroy   TaskType = "destroy"
	TaskRotate    TaskType = "rotate"
)

// TaskStatus is the durable, persisted state. `acked` is deliberately
// NOT one of these: it's a streamed TaskAck channel event (see
// proto/agent.proto), not a terminal status — api-cli.md, "Task states":
// "pending, running, then exactly one of completed, failed, timed_out,
// or aborted. acked is a streamed acknowledgement event, not a durable
// terminal state."
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"
	StatusRunning   TaskStatus = "running"
	StatusCompleted TaskStatus = "completed"
	StatusFailed    TaskStatus = "failed"
	StatusAborted   TaskStatus = "aborted"
	StatusTimedOut  TaskStatus = "timed_out"
)

// Task is the etcd-stored contract. OperationID is stable across
// retries; RetryOf links a retry task back to the attempt it replaces;
// PlanHash ties the task to the ExecutionPlan (plan.go) it dispatches —
// the Agent rejects a bundle whose two projections of the plan don't
// match. See blueprint.md, "Tasks have immutable attempt ids and a
// stable operation identity".
type Task struct {
	ID          string            `json:"id"`           // task_<ulid> — THIS execution attempt
	OperationID string            `json:"operation_id"` // op_<ulid> — same across retries
	RetryOf     string            `json:"retry_of,omitempty"`
	PlanHash    string            `json:"plan_hash,omitempty"`
	Type        TaskType          `json:"type"`
	Target      string            `json:"target"` // e.g. a service id
	Params      map[string]string `json:"params,omitempty"`
	Steps       []adapters.Step   `json:"steps"`
	Timeout     time.Duration     `json:"timeout"`
	Status      TaskStatus        `json:"status"`
	Created     time.Time         `json:"created"`
}

// Dispatcher owns the Controller-side serialization lock: it refuses a
// second Deploy/Rollback for a service that already has one in-flight.
// A new deploy is allowed only after the previous one is aborted or
// completes. See mvp.md, "Baked-in actions".
type Dispatcher struct {
	mu       sync.Mutex
	inFlight map[string]string // service id -> task id, deploy/rollback only
	tasks    map[string]*Task  // task id -> task, for Retry's lookup (TODO: etcd-backed, not in-memory)
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{inFlight: map[string]string{}, tasks: map[string]*Task{}}
}

// Dispatch creates and stores a new Task under a FRESH OperationID. For
// TaskDeploy/TaskRollback it enforces the serialization lock and
// returns CodeDeployInFlight if one is already running for target.
//
// TODO: persist to etcd (task queue), and make it pullable by the Agent
// over the gRPC channel (see proto/agent.proto's TaskAssignment).
func (d *Dispatcher) Dispatch(ctx context.Context, t TaskType, target string, params map[string]string, steps []adapters.Step, timeout time.Duration) (*Task, error) {
	return d.dispatch(t, target, params, steps, timeout, ids.New(ids.KindOperation), "")
}

// Retry creates a NEW task record with the SAME operation identity,
// target, and parameters as taskID's original dispatch, linked via
// RetryOf — never a task-replay endpoint (api-cli.md: "Retry = a new
// task with the same operation identity"). Safety comes from the
// Agent's reconcile-based idempotency (checkpoints + labeled-container
// state), not from this method.
func (d *Dispatcher) Retry(ctx context.Context, taskID string) (*Task, error) {
	d.mu.Lock()
	original, ok := d.tasks[taskID]
	d.mu.Unlock()
	if !ok {
		return nil, errs.Newf(errs.CodeTaskNotFound, "task %s not found", taskID)
	}
	return d.dispatch(original.Type, original.Target, original.Params, original.Steps, original.Timeout, original.OperationID, taskID)
}

func (d *Dispatcher) dispatch(t TaskType, target string, params map[string]string, steps []adapters.Step, timeout time.Duration, operationID, retryOf string) (*Task, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	if t == TaskDeploy || t == TaskRollback {
		if existing, ok := d.inFlight[target]; ok {
			return nil, errs.Newf(errs.CodeDeployInFlight,
				"a deploy or rollback (task %s) is already in flight for %s", existing, target)
		}
	}

	task := &Task{
		ID:          ids.New(ids.KindTask),
		OperationID: operationID,
		RetryOf:     retryOf,
		Type:        t,
		Target:      target,
		Params:      params,
		Steps:       steps,
		Timeout:     timeout,
		Status:      StatusPending,
		Created:     time.Now(),
	}

	if t == TaskDeploy || t == TaskRollback {
		d.inFlight[target] = task.ID
	}
	d.tasks[task.ID] = task

	// TODO: etcd.Store.Put(ctx, "/tasks/"+task.ID, marshal(task))
	return task, nil
}

// Release clears the serialization lock — called on task completion,
// failure, or abort (see proto/agent.proto's TaskAbort semantics).
func (d *Dispatcher) Release(target string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inFlight, target)
}
