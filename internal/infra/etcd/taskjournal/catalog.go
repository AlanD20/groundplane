package taskjournal

// TaskType is the closed durable task catalog. It is a persistence DTO rather
// than a controller model so infra remains independent of controller and core.
type TaskType string

const (
	TaskDeploy      TaskType = "deploy"
	TaskRollback    TaskType = "rollback"
	TaskBackup      TaskType = "backup"
	TaskBackupPrune TaskType = "backup_prune"
	TaskRestore     TaskType = "restore"
	TaskAttach      TaskType = "attach"
	TaskDetach      TaskType = "detach"
	TaskRun         TaskType = "run"
	TaskScript      TaskType = "script"
	TaskProvision   TaskType = "provision"
	TaskCreate      TaskType = "create"
	TaskUpdate      TaskType = "update"
	TaskRemove      TaskType = "remove"
	TaskStart       TaskType = "start"
	TaskStop        TaskType = "stop"
	TaskDestroy     TaskType = "destroy"
	TaskRotate      TaskType = "rotate"
)

// TaskExecutor is the immutable authority allowed to claim a Task. It is
// explicit durable input so execution placement is never inferred from type or
// target identity.
type TaskExecutor string

const (
	TaskExecutorAgent      TaskExecutor = "agent"
	TaskExecutorController TaskExecutor = "controller"
)

func ValidExecutor(executor TaskExecutor) bool {
	switch executor {
	case TaskExecutorAgent, TaskExecutorController:
		return true
	default:
		return false
	}
}

// TaskStatus is the durable state machine. Acknowledgement remains a streamed
// Agent event and is deliberately not a task status.
type TaskStatus string

const (
	TaskStatusPending   TaskStatus = "pending"
	TaskStatusRunning   TaskStatus = "running"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusAborted   TaskStatus = "aborted"
	TaskStatusTimedOut  TaskStatus = "timed_out"
)

// TaskEventState is the lifecycle of one step, not the Task lifecycle. A
// terminal step event never completes the Task; the Agent acknowledgement
// drives the separate TaskStatus transition after the full procedure ends.
type TaskEventState string

const (
	TaskEventStatePending   TaskEventState = "pending"
	TaskEventStateRunning   TaskEventState = "running"
	TaskEventStateCompleted TaskEventState = "completed"
	TaskEventStateFailed    TaskEventState = "failed"
	TaskEventStateAborted   TaskEventState = "aborted"
	TaskEventStateTimedOut  TaskEventState = "timed_out"
)

type TaskStepKind string

const (
	TaskStepOperation TaskStepKind = "operation"
	TaskStepScript    TaskStepKind = "script"
)

// TaskStepRecord is the immutable execution procedure stored with a Task.
// ID is stable within the task and participates in Agent event identity.
type TaskStepRecord struct {
	Kind       TaskStepKind `json:"kind"`
	ID         string       `json:"id"`
	ScriptID   string       `json:"script_id,omitempty"`
	ScriptSlug string       `json:"script_slug,omitempty"`
}

type TaskResultKind string

const (
	TaskResultCompose              TaskResultKind = "compose"
	TaskResultEnvironmentDirectory TaskResultKind = "environment_directory"
)

type TaskResultDiagnostic string

const (
	TaskResultDiagnosticNone                TaskResultDiagnostic = "none"
	TaskResultDiagnosticConfigRejected      TaskResultDiagnostic = "config_rejected"
	TaskResultDiagnosticComposeFailed       TaskResultDiagnostic = "compose_failed"
	TaskResultDiagnosticTimeoutBeforeEffect TaskResultDiagnostic = "timeout_before_effect"
)
