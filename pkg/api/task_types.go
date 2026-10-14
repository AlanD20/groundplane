package api

import (
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	"github.com/AlanD20/groundplane/internal/common/taskdescription"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"time"
)

// TaskStatus is the durable, persisted state — "acked" is a streamed
// event, not a terminal status. See api-cli.md, "Task states".
type TaskStatus string

const (
	TaskPending   TaskStatus = "pending"
	TaskRunning   TaskStatus = "running"
	TaskCompleted TaskStatus = "completed"
	TaskFailed    TaskStatus = "failed"
	TaskAborted   TaskStatus = "aborted"
	TaskTimedOut  TaskStatus = "timed_out"
)

type TaskWorkspaceType string

const (
	TaskWorkspacePlatform TaskWorkspaceType = "platform"
	TaskWorkspaceTenant   TaskWorkspaceType = "tenant"
)

type TaskActor string

const (
	TaskActorOperator TaskActor = "operator"
	TaskActorSystem   TaskActor = "system"
)

type Task struct {
	ID                     string               `json:"id"`
	OperationID            string               `json:"operation_id"`
	RetryOf                string               `json:"retry_of,omitempty"`
	PlanHash               string               `json:"plan_hash,omitempty"`
	Type                   string               `json:"type"                     enum:"deploy,rollback,backup,backup_prune,restore,attach,detach,run,script,provision,create,update,remove,start,stop,destroy,rotate,fetch,prepare,apply,check"`
	Target                 string               `json:"target"`
	TargetName             string               `json:"target_name,omitempty"`
	ResourceKind           string               `json:"resource_kind,omitempty"`
	Executor               string               `json:"executor" enum:"agent,controller,blueprint,software"`
	TimeoutSeconds         int64                `json:"timeout_seconds"`
	Failure                *TaskFailure         `json:"failure,omitempty"`
	Recovery               *TaskRecovery        `json:"recovery,omitempty"`
	Preparation            *TaskPreparation     `json:"preparation,omitempty"`
	AttemptHistory         *TaskAttemptHistory  `json:"attempt_history,omitempty"`
	InterruptedDetachRetry *TaskDetachRetry     `json:"interrupted_detach_retry,omitempty"`
	RecoveryRetryAvailable bool                 `json:"recovery_retry_available"`
	DiagnosticsExpiredAt   *time.Time           `json:"diagnostics_expired_at,omitempty"`
	ResultSummary          string               `json:"result_summary,omitempty"`
	Status                 TaskStatus           `json:"status"`
	ReconciliationRequired bool                 `json:"reconciliation_required"`
	WorkspaceType          TaskWorkspaceType    `json:"workspace_type"           enum:"platform,tenant"`
	TenantID               string               `json:"tenant_id,omitempty"`
	ProjectID              string               `json:"project_id,omitempty"`
	EnvironmentID          string               `json:"environment_id,omitempty"`
	Actor                  TaskActor            `json:"actor"                    enum:"operator,system"`
	CreatedAt              time.Time            `json:"created_at"`
	UpdatedAt              time.Time            `json:"updated_at"`
	StartedAt              *time.Time           `json:"started_at"`
	FinishedAt             *time.Time           `json:"finished_at"`
	Steps                  []TaskStep           `json:"steps,omitempty"`
	ImageFetch             *TaskImageFetch      `json:"image_fetch,omitempty"`
	Executions             *Page[TaskExecution] `json:"executions,omitempty"`
}

// TaskExecution is an internal Blueprint child's recorded work, inspected only
// through its visible parent. It is not an independently controllable Task.
type TaskExecution struct {
	ID                     string        `json:"id"`
	Type                   string        `json:"type"`
	Target                 string        `json:"target"`
	TargetName             string        `json:"target_name,omitempty"`
	ResourceKind           string        `json:"resource_kind,omitempty"`
	Status                 TaskStatus    `json:"status"`
	CreatedAt              time.Time     `json:"created_at"`
	UpdatedAt              time.Time     `json:"updated_at"`
	StartedAt              *time.Time    `json:"started_at"`
	FinishedAt             *time.Time    `json:"finished_at"`
	Steps                  []TaskStep    `json:"steps,omitempty"`
	Failure                *TaskFailure  `json:"failure,omitempty"`
	Recovery               *TaskRecovery `json:"recovery,omitempty"`
	ReconciliationRequired bool          `json:"reconciliation_required"`
}

// The existing Retry action resumes this captured hook, not the Blueprint.
type TaskDetachRetry struct {
	AttachID string `json:"attach_id"`
	StepID   string `json:"step_id"`
}

// TaskPreparation explains why a native release has not started its workload.
type TaskPreparation struct {
	State        string       `json:"state" enum:"network_queued,network_preparing,network_reconciling,network_proof_required,network_failed,dns_waiting,dns_failed,ready"`
	Summary      string       `json:"summary"`
	NetworkCount int          `json:"network_count" minimum:"1"`
	Deadline     time.Time    `json:"deadline"`
	Failure      *TaskFailure `json:"failure,omitempty"`
}

// TaskAttemptHistory is diagnostic history, not execution or recovery proof.
// ThroughSequence is the archive boundary, not the live resume cursor.
type TaskAttemptHistory struct {
	ThroughSequence  uint64               `json:"through_sequence"`
	DroppedSummaries uint64               `json:"dropped_summaries"`
	Summaries        []TaskAttemptSummary `json:"summaries"`
}

type TaskAttemptSummary struct {
	StepID              string                   `json:"step_id,omitempty"`
	Attempt             uint32                   `json:"attempt"`
	Ordinal             uint64                   `json:"ordinal"`
	Action              string                   `json:"action"`
	Target              string                   `json:"target,omitempty"`
	State               TaskStatus               `json:"state"`
	FirstSequence       uint64                   `json:"first_sequence"`
	LastSequence        uint64                   `json:"last_sequence"`
	StartedReceivedAt   *time.Time               `json:"started_received_at,omitempty"`
	FinishedReceivedAt  *time.Time               `json:"finished_received_at,omitempty"`
	LastReceivedAt      time.Time                `json:"last_received_at"`
	ControllerOperation *TaskControllerOperation `json:"controller_operation,omitempty"`
	Failure             *TaskFailure             `json:"failure,omitempty"`
}

// TaskFailure describes the original outcome, even while recovery is running.
// Empty diagnostics are unavailable evidence, not proof that nothing changed.
type TaskFailure struct {
	Status   string `json:"status" enum:"failed,timed_out,aborted"`
	StepID   string `json:"step_id,omitempty"`
	Code     string `json:"code,omitempty"`
	Summary  string `json:"summary"`
	ExitCode *int32 `json:"exit_code,omitempty"`
}

// TaskRecovery is returned by Task details from retained recovery authority at
// the Task's read revision. A completed step is not a verified recovery outcome.
type TaskRecovery struct {
	State             string       `json:"state" enum:"pending,running,awaiting_confirmation,proof_required,retry_wait,automatic_retries_stopped,completed"`
	Summary           string       `json:"summary"`
	StepIDs           []string     `json:"step_ids,omitempty"`
	CompletedSteps    *int32       `json:"completed_steps,omitempty" minimum:"0"`
	TotalSteps        *int32       `json:"total_steps,omitempty" minimum:"1"`
	Deadline          *time.Time   `json:"deadline,omitempty"`
	LastFailure       *TaskFailure `json:"last_failure,omitempty"`
	AutomaticFailures uint32       `json:"automatic_failures"`
	RetryAfter        *time.Time   `json:"retry_after,omitempty"`
	RetryAvailable    bool         `json:"retry_available"`
	StopReason        string       `json:"stop_reason,omitempty" enum:"non_transient_failure,retry_budget_exhausted,recovery_deadline,unsafe_replay"`
}

type TaskImageFetch struct {
	Requested string              `json:"requested"`
	Image     string              `json:"image"`
	Platform  string              `json:"platform"`
	Progress  imagefetch.Progress `json:"progress"`
}

type TaskStepKind string

const (
	TaskStepOperation TaskStepKind = "operation"
	TaskStepScript    TaskStepKind = "script"
)

type TaskStep struct {
	Failure              *TaskFailure                            `json:"failure,omitempty"`
	Name                 string                                  `json:"name"`
	Action               string                                  `json:"action"`
	Description          string                                  `json:"description,omitempty"`
	Target               string                                  `json:"target,omitempty"`
	TimeoutSeconds       uint32                                  `json:"timeout_seconds,omitempty"`
	Status               TaskStatus                              `json:"status"`
	Kind                 TaskStepKind                            `json:"kind" enum:"operation,script"`
	ScriptID             string                                  `json:"script_id,omitempty"`
	ScriptSlug           string                                  `json:"script_slug,omitempty"`
	HealthWait           *taskprogress.Readiness                 `json:"health_wait,omitempty"`
	HealthWaitReceivedAt *time.Time                              `json:"health_wait_received_at,omitempty"`
	Healthchecks         []taskdescription.ConfiguredHealthcheck `json:"healthchecks,omitempty"`
}

// TaskAccepted is the 202 body every action endpoint returns — the
// field is `task_id`, snake_case, matching every other JSON field name
// in this API (api-cli.md: "Snake_case everywhere").
type TaskAccepted struct {
	TaskID string `json:"task_id"`
}

// --- Pagination ---
