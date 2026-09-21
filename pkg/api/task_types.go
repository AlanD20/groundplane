package api

import (
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
	ID            string            `json:"id"`
	OperationID   string            `json:"operation_id"`
	RetryOf       string            `json:"retry_of,omitempty"`
	PlanHash      string            `json:"plan_hash,omitempty"`
	Type          string            `json:"type"                     enum:"deploy,rollback,backup,backup_prune,restore,attach,detach,run,script,provision,create,update,remove,start,stop,destroy,rotate"`
	Target        string            `json:"target"`
	Status        TaskStatus        `json:"status"`
	WorkspaceType TaskWorkspaceType `json:"workspace_type"           enum:"platform,tenant"`
	TenantID      string            `json:"tenant_id,omitempty"`
	ProjectID     string            `json:"project_id,omitempty"`
	EnvironmentID string            `json:"environment_id,omitempty"`
	Actor         TaskActor         `json:"actor"                    enum:"operator,system"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	StartedAt     *time.Time        `json:"started_at"`
	FinishedAt    *time.Time        `json:"finished_at"`
	Steps         []TaskStep        `json:"steps,omitempty"`
}

type TaskStepKind string

const (
	TaskStepOperation TaskStepKind = "operation"
	TaskStepScript    TaskStepKind = "script"
)

type TaskStep struct {
	Name       string       `json:"name"`
	Status     TaskStatus   `json:"status"`
	Kind       TaskStepKind `json:"kind" enum:"operation,script"`
	ScriptID   string       `json:"script_id,omitempty"`
	ScriptSlug string       `json:"script_slug,omitempty"`
}

// TaskAccepted is the 202 body every action endpoint returns — the
// field is `task_id`, snake_case, matching every other JSON field name
// in this API (api-cli.md: "Snake_case everywhere").
type TaskAccepted struct {
	TaskID string `json:"task_id"`
}

// --- Pagination ---
