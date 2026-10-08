package api

import (
	"github.com/AlanD20/groundplane/internal/common/imagefetch"
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
	ID                     string            `json:"id"`
	OperationID            string            `json:"operation_id"`
	RetryOf                string            `json:"retry_of,omitempty"`
	PlanHash               string            `json:"plan_hash,omitempty"`
	Type                   string            `json:"type"                     enum:"deploy,rollback,backup,backup_prune,restore,attach,detach,run,script,provision,create,update,remove,start,stop,destroy,rotate,fetch,prepare,apply"`
	Target                 string            `json:"target"`
	TargetName             string            `json:"target_name,omitempty"`
	ResourceKind           string            `json:"resource_kind,omitempty"`
	Executor               string            `json:"executor" enum:"agent,controller,blueprint"`
	TimeoutSeconds         int64             `json:"timeout_seconds"`
	FailureSummary         string            `json:"failure_summary,omitempty"`
	ResultSummary          string            `json:"result_summary,omitempty"`
	Status                 TaskStatus        `json:"status"`
	ReconciliationRequired bool              `json:"reconciliation_required"`
	WorkspaceType          TaskWorkspaceType `json:"workspace_type"           enum:"platform,tenant"`
	TenantID               string            `json:"tenant_id,omitempty"`
	ProjectID              string            `json:"project_id,omitempty"`
	EnvironmentID          string            `json:"environment_id,omitempty"`
	Actor                  TaskActor         `json:"actor"                    enum:"operator,system"`
	CreatedAt              time.Time         `json:"created_at"`
	UpdatedAt              time.Time         `json:"updated_at"`
	StartedAt              *time.Time        `json:"started_at"`
	FinishedAt             *time.Time        `json:"finished_at"`
	Steps                  []TaskStep        `json:"steps,omitempty"`
	ImageFetch             *TaskImageFetch   `json:"image_fetch,omitempty"`
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
	Name           string       `json:"name"`
	Action         string       `json:"action"`
	Description    string       `json:"description,omitempty"`
	Target         string       `json:"target,omitempty"`
	TimeoutSeconds uint32       `json:"timeout_seconds,omitempty"`
	Status         TaskStatus   `json:"status"`
	Kind           TaskStepKind `json:"kind" enum:"operation,script"`
	ScriptID       string       `json:"script_id,omitempty"`
	ScriptSlug     string       `json:"script_slug,omitempty"`
}

// TaskAccepted is the 202 body every action endpoint returns — the
// field is `task_id`, snake_case, matching every other JSON field name
// in this API (api-cli.md: "Snake_case everywhere").
type TaskAccepted struct {
	TaskID string `json:"task_id"`
}

// --- Pagination ---
