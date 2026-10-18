package api

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
)

// TaskSummary describes the operator action, not an executor or hidden child Task.
type TaskSummary struct {
	ResourceKind   string                 `json:"resource_kind"`
	ID             string                 `json:"id"`
	Action         workexecution.Action   `json:"action"`
	TargetID       string                 `json:"target_id"`
	TargetName     string                 `json:"target_name"`
	Scope          workexecution.Scope    `json:"scope"`
	Actor          workexecution.Actor    `json:"actor" enum:"operator,system"`
	Status         string                 `json:"status" enum:"queued,preparing,waiting,running,attention_required,stopping,finishing,succeeded,failed,cancelled,superseded"`
	Counts         workexecution.Progress `json:"counts"`
	CreatedAt      time.Time              `json:"created_at"`
	UpdatedAt      time.Time              `json:"updated_at"`
	Deadline       time.Time              `json:"deadline"`
	FinishedAt     *time.Time             `json:"finished_at,omitempty"`
	HistoryExpired bool                   `json:"history_expired"`
	StopRequested  bool                   `json:"stop_requested"`
	Cancel         TaskActionAvailability `json:"cancel"`
}

type TaskActionAvailability struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason,omitempty"`
}

type TaskWork struct {
	Preparation     *TaskWorkPreparation    `json:"preparation,omitempty"`
	Retry           TaskActionAvailability  `json:"retry"`
	ID              string                  `json:"id"`
	TargetID        string                  `json:"target_id"`
	Operation       workexecution.Operation `json:"operation"`
	Placement       workexecution.Placement `json:"placement"`
	Phase           workexecution.Phase     `json:"phase" enum:"waiting,ready,assigned,running,attention,settled"`
	Outcome         workexecution.Outcome   `json:"outcome,omitempty"`
	Wait            *workexecution.Wait     `json:"wait,omitempty"`
	Cleanup         workexecution.Cleanup   `json:"cleanup"`
	AttemptCount    uint64                  `json:"attempt_count"`
	ActiveAttemptID string                  `json:"active_attempt_id,omitempty"`
	CreatedAt       time.Time               `json:"created_at"`
	UpdatedAt       time.Time               `json:"updated_at"`
	Deadline        time.Time               `json:"deadline"`
}

// TaskWorkPreparation describes work that ended before executable input existed.
// It is distinct from an execution attempt or a failed host effect.
type TaskWorkPreparation struct {
	Summary      string    `json:"summary"`
	DependencyID string    `json:"dependency_id,omitempty"`
	FinishedAt   time.Time `json:"finished_at"`
}

// ExecutionPage fixes the storage revision across pages. After is an exclusive
// identity cursor, not an offset; clients preserve Revision until refreshing.
type ExecutionPage[T any] struct {
	ScanLimited bool   `json:"scan_limited" doc:"More history remains but this request reached its bounded scan budget; continue with after and revision"`
	Items       []T    `json:"items" nullable:"false"`
	More        bool   `json:"more"`
	After       string `json:"after,omitempty"`
	Revision    int64  `json:"revision"`
}

type TaskDetails struct {
	Preparation *TaskPreparation        `json:"preparation,omitempty"`
	Task        TaskSummary             `json:"task"`
	Work        ExecutionPage[TaskWork] `json:"work"`
}

type TaskPreparation struct {
	Reason     string                `json:"reason" enum:"planning_deadline_exceeded,planning_stopped,planning_invalid_input,planning_dependency_conflict"`
	Outcome    workexecution.Outcome `json:"outcome" enum:"failed,cancelled,superseded"`
	Summary    string                `json:"summary"`
	NextAction string                `json:"next_action"`
	FinishedAt time.Time             `json:"finished_at"`
}

type TaskAttempt struct {
	ID         string                  `json:"id"`
	Number     uint64                  `json:"number"`
	Executor   workexecution.Executor  `json:"executor" enum:"agent,controller"`
	HostID     string                  `json:"host_id,omitempty"`
	AgentID    string                  `json:"agent_id,omitempty"`
	Generation uint64                  `json:"generation"`
	Direction  workexecution.Direction `json:"direction" enum:"forward,recovery"`
	Outcome    workexecution.Outcome   `json:"outcome,omitempty"`
	AssignedAt time.Time               `json:"assigned_at"`
	Deadline   time.Time               `json:"deadline"`
	StartedAt  *time.Time              `json:"started_at,omitempty"`
	FinishedAt *time.Time              `json:"finished_at,omitempty"`
}

type TaskAttemptPage struct {
	Items    []TaskAttempt `json:"items" nullable:"false"`
	More     bool          `json:"more"`
	After    uint64        `json:"after"`
	Revision int64         `json:"revision"`
}

type TaskStages struct {
	Incomplete bool                  `json:"incomplete"`
	Items      []workexecution.Stage `json:"items" nullable:"false"`
	Available  bool                  `json:"available" doc:"False means no stage diagnostics are retained; it does not prove no effects occurred"`
	Sequence   uint64                `json:"sequence"`
	UpdatedAt  *time.Time            `json:"updated_at,omitempty"`
	Revision   int64                 `json:"revision"`
}
