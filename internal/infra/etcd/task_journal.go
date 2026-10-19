package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/imagefetch"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

// TaskRecord is the versioned persistence DTO for one execution attempt.
// NextEventSequence starts at one. FinishedAt is the retention epoch; the
// pruning scheduler can delete the task, events, and dedupe records together
// after RetainUntil without deriving time from a ULID.
type TaskRecord struct {
	ID                 string                                    `json:"id"`
	OperationID        string                                    `json:"operation_id"`
	RetryOf            string                                    `json:"retry_of,omitempty"`
	IdempotencyKey     string                                    `json:"idempotency_key,omitempty"`
	Owner              taskjournal.TaskOwner                     `json:"owner"`
	Actor              taskjournal.TaskActor                     `json:"actor"`
	Executor           taskjournal.TaskExecutor                  `json:"executor"`
	PlanID             string                                    `json:"plan_id"`
	PlanHash           string                                    `json:"plan_hash,omitempty"`
	RenderGeneration   int32                                     `json:"render_generation"`
	Type               taskjournal.TaskType                      `json:"type"`
	Target             string                                    `json:"target"`
	TargetName         string                                    `json:"target_name,omitempty"`
	Params             map[string]string                         `json:"params,omitempty"`
	Steps              []taskjournal.TaskStepRecord              `json:"steps,omitempty"`
	Materializations   []materializationrecord.Record            `json:"materializations,omitempty"`
	Configuration      *taskconfiguration.TaskConfiguration      `json:"configuration,omitempty"`
	TimeoutSeconds     int64                                     `json:"timeout_seconds"`
	Status             taskjournal.TaskStatus                    `json:"status"`
	ImageFetchProgress imagefetch.Progress                       `json:"image_fetch_progress,omitzero"`
	Result             *taskjournal.TaskResultRecord             `json:"result,omitempty"`
	BlueprintFailure   *taskjournal.BlueprintFailure             `json:"blueprint_failure,omitempty"`
	TerminalAssignment *taskjournal.TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	NextEventSequence  uint64                                    `json:"next_event_sequence"`
	EventCount         uint32                                    `json:"event_count"`
	EventCheckpoints   []taskjournal.TaskEventCheckpoint         `json:"event_checkpoints,omitempty"`
	CreatedAt          time.Time                                 `json:"created_at"`
	UpdatedAt          time.Time                                 `json:"updated_at"`
	StartedAt          *time.Time                                `json:"started_at,omitempty"`
	FinishedAt         *time.Time                                `json:"finished_at,omitempty"`
	RetainUntil        *time.Time                                `json:"retain_until,omitempty"`
	idempotencyMarker  *idempotencyrecord.IdempotencyLocator

	ComponentActionStepIDs          []string                                         `json:"component_action_step_ids"`
	ManagedComponentTeardownSources []projectionrecord.ManagedComponentRuntimeSource `json:"managed_component_teardown_sources,omitempty"`
}
