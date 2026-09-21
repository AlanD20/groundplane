package etcd

import (
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

const (
	MaximumTaskRecordBytes                 = 256 * 1024
	MaximumTaskEventBytes                  = 32 * 1024
	MaximumTaskEvents                      = 1000
	TaskRetention                          = 90 * 24 * time.Hour
	TaskMaterializationEnvironmentParam    = "materialization_environment_id"
	TaskMutationEnvironmentParam           = "mutation_environment_id"
	TaskBackingServiceCreationParam        = "backing_service_creation_service_id"
	TaskBackingServiceHealthParam          = "backing_service_health_service_id"
	TaskBackingServiceVolumeDirectoryParam = "backing_service_volume_directory"
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
	Params             map[string]string                         `json:"params,omitempty"`
	Steps              []taskjournal.TaskStepRecord              `json:"steps,omitempty"`
	Materializations   []materializationrecord.Record            `json:"materializations,omitempty"`
	EntryRuntime       *EntryTaskRuntime                         `json:"entry_runtime,omitempty"`
	Configuration      *TaskConfiguration                        `json:"configuration,omitempty"`
	TimeoutSeconds     int64                                     `json:"timeout_seconds"`
	Status             taskjournal.TaskStatus                    `json:"status"`
	Result             *taskjournal.TaskResultRecord             `json:"result,omitempty"`
	TerminalAssignment *taskjournal.TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	NextEventSequence  uint64                                    `json:"next_event_sequence"`
	EventCount         uint32                                    `json:"event_count"`
	EventCheckpoints   []TaskEventCheckpoint                     `json:"event_checkpoints,omitempty"`
	CreatedAt          time.Time                                 `json:"created_at"`
	UpdatedAt          time.Time                                 `json:"updated_at"`
	StartedAt          *time.Time                                `json:"started_at,omitempty"`
	FinishedAt         *time.Time                                `json:"finished_at,omitempty"`
	RetainUntil        *time.Time                                `json:"retain_until,omitempty"`
	idempotencyMarker  *idempotencyrecord.IdempotencyLocator

	ComponentActionStepIDs          []string                                         `json:"component_action_step_ids"`
	ManagedComponentTeardownSources []projectionrecord.ManagedComponentRuntimeSource `json:"managed_component_teardown_sources,omitempty"`
}

func newTaskRecord(
	id string,
	operationID string,
	owner taskjournal.TaskOwner,
	actor taskjournal.TaskActor,
	taskType taskjournal.TaskType,
	target string,
	timeoutSeconds int64,
	createdAt time.Time,
) TaskRecord {
	return TaskRecord{
		ID: id, OperationID: operationID, Owner: owner, Actor: actor,
		Executor: taskjournal.TaskExecutorAgent, Type: taskType, Target: target,
		TimeoutSeconds: timeoutSeconds, Status: taskjournal.TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

func transitionTaskStatus(
	record TaskRecord,
	expected taskjournal.TaskStatus,
	next taskjournal.TaskStatus,
	at time.Time,
) (TaskRecord, error) {
	if err := validateTaskRecord(record); err != nil {
		return TaskRecord{}, err
	}
	if record.Status != expected {
		return TaskRecord{}, errs.Newf(
			errs.KindStateConflict,
			"task %s status changed from %s to %s",
			record.ID,
			expected,
			record.Status,
		)
	}
	if expected == next || !validTaskTransition(expected, next) {
		return TaskRecord{}, errs.Newf(
			errs.KindStateConflict,
			"task %s cannot transition from %s to %s",
			record.ID,
			expected,
			next,
		)
	}
	if err := recordcodec.ValidateTimestamp("task transition", at); err != nil {
		return TaskRecord{}, err
	}

	at, err := nextTaskControllerTimestamp(record.UpdatedAt, at)
	if err != nil {
		return TaskRecord{}, err
	}
	replacement := cloneTaskRecord(record)
	replacement.Status = next
	replacement.UpdatedAt = at
	if next == taskjournal.TaskStatusRunning {
		replacement.StartedAt = timePointer(at)
	}
	if isTerminalTaskStatus(next) {
		replacement.FinishedAt = timePointer(at)
		retention := at.Add(TaskRetention)
		replacement.RetainUntil = &retention
	}
	if err := validateTaskRecord(replacement); err != nil {
		return TaskRecord{}, err
	}
	return replacement, nil
}

func validTaskTransition(current taskjournal.TaskStatus, next taskjournal.TaskStatus) bool {
	switch current {
	case taskjournal.TaskStatusPending:
		return next == taskjournal.TaskStatusRunning || next == taskjournal.TaskStatusAborted
	case taskjournal.TaskStatusRunning:
		return isTerminalTaskStatus(next)
	default:
		return false
	}
}

func isTerminalTaskStatus(status taskjournal.TaskStatus) bool {
	switch status {
	case taskjournal.TaskStatusCompleted, taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, taskjournal.TaskStatusTimedOut:
		return true
	default:
		return false
	}
}

func validTaskType(taskType taskjournal.TaskType) bool {
	switch taskType {
	case taskjournal.TaskDeploy, taskjournal.TaskRollback, taskjournal.TaskBackup, taskjournal.TaskBackupPrune, taskjournal.TaskRestore, taskjournal.TaskAttach, taskjournal.TaskDetach,
		taskjournal.TaskRun, taskjournal.TaskScript, taskjournal.TaskProvision, taskjournal.TaskCreate, taskjournal.TaskUpdate, taskjournal.TaskRemove,
		taskjournal.TaskStart, taskjournal.TaskStop, taskjournal.TaskDestroy, taskjournal.TaskRotate:
		return true
	default:
		return false
	}
}

func validTaskStatus(status taskjournal.TaskStatus) bool {
	switch status {
	case taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning, taskjournal.TaskStatusCompleted,
		taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, taskjournal.TaskStatusTimedOut:
		return true
	default:
		return false
	}
}
