package etcd

import (
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
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

func validTaskExecutor(executor TaskExecutor) bool {
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

type TaskObservedProjectSummary struct {
	ProjectName    string    `json:"project_name"`
	ObservedAt     time.Time `json:"observed_at"`
	ContainerCount uint32    `json:"container_count"`
	NetworkCount   uint32    `json:"network_count"`
	VolumeCount    uint32    `json:"volume_count"`
	CollisionCount uint32    `json:"collision_count"`
}

type TaskProxyEvidence struct {
	ServiceID       string `json:"service_id"`
	Target          string `json:"target"`
	ProxyGeneration uint64 `json:"proxy_generation"`
	ConfigSHA256    string `json:"config_sha256"`
	ReleaseID       string `json:"release_id"`
	Compensated     bool   `json:"compensated"`
}

type TaskRecreateEvidence struct {
	ServiceID   string `json:"service_id"`
	ReleaseID   string `json:"release_id"`
	ArtifactID  string `json:"artifact_id"`
	Compensated bool   `json:"compensated"`
	Target      string `json:"target"`
}

type TaskCandidateAbsenceCandidate struct {
	ServiceID string `json:"service_id"`
	ReleaseID string `json:"release_id"`
}

type TaskCandidateAbsenceEvidence struct {
	AssignmentID        string                          `json:"assignment_id"`
	PlanHash            string                          `json:"plan_hash"`
	AuthoritySHA256     string                          `json:"authority_sha256"`
	ComposeProjectName  string                          `json:"compose_project_name"`
	CandidateArtifactID string                          `json:"candidate_artifact_id"`
	Candidates          []TaskCandidateAbsenceCandidate `json:"candidates"`
	AbsenceProven       bool                            `json:"absence_proven"`
}

type TaskResultRecord struct {
	Kind                            TaskResultKind                      `json:"kind"`
	ExitCode                        int32                               `json:"exit_code"`
	FailedStepID                    string                              `json:"failed_step_id,omitempty"`
	Diagnostic                      TaskResultDiagnostic                `json:"diagnostic"`
	ReconciliationRequired          bool                                `json:"reconciliation_required"`
	Projects                        []TaskObservedProjectSummary        `json:"projects,omitempty"`
	ProxyEvidence                   []TaskProxyEvidence                 `json:"proxy_evidence,omitempty"`
	RecreateEvidence                []TaskRecreateEvidence              `json:"recreate_evidence,omitempty"`
	CandidateAbsenceEvidence        *TaskCandidateAbsenceEvidence       `json:"candidate_absence_evidence,omitempty"`
	DNSResolverCandidateObservation *TaskDNSResolverObservationEvidence `json:"dns_resolver_candidate_observation,omitempty"`
	DNSResolverRollbackObservation  *TaskDNSResolverObservationEvidence `json:"dns_resolver_rollback_observation,omitempty"`
	ExecutionEpoch                  uint32                              `json:"-"`
	ReleaseRecoveryRecordSHA256     string                              `json:"-"`
}

type TaskTerminalAssignmentRecord struct {
	AssignmentID    string `json:"assignment_id"`
	AgentID         string `json:"agent_id"`
	AgentGeneration uint64 `json:"agent_generation"`
}

// TaskRecord is the versioned persistence DTO for one execution attempt.
// NextEventSequence starts at one. FinishedAt is the retention epoch; the
// pruning scheduler can delete the task, events, and dedupe records together
// after RetainUntil without deriving time from a ULID.
type TaskRecord struct {
	ID                 string                         `json:"id"`
	OperationID        string                         `json:"operation_id"`
	RetryOf            string                         `json:"retry_of,omitempty"`
	IdempotencyKey     string                         `json:"idempotency_key,omitempty"`
	Owner              TaskOwner                      `json:"owner"`
	Actor              TaskActor                      `json:"actor"`
	Executor           TaskExecutor                   `json:"executor"`
	PlanID             string                         `json:"plan_id"`
	PlanHash           string                         `json:"plan_hash,omitempty"`
	RenderGeneration   int32                          `json:"render_generation"`
	Type               TaskType                       `json:"type"`
	Target             string                         `json:"target"`
	Params             map[string]string              `json:"params,omitempty"`
	Steps              []TaskStepRecord               `json:"steps,omitempty"`
	Materializations   []materializationrecord.Record `json:"materializations,omitempty"`
	EntryRuntime       *EntryTaskRuntime              `json:"entry_runtime,omitempty"`
	Configuration      *TaskConfiguration             `json:"configuration,omitempty"`
	TimeoutSeconds     int64                          `json:"timeout_seconds"`
	Status             TaskStatus                     `json:"status"`
	Result             *TaskResultRecord              `json:"result,omitempty"`
	TerminalAssignment *TaskTerminalAssignmentRecord  `json:"terminal_assignment,omitempty"`
	NextEventSequence  uint64                         `json:"next_event_sequence"`
	EventCount         uint32                         `json:"event_count"`
	EventCheckpoints   []TaskEventCheckpoint          `json:"event_checkpoints,omitempty"`
	CreatedAt          time.Time                      `json:"created_at"`
	UpdatedAt          time.Time                      `json:"updated_at"`
	StartedAt          *time.Time                     `json:"started_at,omitempty"`
	FinishedAt         *time.Time                     `json:"finished_at,omitempty"`
	RetainUntil        *time.Time                     `json:"retain_until,omitempty"`
	idempotencyMarker  *idempotencyrecord.IdempotencyLocator

	ComponentActionStepIDs          []string                                         `json:"component_action_step_ids"`
	ManagedComponentTeardownSources []projectionrecord.ManagedComponentRuntimeSource `json:"managed_component_teardown_sources,omitempty"`
}

func newTaskRecord(
	id string,
	operationID string,
	owner TaskOwner,
	actor TaskActor,
	taskType TaskType,
	target string,
	timeoutSeconds int64,
	createdAt time.Time,
) TaskRecord {
	return TaskRecord{
		ID: id, OperationID: operationID, Owner: owner, Actor: actor,
		Executor: TaskExecutorAgent, Type: taskType, Target: target,
		TimeoutSeconds: timeoutSeconds, Status: TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt, UpdatedAt: createdAt,
	}
}

func transitionTaskStatus(
	record TaskRecord,
	expected TaskStatus,
	next TaskStatus,
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
	if next == TaskStatusRunning {
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

func validTaskTransition(current TaskStatus, next TaskStatus) bool {
	switch current {
	case TaskStatusPending:
		return next == TaskStatusRunning || next == TaskStatusAborted
	case TaskStatusRunning:
		return isTerminalTaskStatus(next)
	default:
		return false
	}
}

func isTerminalTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusCompleted, TaskStatusFailed, TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}

func validTaskType(taskType TaskType) bool {
	switch taskType {
	case TaskDeploy, TaskRollback, TaskBackup, TaskBackupPrune, TaskRestore, TaskAttach, TaskDetach,
		TaskRun, TaskScript, TaskProvision, TaskCreate, TaskUpdate, TaskRemove,
		TaskStart, TaskStop, TaskDestroy, TaskRotate:
		return true
	default:
		return false
	}
}

func validTaskStatus(status TaskStatus) bool {
	switch status {
	case TaskStatusPending, TaskStatusRunning, TaskStatusCompleted,
		TaskStatusFailed, TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}
