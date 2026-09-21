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
	Kind                            taskjournal.TaskResultKind          `json:"kind"`
	ExitCode                        int32                               `json:"exit_code"`
	FailedStepID                    string                              `json:"failed_step_id,omitempty"`
	Diagnostic                      taskjournal.TaskResultDiagnostic    `json:"diagnostic"`
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
	Owner              taskjournal.TaskOwner          `json:"owner"`
	Actor              taskjournal.TaskActor          `json:"actor"`
	Executor           taskjournal.TaskExecutor       `json:"executor"`
	PlanID             string                         `json:"plan_id"`
	PlanHash           string                         `json:"plan_hash,omitempty"`
	RenderGeneration   int32                          `json:"render_generation"`
	Type               taskjournal.TaskType           `json:"type"`
	Target             string                         `json:"target"`
	Params             map[string]string              `json:"params,omitempty"`
	Steps              []taskjournal.TaskStepRecord   `json:"steps,omitempty"`
	Materializations   []materializationrecord.Record `json:"materializations,omitempty"`
	EntryRuntime       *EntryTaskRuntime              `json:"entry_runtime,omitempty"`
	Configuration      *TaskConfiguration             `json:"configuration,omitempty"`
	TimeoutSeconds     int64                          `json:"timeout_seconds"`
	Status             taskjournal.TaskStatus         `json:"status"`
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
	case TaskStatusPending:
		return next == taskjournal.TaskStatusRunning || next == taskjournal.TaskStatusAborted
	case TaskStatusRunning:
		return isTerminalTaskStatus(next)
	default:
		return false
	}
}

func isTerminalTaskStatus(status taskjournal.TaskStatus) bool {
	switch status {
	case taskjournal.TaskStatusCompleted, taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}

func validTaskType(taskType taskjournal.TaskType) bool {
	switch taskType {
	case taskjournal.TaskDeploy, taskjournal.TaskRollback, taskjournal.TaskBackup, taskjournal.TaskBackupPrune, taskjournal.TaskRestore, taskjournal.TaskAttach, taskjournal.TaskDetach,
		taskjournal.TaskRun, taskjournal.TaskScript, taskjournal.TaskProvision, taskjournal.TaskCreate, taskjournal.TaskUpdate, taskjournal.TaskRemove,
		taskjournal.TaskStart, taskjournal.TaskStop, taskjournal.TaskDestroy, TaskRotate:
		return true
	default:
		return false
	}
}

func validTaskStatus(status taskjournal.TaskStatus) bool {
	switch status {
	case taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning, taskjournal.TaskStatusCompleted,
		taskjournal.TaskStatusFailed, taskjournal.TaskStatusAborted, TaskStatusTimedOut:
		return true
	default:
		return false
	}
}
