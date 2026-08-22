package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const (
	MaximumTaskRecordBytes              = 256 * 1024
	MaximumTaskEventBytes               = 32 * 1024
	MaximumTaskEvents                   = 1000
	TaskRetention                       = 90 * 24 * time.Hour
	TaskMaterializationEnvironmentParam = "materialization_environment_id"
	TaskMutationEnvironmentParam        = "mutation_environment_id"
)

// TaskType is the closed durable task catalog. It is a persistence DTO rather
// than a controller model so infra remains independent of controller and core.
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
	TaskUpdate    TaskType = "update"
	TaskRemove    TaskType = "remove"
	TaskStart     TaskType = "start"
	TaskStop      TaskType = "stop"
	TaskDestroy   TaskType = "destroy"
	TaskRotate    TaskType = "rotate"
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

// TaskStepRecord is the immutable execution procedure stored with a Task.
// ID is stable within the task and participates in Agent event identity.
type TaskStepRecord struct {
	ID string `json:"id"`
}

type TaskResultKind string

const (
	TaskResultCompose              TaskResultKind = "compose"
	TaskResultEnvironmentDirectory TaskResultKind = "environment_directory"
)

type TaskResultDiagnostic string

const (
	TaskResultDiagnosticNone           TaskResultDiagnostic = "none"
	TaskResultDiagnosticConfigRejected TaskResultDiagnostic = "config_rejected"
	TaskResultDiagnosticComposeFailed  TaskResultDiagnostic = "compose_failed"
)

type TaskObservedProjectSummary struct {
	ProjectName    string    `json:"project_name"`
	ObservedAt     time.Time `json:"observed_at"`
	ContainerCount uint32    `json:"container_count"`
	NetworkCount   uint32    `json:"network_count"`
	VolumeCount    uint32    `json:"volume_count"`
	CollisionCount uint32    `json:"collision_count"`
}

type TaskResultRecord struct {
	Kind                   TaskResultKind               `json:"kind"`
	ExitCode               int32                        `json:"exit_code"`
	FailedStepID           string                       `json:"failed_step_id,omitempty"`
	Diagnostic             TaskResultDiagnostic         `json:"diagnostic"`
	ReconciliationRequired bool                         `json:"reconciliation_required"`
	Projects               []TaskObservedProjectSummary `json:"projects,omitempty"`
}

// TaskRecord is the versioned persistence DTO for one execution attempt.
// NextEventSequence starts at one. TerminalAt is the retention epoch; the
// pruning scheduler can delete the task, events, and dedupe records together
// after RetainUntil without deriving time from a ULID.
type TaskRecord struct {
	ID                string                      `json:"id"`
	OperationID       string                      `json:"operation_id"`
	RetryOf           string                      `json:"retry_of,omitempty"`
	IdempotencyKey    string                      `json:"idempotency_key,omitempty"`
	Executor          TaskExecutor                `json:"executor"`
	PlanID            string                      `json:"plan_id"`
	PlanHash          string                      `json:"plan_hash,omitempty"`
	RenderGeneration  int32                       `json:"render_generation"`
	Type              TaskType                    `json:"type"`
	Target            string                      `json:"target"`
	Params            map[string]string           `json:"params,omitempty"`
	Steps             []TaskStepRecord            `json:"steps,omitempty"`
	Materializations  []TaskMaterializationRecord `json:"materializations,omitempty"`
	TimeoutSeconds    int64                       `json:"timeout_seconds"`
	Status            TaskStatus                  `json:"status"`
	Result            *TaskResultRecord           `json:"result,omitempty"`
	NextEventSequence uint64                      `json:"next_event_sequence"`
	EventCount        uint32                      `json:"event_count"`
	CreatedAt         time.Time                   `json:"created_at"`
	StartedAt         *time.Time                  `json:"started_at,omitempty"`
	TerminalAt        *time.Time                  `json:"terminal_at,omitempty"`
	RetainUntil       *time.Time                  `json:"retain_until,omitempty"`
	idempotencyMarker *IdempotencyLocator
}

// TaskEventIdentity is stable across Agent reconnects and Controller restarts.
// Attempt and Ordinal are one-based.
type TaskEventIdentity struct {
	TaskID  string `json:"task_id"`
	StepID  string `json:"step_id"`
	Attempt uint32 `json:"attempt"`
	Ordinal uint64 `json:"ordinal"`
}

// TaskEventInput is an unsequenced Agent event. Payload must be one complete
// JSON value; the Controller compacts it before hashing and persistence.
type TaskEventInput struct {
	Identity TaskEventIdentity
	State    TaskEventState
	Payload  json.RawMessage
}

// TaskEventRecord is one Controller-sequenced durable activity item.
type TaskEventRecord struct {
	Sequence      uint64            `json:"sequence"`
	Identity      TaskEventIdentity `json:"identity"`
	State         TaskEventState    `json:"state"`
	Payload       json.RawMessage   `json:"payload"`
	PayloadSHA256 string            `json:"payload_sha256"`
	ReceivedAt    time.Time         `json:"received_at"`
}

// TaskEventDedupRecord makes Agent delivery idempotent across process restarts.
// The repository stores it atomically with the event and Task summary.
type TaskEventDedupRecord struct {
	Identity      TaskEventIdentity `json:"identity"`
	Sequence      uint64            `json:"sequence"`
	PayloadSHA256 string            `json:"payload_sha256"`
}

// PreparedTaskEvent contains the values a repository must publish in one CAS
// transaction. Duplicate is true only for an identical prior Agent event.
type PreparedTaskEvent struct {
	Task      TaskRecord
	Event     TaskEventRecord
	Dedup     TaskEventDedupRecord
	Sequence  uint64
	Duplicate bool
}

type taskRecordData struct {
	ID                string                      `json:"id"`
	OperationID       string                      `json:"operation_id"`
	RetryOf           string                      `json:"retry_of,omitempty"`
	IdempotencyKey    string                      `json:"idempotency_key,omitempty"`
	Executor          TaskExecutor                `json:"executor"`
	PlanID            string                      `json:"plan_id"`
	PlanHash          string                      `json:"plan_hash,omitempty"`
	RenderGeneration  int32                       `json:"render_generation"`
	Type              TaskType                    `json:"type"`
	Target            string                      `json:"target"`
	Params            map[string]string           `json:"params,omitempty"`
	Steps             []TaskStepRecord            `json:"steps,omitempty"`
	Materializations  []TaskMaterializationRecord `json:"materializations,omitempty"`
	TimeoutSeconds    int64                       `json:"timeout_seconds"`
	Status            TaskStatus                  `json:"status"`
	Result            *taskResultData             `json:"result,omitempty"`
	NextEventSequence uint64                      `json:"next_event_sequence"`
	EventCount        uint32                      `json:"event_count"`
	CreatedAt         string                      `json:"created_at"`
	StartedAt         string                      `json:"started_at,omitempty"`
	TerminalAt        string                      `json:"terminal_at,omitempty"`
	RetainUntil       string                      `json:"retain_until,omitempty"`
	IdempotencyMarker *IdempotencyLocator         `json:"idempotency_marker,omitempty"`
}

type taskResultData struct {
	Kind                   TaskResultKind                   `json:"kind"`
	ExitCode               int32                            `json:"exit_code"`
	FailedStepID           string                           `json:"failed_step_id,omitempty"`
	Diagnostic             TaskResultDiagnostic             `json:"diagnostic"`
	ReconciliationRequired bool                             `json:"reconciliation_required"`
	Projects               []taskObservedProjectSummaryData `json:"projects,omitempty"`
}

type taskObservedProjectSummaryData struct {
	ProjectName    string `json:"project_name"`
	ObservedAt     string `json:"observed_at"`
	ContainerCount uint32 `json:"container_count"`
	NetworkCount   uint32 `json:"network_count"`
	VolumeCount    uint32 `json:"volume_count"`
	CollisionCount uint32 `json:"collision_count"`
}

type taskEventRecordData struct {
	Sequence      uint64            `json:"sequence"`
	Identity      TaskEventIdentity `json:"identity"`
	State         TaskEventState    `json:"state"`
	Payload       json.RawMessage   `json:"payload"`
	PayloadSHA256 string            `json:"payload_sha256"`
	ReceivedAt    string            `json:"received_at"`
}

type taskEventFingerprint struct {
	State   TaskEventState  `json:"state"`
	Payload json.RawMessage `json:"payload"`
}

func newTaskRecord(
	id string,
	operationID string,
	taskType TaskType,
	target string,
	timeoutSeconds int64,
	createdAt time.Time,
) TaskRecord {
	return TaskRecord{
		ID: id, OperationID: operationID, Executor: TaskExecutorAgent, Type: taskType, Target: target,
		TimeoutSeconds: timeoutSeconds, Status: TaskStatusPending,
		NextEventSequence: 1, CreatedAt: createdAt,
	}
}

func cloneRetryTask(source TaskRecord, id string, createdAt time.Time) (TaskRecord, error) {
	if err := validateTaskRecord(source); err != nil {
		return TaskRecord{}, err
	}
	if source.Status != TaskStatusFailed && source.Status != TaskStatusTimedOut && source.Status != TaskStatusAborted {
		return TaskRecord{}, errs.Newf(
			errs.KindTaskNotRetryable,
			"task %s has status %s",
			source.ID,
			source.Status,
		)
	}
	if err := validateStableID(ids.KindTask, id); err != nil {
		return TaskRecord{}, err
	}
	if id == source.ID {
		return TaskRecord{}, errs.New(errs.KindValidationFailed, "a retry requires a new task id")
	}

	retry := TaskRecord{
		ID: id, OperationID: source.OperationID, RetryOf: source.ID,
		IdempotencyKey: source.IdempotencyKey, Executor: source.Executor, PlanID: source.PlanID,
		PlanHash: source.PlanHash, RenderGeneration: source.RenderGeneration,
		Type: source.Type, Target: source.Target, Params: cloneStringMap(source.Params),
		Steps: cloneTaskSteps(source.Steps), TimeoutSeconds: source.TimeoutSeconds,
		Materializations: cloneTaskMaterializationReferences(source.Materializations),
		Status:           TaskStatusPending, NextEventSequence: 1, CreatedAt: createdAt,
	}
	if err := validateTaskRecord(retry); err != nil {
		return TaskRecord{}, err
	}
	return retry, nil
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
	if err := validateTimestamp("task transition", at); err != nil {
		return TaskRecord{}, err
	}

	replacement := cloneTaskRecord(record)
	replacement.Status = next
	if next == TaskStatusRunning {
		replacement.StartedAt = timePointer(at)
	}
	if isTerminalTaskStatus(next) {
		replacement.TerminalAt = timePointer(at)
		retention := at.Add(TaskRetention)
		replacement.RetainUntil = &retention
	}
	if err := validateTaskRecord(replacement); err != nil {
		return TaskRecord{}, err
	}
	return replacement, nil
}

func prepareTaskEvent(
	task TaskRecord,
	input TaskEventInput,
	existing *TaskEventDedupRecord,
	receivedAt time.Time,
) (PreparedTaskEvent, error) {
	if err := validateTaskRecord(task); err != nil {
		return PreparedTaskEvent{}, err
	}
	if err := validateTaskEventIdentity(input.Identity); err != nil {
		return PreparedTaskEvent{}, err
	}
	if input.Identity.TaskID != task.ID {
		return PreparedTaskEvent{}, errs.New(errs.KindValidationFailed, "task event identity does not match its task")
	}
	if !taskContainsStep(task, input.Identity.StepID) {
		return PreparedTaskEvent{}, errs.New(
			errs.KindValidationFailed,
			"task event step id does not belong to its task",
		)
	}
	if err := validateTimestamp("task event received_at", receivedAt); err != nil {
		return PreparedTaskEvent{}, err
	}
	payload, hash, err := canonicalTaskEventPayload(input.State, input.Payload)
	if err != nil {
		return PreparedTaskEvent{}, err
	}

	if existing != nil {
		if err := validateTaskEventDedupRecord(*existing); err != nil {
			return PreparedTaskEvent{}, err
		}
		if existing.Identity != input.Identity {
			return PreparedTaskEvent{}, errs.New(errs.KindInternal, "task event dedupe identity mismatch")
		}
		if existing.PayloadSHA256 != hash {
			return PreparedTaskEvent{}, errs.New(
				errs.KindInternal,
				"task event identity was reused with a different payload",
			)
		}
		return PreparedTaskEvent{
			Task: cloneTaskRecord(task), Sequence: existing.Sequence, Duplicate: true,
		}, nil
	}
	if isTerminalTaskStatus(task.Status) {
		return PreparedTaskEvent{}, errs.New(
			errs.KindInternal,
			"terminal task received a new event identity",
		)
	}

	if task.EventCount >= MaximumTaskEvents {
		return PreparedTaskEvent{}, errs.Newf(
			errs.KindValidationFailed,
			"task %s already has the maximum of %d durable events",
			task.ID,
			MaximumTaskEvents,
		)
	}
	updated := cloneTaskRecord(task)
	sequence := task.NextEventSequence
	event := TaskEventRecord{
		Sequence: sequence, Identity: input.Identity, State: input.State,
		Payload: payload, PayloadSHA256: hash, ReceivedAt: receivedAt,
	}
	if _, err := encodeTaskEventRecord(event); err != nil {
		return PreparedTaskEvent{}, err
	}
	updated.EventCount++
	updated.NextEventSequence++
	if err := validateTaskRecord(updated); err != nil {
		return PreparedTaskEvent{}, err
	}
	dedup := TaskEventDedupRecord{Identity: input.Identity, Sequence: sequence, PayloadSHA256: hash}
	return PreparedTaskEvent{Task: updated, Event: event, Dedup: dedup, Sequence: sequence}, nil
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

func validateTaskRecord(record TaskRecord) error {
	if err := validateStableID(ids.KindTask, record.ID); err != nil {
		return err
	}
	if err := validateStableID(ids.KindOperation, record.OperationID); err != nil {
		return err
	}
	if record.RetryOf != "" {
		if err := validateStableID(ids.KindTask, record.RetryOf); err != nil {
			return err
		}
		if record.RetryOf == record.ID {
			return errs.New(errs.KindValidationFailed, "task retry_of must name another task")
		}
	}
	if !validTaskExecutor(record.Executor) {
		return errs.New(errs.KindValidationFailed, "task executor is invalid")
	}
	if !validTaskType(record.Type) {
		return errs.New(errs.KindValidationFailed, "task type is not in the durable task catalog")
	}
	if record.Target == "" || !utf8.ValidString(record.Target) {
		return errs.New(errs.KindValidationFailed, "task target is required and must be valid UTF-8")
	}
	if err := validateStableID(ids.KindPlan, record.PlanID); err != nil {
		return err
	}
	if !validSHA256(record.PlanHash) {
		return errs.New(errs.KindValidationFailed, "task plan hash must be a lowercase SHA-256 digest")
	}
	if record.RenderGeneration <= 0 {
		return errs.New(errs.KindValidationFailed, "task render_generation must be positive")
	}
	materializationEnvironment, hasMaterializationEnvironment, err := taskMaterializationEnvironment(record)
	if err != nil {
		return err
	}
	if record.idempotencyMarker != nil {
		if err := validateIdempotencyLocator(*record.idempotencyMarker); err != nil {
			return errs.New(errs.KindInternal, "task idempotency marker locator is invalid")
		}
	}
	if record.TimeoutSeconds <= 0 {
		return errs.New(errs.KindValidationFailed, "task timeout_seconds must be positive")
	}
	if !validTaskStatus(record.Status) {
		return errs.New(errs.KindValidationFailed, "task status is invalid")
	}
	if record.EventCount > MaximumTaskEvents || record.NextEventSequence != uint64(record.EventCount)+1 {
		return errs.New(errs.KindInternal, "task event summary is inconsistent")
	}
	if err := validateTimestamp("task created_at", record.CreatedAt); err != nil {
		return err
	}
	if err := validateTaskSteps(record.Steps); err != nil {
		return err
	}
	if err := validateTaskMaterializationReferences(
		record.Materializations,
		record.Steps,
		materializationEnvironment,
		hasMaterializationEnvironment,
		uint64(record.RenderGeneration),
	); err != nil {
		return err
	}
	if record.Result != nil {
		if !isTerminalTaskStatus(record.Status) {
			return errs.New(errs.KindInternal, "nonterminal task has a completion result")
		}
		if err := validateTaskResult(*record.Result, record.Steps, record.Status); err != nil {
			return err
		}
	}
	return validateTaskTimeline(record)
}

func validateTaskResult(result TaskResultRecord, steps []TaskStepRecord, status TaskStatus) error {
	if result.Kind != TaskResultCompose && result.Kind != TaskResultEnvironmentDirectory {
		return errs.New(errs.KindValidationFailed, "task result kind is invalid")
	}
	switch result.Diagnostic {
	case TaskResultDiagnosticNone, TaskResultDiagnosticConfigRejected, TaskResultDiagnosticComposeFailed:
	default:
		return errs.New(errs.KindValidationFailed, "task result diagnostic is invalid")
	}
	if result.FailedStepID != "" {
		if err := validateStableID(ids.KindStep, result.FailedStepID); err != nil {
			return err
		}
		found := false
		for _, step := range steps {
			found = found || step.ID == result.FailedStepID
		}
		if !found {
			return errs.New(errs.KindValidationFailed, "task result failed step does not belong to the task")
		}
	}
	if status == TaskStatusCompleted && (result.ExitCode != 0 || result.FailedStepID != "" ||
		result.Diagnostic != TaskResultDiagnosticNone || result.ReconciliationRequired) {
		return errs.New(errs.KindValidationFailed, "completed task result is inconsistent")
	}
	if result.Kind == TaskResultEnvironmentDirectory &&
		(len(result.Projects) != 0 || result.Diagnostic != TaskResultDiagnosticNone || result.ReconciliationRequired) {
		return errs.New(errs.KindValidationFailed, "environment directory task result is inconsistent")
	}
	if len(result.Projects) > 64 {
		return errs.New(errs.KindValidationFailed, "task result has too many project summaries")
	}
	for index, project := range result.Projects {
		if project.ProjectName == "" || !utf8.ValidString(project.ProjectName) ||
			strings.IndexByte(project.ProjectName, 0) >= 0 {
			return errs.New(errs.KindValidationFailed, "task result project name is invalid")
		}
		if index > 0 && result.Projects[index-1].ProjectName >= project.ProjectName {
			return errs.New(errs.KindValidationFailed, "task result projects are not strictly sorted")
		}
		if err := validateTimestamp("task result observed_at", project.ObservedAt); err != nil {
			return err
		}
		if project.ContainerCount > 4096 || project.NetworkCount > 4096 ||
			project.VolumeCount > 4096 || project.CollisionCount > 4096 {
			return errs.New(errs.KindValidationFailed, "task result project count exceeds its limit")
		}
	}
	return nil
}

func validateTaskTimeline(record TaskRecord) error {
	if record.StartedAt != nil {
		if err := validateTimestamp("task started_at", *record.StartedAt); err != nil {
			return err
		}
		if record.StartedAt.Before(record.CreatedAt) {
			return errs.New(errs.KindInternal, "task started_at precedes created_at")
		}
	}
	if isTerminalTaskStatus(record.Status) {
		if record.TerminalAt == nil || record.RetainUntil == nil {
			return errs.New(errs.KindInternal, "terminal task is missing retention timestamps")
		}
		if err := validateTimestamp("task terminal_at", *record.TerminalAt); err != nil {
			return err
		}
		if err := validateTimestamp("task retain_until", *record.RetainUntil); err != nil {
			return err
		}
		if record.TerminalAt.Before(record.CreatedAt) ||
			(record.StartedAt != nil && record.TerminalAt.Before(*record.StartedAt)) {
			return errs.New(errs.KindInternal, "task terminal_at precedes its lifecycle")
		}
		if !record.RetainUntil.Equal(record.TerminalAt.Add(TaskRetention)) {
			return errs.New(errs.KindInternal, "task retention deadline is inconsistent")
		}
		return nil
	}
	if record.TerminalAt != nil || record.RetainUntil != nil {
		return errs.New(errs.KindInternal, "nonterminal task has terminal retention timestamps")
	}
	if record.Status == TaskStatusPending && record.StartedAt != nil {
		return errs.New(errs.KindInternal, "pending task has a started_at timestamp")
	}
	if record.Status == TaskStatusRunning && record.StartedAt == nil {
		return errs.New(errs.KindInternal, "running task is missing started_at")
	}
	return nil
}

func validateTaskSteps(steps []TaskStepRecord) error {
	seen := make(map[string]struct{}, len(steps))
	for _, step := range steps {
		if err := validateStableID(ids.KindStep, step.ID); err != nil {
			return err
		}
		if _, exists := seen[step.ID]; exists {
			return errs.New(errs.KindValidationFailed, "task step ids must be unique")
		}
		seen[step.ID] = struct{}{}
	}
	return nil
}

func validateTaskEventIdentity(identity TaskEventIdentity) error {
	if err := validateStableID(ids.KindTask, identity.TaskID); err != nil {
		return err
	}
	if err := validateStableID(ids.KindStep, identity.StepID); err != nil {
		return err
	}
	if identity.Attempt == 0 || identity.Ordinal == 0 {
		return errs.New(errs.KindValidationFailed, "task event attempt and ordinal must start at one")
	}
	return nil
}

func validateTaskEventRecord(record TaskEventRecord) error {
	if record.Sequence == 0 {
		return errs.New(errs.KindInternal, "task event sequence must start at one")
	}
	if err := validateTaskEventIdentity(record.Identity); err != nil {
		return err
	}
	if !validTaskEventState(record.State) {
		return errs.New(errs.KindInternal, "task event status is invalid")
	}
	if err := validateTimestamp("task event received_at", record.ReceivedAt); err != nil {
		return err
	}
	payload, hash, err := canonicalTaskEventPayload(record.State, record.Payload)
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, record.Payload) || hash != record.PayloadSHA256 {
		return errs.New(errs.KindInternal, "task event payload or digest is not canonical")
	}
	return nil
}

func validateTaskEventDedupRecord(record TaskEventDedupRecord) error {
	if err := validateTaskEventIdentity(record.Identity); err != nil {
		return err
	}
	if record.Sequence == 0 || !validSHA256(record.PayloadSHA256) {
		return errs.New(errs.KindInternal, "task event dedupe record is invalid")
	}
	return nil
}

func validTaskType(taskType TaskType) bool {
	switch taskType {
	case TaskDeploy, TaskRollback, TaskBackup, TaskRestore, TaskAttach, TaskDetach,
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

func validTaskEventState(state TaskEventState) bool {
	switch state {
	case TaskEventStatePending, TaskEventStateRunning, TaskEventStateCompleted,
		TaskEventStateFailed, TaskEventStateAborted, TaskEventStateTimedOut:
		return true
	default:
		return false
	}
}

func validateStableID(kind ids.Kind, value string) error {
	if err := ids.Validate(kind, value); err != nil {
		return errs.New(errs.KindValidationFailed, err.Error())
	}
	return nil
}

func validSHA256(value string) bool {
	if len(value) != sha256.Size*2 || value != strings.ToLower(value) {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validateTimestamp(field string, value time.Time) error {
	if value.IsZero() || value.Location() != time.UTC {
		return errs.Newf(errs.KindValidationFailed, "%s must be a non-zero UTC timestamp", field)
	}
	return nil
}

func canonicalTaskEventPayload(state TaskEventState, value json.RawMessage) (json.RawMessage, string, error) {
	if !validTaskEventState(state) {
		return nil, "", errs.New(errs.KindValidationFailed, "task event status is invalid")
	}
	if len(value) == 0 || rejectDuplicateJSONFields(value) != nil {
		return nil, "", errs.New(errs.KindValidationFailed, "task event payload must be one valid JSON value")
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, value); err != nil {
		return nil, "", errs.New(errs.KindValidationFailed, "task event payload must be valid JSON")
	}
	payload := json.RawMessage(append([]byte(nil), compact.Bytes()...))
	fingerprint, err := json.Marshal(taskEventFingerprint{State: state, Payload: payload})
	if err != nil {
		return nil, "", errs.Wrap(errs.KindInternal, err)
	}
	hash := sha256.Sum256(fingerprint)
	return payload, hex.EncodeToString(hash[:]), nil
}

func encodeTaskRecord(record TaskRecord) ([]byte, error) {
	if err := validateTaskRecord(record); err != nil {
		return nil, err
	}
	value, err := encodeEnvelope("task", taskRecordToData(record))
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumTaskRecordBytes {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"task record exceeds the %d-byte durable record limit",
			MaximumTaskRecordBytes,
		)
	}
	return value, nil
}

func decodeTaskRecord(value []byte) (TaskRecord, error) {
	data, err := decodeEnvelope[taskRecordData](value, "task")
	if err != nil {
		return TaskRecord{}, err
	}
	record, err := taskRecordFromData(data)
	if err != nil {
		return TaskRecord{}, errs.New(errs.KindInternal, "task record has invalid timestamps")
	}
	if err := validateTaskRecord(record); err != nil {
		return TaskRecord{}, corruptRecord()
	}
	return record, nil
}

func encodeTaskEventRecord(record TaskEventRecord) ([]byte, error) {
	if err := validateTaskEventRecord(record); err != nil {
		return nil, err
	}
	value, err := encodeEnvelope("task_event", taskEventRecordData{
		Sequence: record.Sequence, Identity: record.Identity, State: record.State,
		Payload: record.Payload, PayloadSHA256: record.PayloadSHA256,
		ReceivedAt: record.ReceivedAt.UTC().Format(time.RFC3339Nano),
	})
	if err != nil {
		return nil, err
	}
	if len(value) > MaximumTaskEventBytes {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"task event exceeds the %d-byte durable event limit",
			MaximumTaskEventBytes,
		)
	}
	return value, nil
}

func decodeTaskEventRecord(value []byte) (TaskEventRecord, error) {
	data, err := decodeEnvelope[taskEventRecordData](value, "task_event")
	if err != nil {
		return TaskEventRecord{}, err
	}
	receivedAt, err := parseCanonicalTimestamp(data.ReceivedAt)
	if err != nil {
		return TaskEventRecord{}, errs.New(errs.KindInternal, "task event record has an invalid received_at")
	}
	record := TaskEventRecord{
		Sequence: data.Sequence, Identity: data.Identity, State: data.State,
		Payload: data.Payload, PayloadSHA256: data.PayloadSHA256, ReceivedAt: receivedAt,
	}
	if err := validateTaskEventRecord(record); err != nil {
		return TaskEventRecord{}, corruptRecord()
	}
	if len(value) > MaximumTaskEventBytes {
		return TaskEventRecord{}, corruptRecord()
	}
	return record, nil
}

func encodeTaskEventDedupRecord(record TaskEventDedupRecord) ([]byte, error) {
	if err := validateTaskEventDedupRecord(record); err != nil {
		return nil, err
	}
	return encodeEnvelope("task_event_dedup", record)
}

func decodeTaskEventDedupRecord(value []byte) (TaskEventDedupRecord, error) {
	record, err := decodeEnvelope[TaskEventDedupRecord](value, "task_event_dedup")
	if err != nil {
		return TaskEventDedupRecord{}, err
	}
	if err := validateTaskEventDedupRecord(record); err != nil {
		return TaskEventDedupRecord{}, corruptRecord()
	}
	return record, nil
}

func taskRecordToData(record TaskRecord) taskRecordData {
	return taskRecordData{
		ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
		IdempotencyKey: record.IdempotencyKey, Executor: record.Executor, PlanID: record.PlanID,
		PlanHash: record.PlanHash, RenderGeneration: record.RenderGeneration,
		Type: record.Type, Target: record.Target, Params: cloneStringMap(record.Params),
		Steps: cloneTaskSteps(record.Steps), TimeoutSeconds: record.TimeoutSeconds,
		Materializations: cloneTaskMaterializationReferences(record.Materializations),
		Status:           record.Status, NextEventSequence: record.NextEventSequence,
		Result:     taskResultToData(record.Result),
		EventCount: record.EventCount, CreatedAt: record.CreatedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:         formatOptionalTimestamp(record.StartedAt),
		TerminalAt:        formatOptionalTimestamp(record.TerminalAt),
		RetainUntil:       formatOptionalTimestamp(record.RetainUntil),
		IdempotencyMarker: cloneIdempotencyLocator(record.idempotencyMarker),
	}
}

func taskRecordFromData(data taskRecordData) (TaskRecord, error) {
	createdAt, err := parseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	startedAt, err := parseOptionalTimestamp(data.StartedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	terminalAt, err := parseOptionalTimestamp(data.TerminalAt)
	if err != nil {
		return TaskRecord{}, err
	}
	retainUntil, err := parseOptionalTimestamp(data.RetainUntil)
	if err != nil {
		return TaskRecord{}, err
	}
	result, err := taskResultFromData(data.Result)
	if err != nil {
		return TaskRecord{}, err
	}
	return TaskRecord{
		ID: data.ID, OperationID: data.OperationID, RetryOf: data.RetryOf,
		IdempotencyKey: data.IdempotencyKey, Executor: data.Executor, PlanID: data.PlanID,
		PlanHash: data.PlanHash, RenderGeneration: data.RenderGeneration,
		Type: data.Type, Target: data.Target, Params: data.Params, Steps: data.Steps,
		Materializations: data.Materializations,
		TimeoutSeconds:   data.TimeoutSeconds, Status: data.Status,
		Result:            result,
		NextEventSequence: data.NextEventSequence, EventCount: data.EventCount,
		CreatedAt: createdAt, StartedAt: startedAt, TerminalAt: terminalAt,
		RetainUntil: retainUntil, idempotencyMarker: cloneIdempotencyLocator(data.IdempotencyMarker),
	}, nil
}

func taskResultToData(result *TaskResultRecord) *taskResultData {
	if result == nil {
		return nil
	}
	data := &taskResultData{
		Kind: result.Kind, ExitCode: result.ExitCode, FailedStepID: result.FailedStepID,
		Diagnostic: result.Diagnostic, ReconciliationRequired: result.ReconciliationRequired,
		Projects: make([]taskObservedProjectSummaryData, len(result.Projects)),
	}
	for index, project := range result.Projects {
		data.Projects[index] = taskObservedProjectSummaryData{
			ProjectName: project.ProjectName, ObservedAt: project.ObservedAt.UTC().Format(time.RFC3339Nano),
			ContainerCount: project.ContainerCount, NetworkCount: project.NetworkCount,
			VolumeCount: project.VolumeCount, CollisionCount: project.CollisionCount,
		}
	}
	return data
}

func taskResultFromData(data *taskResultData) (*TaskResultRecord, error) {
	if data == nil {
		return nil, nil
	}
	result := &TaskResultRecord{
		Kind: data.Kind, ExitCode: data.ExitCode, FailedStepID: data.FailedStepID,
		Diagnostic: data.Diagnostic, ReconciliationRequired: data.ReconciliationRequired,
		Projects: make([]TaskObservedProjectSummary, len(data.Projects)),
	}
	for index, project := range data.Projects {
		observedAt, err := parseCanonicalTimestamp(project.ObservedAt)
		if err != nil {
			return nil, err
		}
		result.Projects[index] = TaskObservedProjectSummary{
			ProjectName: project.ProjectName, ObservedAt: observedAt,
			ContainerCount: project.ContainerCount, NetworkCount: project.NetworkCount,
			VolumeCount: project.VolumeCount, CollisionCount: project.CollisionCount,
		}
	}
	return result, nil
}

func parseCanonicalTimestamp(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil || value != parsed.UTC().Format(time.RFC3339Nano) {
		return time.Time{}, errs.New(errs.KindInternal, "timestamp is not canonical UTC")
	}
	return parsed, nil
}

func parseOptionalTimestamp(value string) (*time.Time, error) {
	if value == "" {
		return nil, nil
	}
	parsed, err := parseCanonicalTimestamp(value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func formatOptionalTimestamp(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func taskContainsStep(task TaskRecord, stepID string) bool {
	for _, step := range task.Steps {
		if step.ID == stepID {
			return true
		}
	}
	return false
}

func cloneTaskRecord(record TaskRecord) TaskRecord {
	cloned := record
	cloned.Params = cloneStringMap(record.Params)
	cloned.Steps = cloneTaskSteps(record.Steps)
	cloned.Materializations = cloneTaskMaterializationReferences(record.Materializations)
	cloned.StartedAt = cloneTimePointer(record.StartedAt)
	cloned.TerminalAt = cloneTimePointer(record.TerminalAt)
	cloned.RetainUntil = cloneTimePointer(record.RetainUntil)
	cloned.Result = cloneTaskResult(record.Result)
	cloned.idempotencyMarker = cloneIdempotencyLocator(record.idempotencyMarker)
	return cloned
}

func cloneTaskResult(result *TaskResultRecord) *TaskResultRecord {
	if result == nil {
		return nil
	}
	cloned := *result
	cloned.Projects = append([]TaskObservedProjectSummary(nil), result.Projects...)
	return &cloned
}

func cloneIdempotencyLocator(locator *IdempotencyLocator) *IdempotencyLocator {
	if locator == nil {
		return nil
	}
	cloned := *locator
	return &cloned
}

func cloneTaskSteps(steps []TaskStepRecord) []TaskStepRecord {
	if steps == nil {
		return nil
	}
	cloned := make([]TaskStepRecord, len(steps))
	for index, step := range steps {
		cloned[index] = TaskStepRecord{ID: step.ID}
	}
	return cloned
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

func cloneTimePointer(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

func timePointer(value time.Time) *time.Time {
	return &value
}
