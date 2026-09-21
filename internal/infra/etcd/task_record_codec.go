package etcd

import (
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskRecordData struct {
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
	EntryRuntime       *taskjournal.EntryTaskRuntime             `json:"entry_runtime,omitempty"`
	Configuration      *taskconfiguration.TaskConfiguration      `json:"configuration,omitempty"`
	TimeoutSeconds     int64                                     `json:"timeout_seconds"`
	Status             taskjournal.TaskStatus                    `json:"status"`
	Result             *taskjournal.TaskResultData               `json:"result,omitempty"`
	TerminalAssignment *taskjournal.TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	NextEventSequence  uint64                                    `json:"next_event_sequence"`
	EventCount         uint32                                    `json:"event_count"`
	EventCheckpoints   []taskjournal.TaskEventCheckpoint         `json:"event_checkpoints,omitempty"`
	CreatedAt          string                                    `json:"created_at"`
	UpdatedAt          string                                    `json:"updated_at"`
	StartedAt          string                                    `json:"started_at,omitempty"`
	FinishedAt         string                                    `json:"finished_at,omitempty"`
	RetainUntil        string                                    `json:"retain_until,omitempty"`
	IdempotencyMarker  *idempotencyrecord.IdempotencyLocator     `json:"idempotency_marker,omitempty"`

	ComponentActionStepIDs          []string                                         `json:"component_action_step_ids"`
	ManagedComponentTeardownSources []projectionrecord.ManagedComponentRuntimeSource `json:"managed_component_teardown_sources,omitempty"`
}

func EncodeTaskRecord(record TaskRecord) ([]byte, error) {
	if err := ValidateTaskRecord(record); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("task", taskRecordToData(record))
	if err != nil {
		return nil, err
	}
	if len(value) > taskjournal.MaximumTaskRecordBytes {
		return nil, errs.Newf(
			errs.KindValidationFailed,
			"task record exceeds the %d-byte durable record limit",
			taskjournal.MaximumTaskRecordBytes,
		)
	}
	return value, nil
}

func DecodeTaskRecord(value []byte) (TaskRecord, error) {
	data, err := recordcodec.Decode[taskRecordData](value, "task")
	if err != nil {
		return TaskRecord{}, err
	}
	record, err := taskRecordFromData(data)
	if err != nil {
		return TaskRecord{}, errs.New(errs.KindInternal, "task record has invalid timestamps")
	}
	if err := ValidateTaskRecord(record); err != nil {
		return TaskRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func taskRecordToData(record TaskRecord) taskRecordData {
	return taskRecordData{
		ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
		IdempotencyKey: record.IdempotencyKey, Owner: record.Owner, Actor: record.Actor,
		Executor: record.Executor, PlanID: record.PlanID,
		PlanHash: record.PlanHash, RenderGeneration: record.RenderGeneration,
		Type: record.Type, Target: record.Target, Params: cloneStringMap(record.Params),
		Steps: taskjournal.CloneTaskSteps(record.Steps), TimeoutSeconds: record.TimeoutSeconds,
		ComponentActionStepIDs: append([]string(nil), record.ComponentActionStepIDs...),
		ManagedComponentTeardownSources: projectionrecord.CloneManagedComponentRuntimeSources(
			record.ManagedComponentTeardownSources,
		),
		Materializations: materializationrecord.Clone(record.Materializations),
		EntryRuntime:     taskjournal.CloneEntryTaskRuntime(record.EntryRuntime),
		Configuration:    taskconfiguration.CloneTaskConfiguration(record.Configuration),
		Status:           record.Status, NextEventSequence: record.NextEventSequence,
		EventCheckpoints:   append([]taskjournal.TaskEventCheckpoint(nil), record.EventCheckpoints...),
		Result:             taskjournal.TaskResultToData(record.Result),
		TerminalAssignment: taskjournal.CloneTaskTerminalAssignment(record.TerminalAssignment),
		EventCount:         record.EventCount, CreatedAt: record.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:         record.UpdatedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:         formatOptionalTimestamp(record.StartedAt),
		FinishedAt:        formatOptionalTimestamp(record.FinishedAt),
		RetainUntil:       formatOptionalTimestamp(record.RetainUntil),
		IdempotencyMarker: cloneIdempotencyLocator(record.idempotencyMarker),
	}
}

func taskRecordFromData(data taskRecordData) (TaskRecord, error) {
	createdAt, err := recordcodec.ParseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	updatedAt, err := recordcodec.ParseCanonicalTimestamp(data.UpdatedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	startedAt, err := parseOptionalTimestamp(data.StartedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	finishedAt, err := parseOptionalTimestamp(data.FinishedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	retainUntil, err := parseOptionalTimestamp(data.RetainUntil)
	if err != nil {
		return TaskRecord{}, err
	}
	result, err := taskjournal.TaskResultFromData(data.Result)
	if err != nil {
		return TaskRecord{}, err
	}
	return TaskRecord{
		EventCheckpoints: append([]taskjournal.TaskEventCheckpoint(nil), data.EventCheckpoints...),
		ID:               data.ID, OperationID: data.OperationID, RetryOf: data.RetryOf,
		IdempotencyKey: data.IdempotencyKey, Owner: data.Owner, Actor: data.Actor,
		Executor: data.Executor, PlanID: data.PlanID,
		PlanHash: data.PlanHash, RenderGeneration: data.RenderGeneration,
		Type: data.Type, Target: data.Target, Params: data.Params, Steps: data.Steps,
		ComponentActionStepIDs: append([]string(nil), data.ComponentActionStepIDs...),
		ManagedComponentTeardownSources: projectionrecord.CloneManagedComponentRuntimeSources(
			data.ManagedComponentTeardownSources,
		),
		Materializations: data.Materializations,
		EntryRuntime:     data.EntryRuntime,
		Configuration:    data.Configuration,
		TimeoutSeconds:   data.TimeoutSeconds, Status: data.Status,
		Result:             result,
		TerminalAssignment: taskjournal.CloneTaskTerminalAssignment(data.TerminalAssignment),
		NextEventSequence:  data.NextEventSequence, EventCount: data.EventCount,
		CreatedAt: createdAt, UpdatedAt: updatedAt, StartedAt: startedAt, FinishedAt: finishedAt,
		RetainUntil: retainUntil, idempotencyMarker: cloneIdempotencyLocator(data.IdempotencyMarker),
	}, nil
}

func cloneTaskRecord(record TaskRecord) TaskRecord {
	cloned := record
	cloned.EventCheckpoints = append([]taskjournal.TaskEventCheckpoint(nil), record.EventCheckpoints...)
	cloned.ComponentActionStepIDs = append([]string(nil), record.ComponentActionStepIDs...)
	cloned.ManagedComponentTeardownSources = projectionrecord.CloneManagedComponentRuntimeSources(
		record.ManagedComponentTeardownSources,
	)
	cloned.Params = cloneStringMap(record.Params)
	cloned.Steps = taskjournal.CloneTaskSteps(record.Steps)
	cloned.Materializations = materializationrecord.Clone(record.Materializations)
	cloned.EntryRuntime = taskjournal.CloneEntryTaskRuntime(record.EntryRuntime)
	cloned.Configuration = taskconfiguration.CloneTaskConfiguration(record.Configuration)
	cloned.StartedAt = cloneTimePointer(record.StartedAt)
	cloned.FinishedAt = cloneTimePointer(record.FinishedAt)
	cloned.RetainUntil = cloneTimePointer(record.RetainUntil)
	cloned.Result = taskjournal.CloneTaskResult(record.Result)
	cloned.TerminalAssignment = taskjournal.CloneTaskTerminalAssignment(record.TerminalAssignment)
	cloned.idempotencyMarker = cloneIdempotencyLocator(record.idempotencyMarker)
	return cloned
}
