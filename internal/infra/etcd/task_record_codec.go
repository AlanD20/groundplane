package etcd

import (
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type taskRecordData struct {
	ID                 string                        `json:"id"`
	OperationID        string                        `json:"operation_id"`
	RetryOf            string                        `json:"retry_of,omitempty"`
	IdempotencyKey     string                        `json:"idempotency_key,omitempty"`
	Owner              TaskOwner                     `json:"owner"`
	Actor              TaskActor                     `json:"actor"`
	Executor           TaskExecutor                  `json:"executor"`
	PlanID             string                        `json:"plan_id"`
	PlanHash           string                        `json:"plan_hash,omitempty"`
	RenderGeneration   int32                         `json:"render_generation"`
	Type               TaskType                      `json:"type"`
	Target             string                        `json:"target"`
	Params             map[string]string             `json:"params,omitempty"`
	Steps              []TaskStepRecord              `json:"steps,omitempty"`
	Materializations   []TaskMaterializationRecord   `json:"materializations,omitempty"`
	EntryRuntime       *EntryTaskRuntime             `json:"entry_runtime,omitempty"`
	Configuration      *TaskConfiguration            `json:"configuration,omitempty"`
	TimeoutSeconds     int64                         `json:"timeout_seconds"`
	Status             TaskStatus                    `json:"status"`
	Result             *taskResultData               `json:"result,omitempty"`
	TerminalAssignment *TaskTerminalAssignmentRecord `json:"terminal_assignment,omitempty"`
	NextEventSequence  uint64                        `json:"next_event_sequence"`
	EventCount         uint32                        `json:"event_count"`
	EventCheckpoints   []TaskEventCheckpoint         `json:"event_checkpoints,omitempty"`
	CreatedAt          string                        `json:"created_at"`
	UpdatedAt          string                        `json:"updated_at"`
	StartedAt          string                        `json:"started_at,omitempty"`
	FinishedAt         string                        `json:"finished_at,omitempty"`
	RetainUntil        string                        `json:"retain_until,omitempty"`
	IdempotencyMarker  *IdempotencyLocator           `json:"idempotency_marker,omitempty"`

	ComponentActionStepIDs          []string                        `json:"component_action_step_ids"`
	ManagedComponentTeardownSources []ManagedComponentRuntimeSource `json:"managed_component_teardown_sources,omitempty"`
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

func EncodeTaskStorageRecord(record TaskRecord) ([]byte, error) { return encodeTaskRecord(record) }

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

func DecodeTaskStorageRecord(value []byte) (TaskRecord, error) { return decodeTaskRecord(value) }

func taskRecordToData(record TaskRecord) taskRecordData {
	return taskRecordData{
		ID: record.ID, OperationID: record.OperationID, RetryOf: record.RetryOf,
		IdempotencyKey: record.IdempotencyKey, Owner: record.Owner, Actor: record.Actor,
		Executor: record.Executor, PlanID: record.PlanID,
		PlanHash: record.PlanHash, RenderGeneration: record.RenderGeneration,
		Type: record.Type, Target: record.Target, Params: cloneStringMap(record.Params),
		Steps: cloneTaskSteps(record.Steps), TimeoutSeconds: record.TimeoutSeconds,
		ComponentActionStepIDs:          append([]string(nil), record.ComponentActionStepIDs...),
		ManagedComponentTeardownSources: cloneManagedComponentRuntimeSources(record.ManagedComponentTeardownSources),
		Materializations:                cloneTaskMaterializationReferences(record.Materializations),
		EntryRuntime:                    cloneEntryTaskRuntime(record.EntryRuntime),
		Configuration:                   cloneTaskConfiguration(record.Configuration),
		Status:                          record.Status, NextEventSequence: record.NextEventSequence,
		EventCheckpoints:   append([]TaskEventCheckpoint(nil), record.EventCheckpoints...),
		Result:             taskResultToData(record.Result),
		TerminalAssignment: cloneTaskTerminalAssignment(record.TerminalAssignment),
		EventCount:         record.EventCount, CreatedAt: record.CreatedAt.UTC().Format(time.RFC3339Nano),
		UpdatedAt:         record.UpdatedAt.UTC().Format(time.RFC3339Nano),
		StartedAt:         formatOptionalTimestamp(record.StartedAt),
		FinishedAt:        formatOptionalTimestamp(record.FinishedAt),
		RetainUntil:       formatOptionalTimestamp(record.RetainUntil),
		IdempotencyMarker: cloneIdempotencyLocator(record.idempotencyMarker),
	}
}

func taskRecordFromData(data taskRecordData) (TaskRecord, error) {
	createdAt, err := parseCanonicalTimestamp(data.CreatedAt)
	if err != nil {
		return TaskRecord{}, err
	}
	updatedAt, err := parseCanonicalTimestamp(data.UpdatedAt)
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
	result, err := taskResultFromData(data.Result)
	if err != nil {
		return TaskRecord{}, err
	}
	return TaskRecord{
		EventCheckpoints: append([]TaskEventCheckpoint(nil), data.EventCheckpoints...),
		ID:               data.ID, OperationID: data.OperationID, RetryOf: data.RetryOf,
		IdempotencyKey: data.IdempotencyKey, Owner: data.Owner, Actor: data.Actor,
		Executor: data.Executor, PlanID: data.PlanID,
		PlanHash: data.PlanHash, RenderGeneration: data.RenderGeneration,
		Type: data.Type, Target: data.Target, Params: data.Params, Steps: data.Steps,
		ComponentActionStepIDs:          append([]string(nil), data.ComponentActionStepIDs...),
		ManagedComponentTeardownSources: cloneManagedComponentRuntimeSources(data.ManagedComponentTeardownSources),
		Materializations:                data.Materializations,
		EntryRuntime:                    data.EntryRuntime,
		Configuration:                   data.Configuration,
		TimeoutSeconds:                  data.TimeoutSeconds, Status: data.Status,
		Result:             result,
		TerminalAssignment: cloneTaskTerminalAssignment(data.TerminalAssignment),
		NextEventSequence:  data.NextEventSequence, EventCount: data.EventCount,
		CreatedAt: createdAt, UpdatedAt: updatedAt, StartedAt: startedAt, FinishedAt: finishedAt,
		RetainUntil: retainUntil, idempotencyMarker: cloneIdempotencyLocator(data.IdempotencyMarker),
	}, nil
}

func cloneTaskRecord(record TaskRecord) TaskRecord {
	cloned := record
	cloned.EventCheckpoints = append([]TaskEventCheckpoint(nil), record.EventCheckpoints...)
	cloned.ComponentActionStepIDs = append([]string(nil), record.ComponentActionStepIDs...)
	cloned.ManagedComponentTeardownSources = cloneManagedComponentRuntimeSources(record.ManagedComponentTeardownSources)
	cloned.Params = cloneStringMap(record.Params)
	cloned.Steps = cloneTaskSteps(record.Steps)
	cloned.Materializations = cloneTaskMaterializationReferences(record.Materializations)
	cloned.EntryRuntime = cloneEntryTaskRuntime(record.EntryRuntime)
	cloned.Configuration = cloneTaskConfiguration(record.Configuration)
	cloned.StartedAt = cloneTimePointer(record.StartedAt)
	cloned.FinishedAt = cloneTimePointer(record.FinishedAt)
	cloned.RetainUntil = cloneTimePointer(record.RetainUntil)
	cloned.Result = cloneTaskResult(record.Result)
	cloned.TerminalAssignment = cloneTaskTerminalAssignment(record.TerminalAssignment)
	cloned.idempotencyMarker = cloneIdempotencyLocator(record.idempotencyMarker)
	return cloned
}
