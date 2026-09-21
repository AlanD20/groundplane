package etcd

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

// TaskEventIdentity is stable across Agent reconnects and Controller restarts.
// Attempt and Ordinal are one-based.
type TaskEventIdentity struct {
	AssignmentID    string `json:"assignment_id"`
	AgentID         string `json:"agent_id"`
	AgentGeneration uint64 `json:"agent_generation"`
	TaskID          string `json:"task_id"`
	StepID          string `json:"step_id"`
	Attempt         uint32 `json:"attempt"`
	Ordinal         uint64 `json:"ordinal"`
}

// TaskEventInput is an unsequenced Agent event. Payload must be one complete
// JSON value; the Controller compacts it before hashing and persistence.
type TaskEventInput struct {
	Identity TaskEventIdentity
	State    taskjournal.TaskEventState
	Payload  json.RawMessage
}

// TaskEventRecord is one Controller-sequenced durable activity item.
type TaskEventRecord struct {
	Sequence      uint64                     `json:"sequence"`
	Identity      TaskEventIdentity          `json:"identity"`
	State         taskjournal.TaskEventState `json:"state"`
	Payload       json.RawMessage            `json:"payload"`
	PayloadSHA256 string                     `json:"payload_sha256"`
	ReceivedAt    time.Time                  `json:"received_at"`
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

type taskEventRecordData struct {
	Sequence      uint64                     `json:"sequence"`
	Identity      TaskEventIdentity          `json:"identity"`
	State         taskjournal.TaskEventState `json:"state"`
	Payload       json.RawMessage            `json:"payload"`
	PayloadSHA256 string                     `json:"payload_sha256"`
	ReceivedAt    string                     `json:"received_at"`
}

type taskEventFingerprint struct {
	State   taskjournal.TaskEventState `json:"state"`
	Payload json.RawMessage            `json:"payload"`
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
	if err := recordcodec.ValidateTimestamp("task event received_at", receivedAt); err != nil {
		return PreparedTaskEvent{}, err
	}
	payload, hash, err := canonicalTaskEventPayload(input.State, input.Payload)
	if err != nil {
		return PreparedTaskEvent{}, err
	}

	if existing == nil {
		existing, err = trimmedTaskEventReplay(task, input, hash)
		if err != nil {
			return PreparedTaskEvent{}, err
		}
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
			Task: cloneTaskRecord(task), Sequence: existing.Sequence, Dedup: *existing, Duplicate: true,
		}, nil
	}
	if isTerminalTaskStatus(task.Status) {
		return PreparedTaskEvent{}, errs.New(
			errs.KindInternal,
			"terminal task received a new event identity",
		)
	}

	if task.NextEventSequence == math.MaxUint64 {
		return PreparedTaskEvent{}, errs.New(errs.KindInternal, "task event sequence exhausted")
	}
	updated := cloneTaskRecord(task)
	sequence := task.NextEventSequence
	eventAt, err := nextTaskControllerTimestamp(task.UpdatedAt, receivedAt)
	if err != nil {
		return PreparedTaskEvent{}, err
	}
	event := TaskEventRecord{
		Sequence: sequence, Identity: input.Identity, State: input.State,
		Payload: payload, PayloadSHA256: hash, ReceivedAt: eventAt,
	}
	if _, err := encodeTaskEventRecord(event); err != nil {
		return PreparedTaskEvent{}, err
	}
	updated.EventCount = min(updated.EventCount+1, MaximumTaskEvents)
	updated.NextEventSequence++
	updated.UpdatedAt = eventAt
	if err := validateTaskRecord(updated); err != nil {
		return PreparedTaskEvent{}, err
	}
	dedup := TaskEventDedupRecord{Identity: input.Identity, Sequence: sequence, PayloadSHA256: hash}
	return PreparedTaskEvent{Task: updated, Event: event, Dedup: dedup, Sequence: sequence}, nil
}

func validateTaskEventIdentity(identity TaskEventIdentity) error {
	if err := recordcodec.ValidateID(ids.KindAssignment, identity.AssignmentID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindAgent, identity.AgentID); err != nil {
		return err
	}
	if identity.AgentGeneration == 0 {
		return errs.New(errs.KindValidationFailed, "task event Agent generation must be positive")
	}
	if err := recordcodec.ValidateID(ids.KindTask, identity.TaskID); err != nil {
		return err
	}
	if err := recordcodec.ValidateID(ids.KindStep, identity.StepID); err != nil {
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
	if err := recordcodec.ValidateTimestamp("task event received_at", record.ReceivedAt); err != nil {
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
	if record.Sequence == 0 || !recordcodec.ValidSHA256(record.PayloadSHA256) {
		return errs.New(errs.KindInternal, "task event dedupe record is invalid")
	}
	return nil
}

func validTaskEventState(state taskjournal.TaskEventState) bool {
	switch state {
	case taskjournal.TaskEventStatePending, taskjournal.TaskEventStateRunning, taskjournal.TaskEventStateCompleted,
		taskjournal.TaskEventStateFailed, taskjournal.TaskEventStateAborted, taskjournal.TaskEventStateTimedOut:
		return true
	default:
		return false
	}
}

func canonicalTaskEventPayload(state taskjournal.TaskEventState, value json.RawMessage) (json.RawMessage, string, error) {
	if !validTaskEventState(state) {
		return nil, "", errs.New(errs.KindValidationFailed, "task event status is invalid")
	}
	if len(value) == 0 || recordcodec.RejectDuplicateFields(value) != nil {
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

func encodeTaskEventRecord(record TaskEventRecord) ([]byte, error) {
	if err := validateTaskEventRecord(record); err != nil {
		return nil, err
	}
	value, err := recordcodec.Encode("task_event", taskEventRecordData{
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
	data, err := recordcodec.Decode[taskEventRecordData](value, "task_event")
	if err != nil {
		return TaskEventRecord{}, err
	}
	receivedAt, err := recordcodec.ParseCanonicalTimestamp(data.ReceivedAt)
	if err != nil {
		return TaskEventRecord{}, errs.New(errs.KindInternal, "task event record has an invalid received_at")
	}
	record := TaskEventRecord{
		Sequence: data.Sequence, Identity: data.Identity, State: data.State,
		Payload: data.Payload, PayloadSHA256: data.PayloadSHA256, ReceivedAt: receivedAt,
	}
	if err := validateTaskEventRecord(record); err != nil {
		return TaskEventRecord{}, recordcodec.CorruptRecord()
	}
	if len(value) > MaximumTaskEventBytes {
		return TaskEventRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}

func encodeTaskEventDedupRecord(record TaskEventDedupRecord) ([]byte, error) {
	if err := validateTaskEventDedupRecord(record); err != nil {
		return nil, err
	}
	return recordcodec.Encode("task_event_dedup", record)
}

func decodeTaskEventDedupRecord(value []byte) (TaskEventDedupRecord, error) {
	record, err := recordcodec.Decode[TaskEventDedupRecord](value, "task_event_dedup")
	if err != nil {
		return TaskEventDedupRecord{}, err
	}
	if err := validateTaskEventDedupRecord(record); err != nil {
		return TaskEventDedupRecord{}, recordcodec.CorruptRecord()
	}
	return record, nil
}
