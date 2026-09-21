package etcd

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"math"
	"time"
)

// PreparedTaskEvent contains the values a repository must publish in one CAS
// transaction. Duplicate is true only for an identical prior Agent event.
type PreparedTaskEvent struct {
	Task      TaskRecord
	Event     taskjournal.TaskEventRecord
	Dedup     taskjournal.TaskEventDedupRecord
	Sequence  uint64
	Duplicate bool
}

func prepareTaskEvent(
	task TaskRecord,
	input taskjournal.TaskEventInput,
	existing *taskjournal.TaskEventDedupRecord,
	receivedAt time.Time,
) (PreparedTaskEvent, error) {
	if err := ValidateTaskRecord(task); err != nil {
		return PreparedTaskEvent{}, err
	}
	if err := taskjournal.ValidateTaskEventIdentity(input.Identity); err != nil {
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
	payload, hash, err := taskjournal.CanonicalTaskEventPayload(input.State, input.Payload)
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
		if err := taskjournal.ValidateTaskEventDedupRecord(*existing); err != nil {
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
	if taskjournal.IsTerminalTaskStatus(task.Status) {
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
	event := taskjournal.TaskEventRecord{
		Sequence: sequence, Identity: input.Identity, State: input.State,
		Payload: payload, PayloadSHA256: hash, ReceivedAt: eventAt,
	}
	if _, err := taskjournal.EncodeTaskEventRecord(event); err != nil {
		return PreparedTaskEvent{}, err
	}
	updated.EventCount = min(updated.EventCount+1, taskjournal.MaximumTaskEvents)
	updated.NextEventSequence++
	updated.UpdatedAt = eventAt
	if err := ValidateTaskRecord(updated); err != nil {
		return PreparedTaskEvent{}, err
	}
	dedup := taskjournal.TaskEventDedupRecord{Identity: input.Identity, Sequence: sequence, PayloadSHA256: hash}
	return PreparedTaskEvent{Task: updated, Event: event, Dedup: dedup, Sequence: sequence}, nil
}
