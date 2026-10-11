package taskjournal

import (
	"slices"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
)

const MaximumAttemptHistoryBytes = 64 * 1024

// AttemptHistory summarizes evicted observations. It is never execution,
// replay, mutation, or recovery authority; those owners retain their own proof.
type AttemptHistory struct {
	TaskID           string           `json:"task_id"`
	ThroughSequence  uint64           `json:"through_sequence"`
	DroppedSummaries uint64           `json:"dropped_summaries"`
	Summaries        []AttemptSummary `json:"summaries"`
}

type AttemptSummary struct {
	Identity            TaskEventIdentity                 `json:"identity"`
	FirstSequence       uint64                            `json:"first_sequence"`
	LastSequence        uint64                            `json:"last_sequence"`
	LastReceivedAt      time.Time                         `json:"last_received_at"`
	StartedAt           *time.Time                        `json:"started_at,omitempty"`
	FinishedAt          *time.Time                        `json:"finished_at,omitempty"`
	State               TaskEventState                    `json:"state"`
	ControllerOperation *taskprogress.ControllerOperation `json:"controller_operation,omitempty"`
	Failure             *taskprogress.Failure             `json:"failure,omitempty"`
}

func TaskAttemptHistoryKey(taskID string) string {
	return "/v1/runtime/task-attempt-history/" + taskID
}

func ValidateAttemptHistory(history AttemptHistory) error {
	if ids.Validate(ids.KindTask, history.TaskID) != nil || history.ThroughSequence == 0 ||
		len(history.Summaries) == 0 || len(history.Summaries) > MaximumTaskEvents ||
		history.DroppedSummaries > history.ThroughSequence {
		return recordcodec.CorruptRecord()
	}
	previous := uint64(0)
	identities := make(map[TaskEventIdentity]bool, len(history.Summaries))
	for _, summary := range history.Summaries {
		if ValidateTaskEventIdentity(summary.Identity) != nil || summary.Identity.TaskID != history.TaskID ||
			summary.FirstSequence == 0 || summary.FirstSequence > summary.LastSequence ||
			summary.LastSequence <= previous || summary.LastSequence > history.ThroughSequence ||
			identities[summary.Identity] || !ValidTaskEventState(summary.State) ||
			recordcodec.ValidateTimestamp("attempt last_received_at", summary.LastReceivedAt) != nil ||
			summary.ControllerOperation.Validate() != nil || summary.Failure.Validate() != nil ||
			summary.Identity.ControllerOperation() != (summary.ControllerOperation != nil) {
			return recordcodec.CorruptRecord()
		}
		if summary.Failure != nil && summary.State != TaskEventStateFailed &&
			summary.State != TaskEventStateAborted && summary.State != TaskEventStateTimedOut {
			return recordcodec.CorruptRecord()
		}
		for _, value := range []*time.Time{summary.StartedAt, summary.FinishedAt} {
			if value != nil &&
				(recordcodec.ValidateTimestamp("attempt observation", *value) != nil || value.After(summary.LastReceivedAt)) {
				return recordcodec.CorruptRecord()
			}
		}
		terminal := summary.State == TaskEventStateCompleted || summary.State == TaskEventStateFailed ||
			summary.State == TaskEventStateAborted || summary.State == TaskEventStateTimedOut
		if terminal != (summary.FinishedAt != nil) {
			return recordcodec.CorruptRecord()
		}
		identities[summary.Identity], previous = true, summary.LastSequence
	}
	return nil
}

func DecodeAttemptHistory(value []byte) (AttemptHistory, error) {
	if len(value) > MaximumAttemptHistoryBytes {
		return AttemptHistory{}, recordcodec.CorruptRecord()
	}
	history, err := recordcodec.Decode[AttemptHistory](value, "task-attempt-history")
	if err != nil || ValidateAttemptHistory(history) != nil {
		return AttemptHistory{}, recordcodec.CorruptRecord()
	}
	return history, nil
}

// AppendEvictedAttemptEvent consumes every trimmed sequence exactly once.
// Completed and failed attempts survive later attempts of the same sealed step.
func AppendEvictedAttemptEvent(history AttemptHistory, event TaskEventRecord) (AttemptHistory, []byte, error) {
	if history.TaskID != event.Identity.TaskID || history.ThroughSequence+1 != event.Sequence ||
		validateTaskEventRecord(event) != nil {
		return AttemptHistory{}, nil, recordcodec.CorruptRecord()
	}
	if history.ThroughSequence != 0 && ValidateAttemptHistory(history) != nil {
		return AttemptHistory{}, nil, recordcodec.CorruptRecord()
	}
	next := CloneAttemptHistory(history)
	if err := observeAttemptEvent(&next, event, true); err != nil {
		return AttemptHistory{}, nil, err
	}
	next.ThroughSequence = event.Sequence
	for {
		value, err := recordcodec.Encode("task-attempt-history", next)
		if err != nil {
			return AttemptHistory{}, nil, err
		}
		if len(next.Summaries) <= MaximumTaskEvents && len(value) <= MaximumAttemptHistoryBytes {
			if err := ValidateAttemptHistory(next); err != nil {
				clear(value)
				return AttemptHistory{}, nil, err
			}
			return next, value, nil
		}
		clear(value)
		if len(next.Summaries) <= 1 {
			return AttemptHistory{}, nil, recordcodec.CorruptRecord()
		}
		next.Summaries = slices.Delete(next.Summaries, 0, 1)
		next.DroppedSummaries++
	}
}

// ProjectAttemptHistory combines archived observations with current retained
// events for the same attempts. It does not manufacture missing start times or
// alter the durable archive's retention boundary.
func ProjectAttemptHistory(history AttemptHistory, events []TaskEventRecord) (AttemptHistory, error) {
	if err := ValidateAttemptHistory(history); err != nil {
		return AttemptHistory{}, err
	}
	result := CloneAttemptHistory(history)
	for _, event := range events {
		if event.Identity.TaskID != history.TaskID || event.Sequence <= history.ThroughSequence ||
			validateTaskEventRecord(event) != nil {
			return AttemptHistory{}, recordcodec.CorruptRecord()
		}
		if err := observeAttemptEvent(&result, event, false); err != nil {
			return AttemptHistory{}, err
		}
	}
	return result, nil
}

func observeAttemptEvent(history *AttemptHistory, event TaskEventRecord, appendMissing bool) error {
	payload, err := taskprogress.DecodePayload(event.Payload)
	if err != nil {
		return err
	}
	index := -1
	for cursor := len(history.Summaries) - 1; cursor >= 0; cursor-- {
		prior := history.Summaries[cursor]
		if prior.Identity.AssignmentID != event.Identity.AssignmentID ||
			prior.Identity.Attempt != event.Identity.Attempt ||
			prior.Identity.StepID != event.Identity.StepID {
			continue
		}
		if event.Identity.ControllerOperation() {
			if prior.ControllerOperation.Stage != payload.ControllerOperation.Stage {
				continue
			}
			if event.State == TaskEventStateRunning || prior.State != TaskEventStateRunning {
				break // A native action occurrence is not another action's duration.
			}
		}
		index = cursor
		break
	}
	if index < 0 && !appendMissing {
		return nil
	}
	summary := AttemptSummary{Identity: event.Identity, FirstSequence: event.Sequence}
	if index >= 0 {
		summary = history.Summaries[index]
		history.Summaries = slices.Delete(history.Summaries, index, index+1)
	}
	if event.State == TaskEventStateRunning &&
		(summary.StartedAt == nil || event.ReceivedAt.Before(*summary.StartedAt)) {
		started := event.ReceivedAt
		summary.StartedAt = &started
	}
	if index < 0 || event.Identity.Ordinal >= summary.Identity.Ordinal {
		summary.Identity, summary.State = event.Identity, event.State
		summary.ControllerOperation = taskprogress.CloneControllerOperation(payload.ControllerOperation)
		summary.Failure = taskprogress.CloneFailure(payload.Failure)
		summary.FinishedAt = nil
		if event.State == TaskEventStateCompleted || event.State == TaskEventStateFailed ||
			event.State == TaskEventStateAborted || event.State == TaskEventStateTimedOut {
			finished := event.ReceivedAt
			summary.FinishedAt = &finished
		}
	}
	summary.LastSequence, summary.LastReceivedAt = event.Sequence, event.ReceivedAt
	history.Summaries = append(history.Summaries, summary)
	return nil
}

func CloneAttemptHistory(history AttemptHistory) AttemptHistory {
	cloned := history
	cloned.Summaries = slices.Clone(history.Summaries)
	for index := range cloned.Summaries {
		value := &cloned.Summaries[index]
		value.ControllerOperation = taskprogress.CloneControllerOperation(value.ControllerOperation)
		value.Failure = taskprogress.CloneFailure(value.Failure)
		if value.StartedAt != nil {
			started := *value.StartedAt
			value.StartedAt = &started
		}
		if value.FinishedAt != nil {
			finished := *value.FinishedAt
			value.FinishedAt = &finished
		}
	}
	return cloned
}
