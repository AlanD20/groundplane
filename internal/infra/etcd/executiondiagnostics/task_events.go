package executiondiagnostics

import (
	"context"
	"math"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// TaskEvent orders observations from parallel work without turning diagnostics
// into execution authority. AttemptSequence retains the producer replay identity.
type TaskEvent struct {
	Sequence        uint64              `json:"sequence"`
	WorkItemID      string              `json:"work_item_id"`
	AttemptID       string              `json:"attempt_id"`
	AttemptSequence uint64              `json:"attempt_sequence"`
	RecordedAt      time.Time           `json:"recorded_at"`
	Stage           workexecution.Stage `json:"stage"`
}

type taskEventsRecord struct {
	TaskID       string      `json:"task_id"`
	Items        []TaskEvent `json:"items"`
	LastSequence uint64      `json:"last_sequence"`
	Expired      bool        `json:"expired"`
}

type TaskEvents struct {
	Items       []TaskEvent
	First, Last uint64
	Finished    bool
	Expired     bool
	Revision    int64
}

func TaskEventsKey(taskID string) string { return "/v1/diagnostics/task-events/" + taskID }

func (repository *Repository) TaskEvents(ctx context.Context, taskID string, after uint64) (TaskEvents, error) {
	task, revision, err := repository.queue.ReadTask(ctx, taskID, 0)
	if err != nil {
		return TaskEvents{}, err
	}
	record, _, err := repository.readTaskEvents(ctx, taskID, revision)
	if err != nil {
		return TaskEvents{}, err
	}
	result := TaskEvents{Items: []TaskEvent{}, Finished: task.Task.FinishedAt != nil, Revision: revision}
	if record == nil {
		result.Expired = after != 0
		return result, nil
	}
	result.Last = record.LastSequence
	if len(record.Items) != 0 {
		result.First = record.Items[0].Sequence
	}
	if after > result.Last {
		return TaskEvents{}, errs.New(errs.KindValidationFailed, "event cursor is ahead of this Task")
	}
	result.Expired = after != 0 && (len(record.Items) == 0 && after < result.Last || result.First != 0 && after < result.First-1)
	for _, event := range record.Items {
		if event.Sequence > after {
			result.Items = append(result.Items, event)
		}
	}
	return result, nil
}

func (repository *Repository) readTaskEvents(ctx context.Context, taskID string, revision int64) (*taskEventsRecord, int64, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{TaskEventsKey(taskID)}, Revision: revision})
	if err != nil {
		return nil, 0, err
	}
	if read == nil {
		return nil, 0, invalid()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, 0, invalid()
	}
	if read.Values[0] == nil {
		return nil, 0, nil
	}
	if len(read.Values[0].Value) > maximumBytes {
		return nil, 0, invalid()
	}
	record, err := recordcodec.Decode[taskEventsRecord](read.Values[0].Value, "task_execution_events")
	if err != nil {
		return nil, 0, err
	}
	if record.TaskID != taskID || record.LastSequence == 0 || len(record.Items) > maximumEvents ||
		len(record.Items) == 0 && !record.Expired || len(record.Items) != 0 && record.Items[len(record.Items)-1].Sequence != record.LastSequence {
		return nil, 0, invalid()
	}
	for index, event := range record.Items {
		if event.Sequence == 0 || event.AttemptSequence == 0 || ids.Validate(ids.KindWorkItem, event.WorkItemID) != nil ||
			ids.Validate(ids.KindAttempt, event.AttemptID) != nil || !recordcodec.IsCanonicalUTC(event.RecordedAt) || event.Stage.Validate() != nil {
			return nil, 0, invalid()
		}
		// Ordering is transaction sequence, not executor wall-clock ordering.
		if index > 0 && event.Sequence-1 != record.Items[index-1].Sequence {
			return nil, 0, invalid()
		}
	}
	return &record, read.Values[0].ModRevision, nil
}

func (repository *Repository) prepareTaskEvent(ctx context.Context, next Record, stage workexecution.Stage, revision int64) (keyvalue.Condition, keyvalue.Mutation, error) {
	previous, modRevision, err := repository.readTaskEvents(ctx, next.TaskID, revision)
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	record := taskEventsRecord{TaskID: next.TaskID}
	sequence := uint64(1)
	if previous != nil {
		record.Items = previous.Items
		record.Expired = previous.Expired
		last := previous.LastSequence
		if last == math.MaxUint64 {
			return keyvalue.Condition{}, keyvalue.Mutation{}, invalid()
		}
		sequence = last + 1
	}
	record.LastSequence = sequence
	record.Items = append(record.Items, TaskEvent{Sequence: sequence, WorkItemID: next.WorkItemID,
		AttemptID: next.AttemptID, AttemptSequence: next.Sequence, RecordedAt: next.UpdatedAt, Stage: stage})
	if len(record.Items) > maximumEvents {
		record.Items = record.Items[len(record.Items)-maximumEvents:]
		record.Expired = true
	}
	for {
		value, err := recordcodec.Encode("task_execution_events", record)
		if err != nil {
			return keyvalue.Condition{}, keyvalue.Mutation{}, err
		}
		if len(value) <= maximumBytes {
			return keyvalue.Condition{Key: TaskEventsKey(next.TaskID), ModRevision: modRevision}, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: TaskEventsKey(next.TaskID), Value: value}, nil
		}
		clear(value)
		if len(record.Items) == 1 {
			return keyvalue.Condition{}, keyvalue.Mutation{}, invalid()
		}
		record.Items = record.Items[1:]
		record.Expired = true
	}
}
