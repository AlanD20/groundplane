package executiondiagnostics

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumEvents = 64

// Event is one safe stage observation, not an execution command or checkpoint.
// Sequence is local to an attempt; reconnect must keep the attempt identity.
type Event struct {
	Sequence   uint64              `json:"sequence"`
	RecordedAt time.Time           `json:"recorded_at"`
	Stage      workexecution.Stage `json:"stage"`
}

type eventRecord struct {
	TaskID     string  `json:"task_id"`
	WorkItemID string  `json:"work_item_id"`
	AttemptID  string  `json:"attempt_id"`
	Items      []Event `json:"items"`
}

func EventsKey(attemptID string) string { return "/v1/diagnostics/execution-events/" + attemptID }

type Events struct {
	FinishedAt *time.Time
	Outcome    workexecution.Outcome
	Items      []Event
	First      uint64
	Last       uint64
	Expired    bool
	Incomplete bool
	Revision   int64
}

// Events returns retained observations at the same revision as membership and
// stage state. An expired cursor is explicit; callers must not silently bridge
// a missing interval or interpret it as an execution failure.
func (repository *Repository) Events(
	ctx context.Context,
	taskID, workID, attemptID string,
	after uint64,
	revision int64,
) (Events, error) {
	snapshot, err := repository.Read(ctx, taskID, workID, attemptID, revision)
	if err != nil {
		return Events{}, err
	}
	record, _, err := repository.readEvents(ctx, taskID, workID, attemptID, snapshot.ReadRevision)
	if err != nil {
		return Events{}, err
	}
	result := Events{
		Items:      []Event{},
		Incomplete: snapshot.Incomplete,
		Expired:    snapshot.Expired,
		Revision:   snapshot.ReadRevision,
		FinishedAt: snapshot.FinishedAt,
		Outcome:    snapshot.Outcome,
	}
	if record == nil {
		result.Expired = result.Expired || after != 0
		result.Incomplete = result.Incomplete || snapshot.Record != nil
		return result, nil
	}
	result.First = record.Items[0].Sequence
	result.Last = record.Items[len(record.Items)-1].Sequence
	if snapshot.Record == nil || result.Last != snapshot.Record.Sequence {
		return Events{}, invalid()
	}
	if after > result.Last {
		return Events{}, errs.New(errs.KindValidationFailed, "event cursor is ahead of this attempt")
	}
	result.Expired = result.Expired || after != 0 && after < result.First-1
	for _, event := range record.Items {
		if event.Sequence > after {
			result.Items = append(result.Items, event)
		}
	}
	return result, nil
}

func (repository *Repository) readEvents(
	ctx context.Context,
	taskID, workID, attemptID string,
	revision int64,
) (*eventRecord, int64, error) {
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{EventsKey(attemptID)}, Revision: revision},
	)
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
	record, err := recordcodec.Decode[eventRecord](read.Values[0].Value, "execution_events")
	if err != nil {
		return nil, 0, err
	}
	if record.TaskID != taskID || record.WorkItemID != workID || record.AttemptID != attemptID ||
		len(record.Items) < 1 ||
		len(record.Items) > maximumEvents {
		return nil, 0, invalid()
	}
	for index, event := range record.Items {
		if event.Sequence == 0 || !recordcodec.IsCanonicalUTC(event.RecordedAt) || event.Stage.Validate() != nil {
			return nil, 0, invalid()
		}
		if event.Stage.StartedAt != nil && event.Stage.StartedAt.After(event.RecordedAt) ||
			event.Stage.FinishedAt != nil && event.Stage.FinishedAt.After(event.RecordedAt) {
			return nil, 0, invalid()
		}
		if index > 0 &&
			(event.Sequence-1 != record.Items[index-1].Sequence || event.RecordedAt.Before(record.Items[index-1].RecordedAt)) {
			return nil, 0, invalid()
		}
	}
	return &record, read.Values[0].ModRevision, nil
}

func (repository *Repository) prepareEvent(
	ctx context.Context,
	next Record,
	stage workexecution.Stage,
	revision int64,
) (keyvalue.Condition, keyvalue.Mutation, error) {
	previous, modRevision, err := repository.readEvents(ctx, next.TaskID, next.WorkItemID, next.AttemptID, revision)
	if err != nil {
		return keyvalue.Condition{}, keyvalue.Mutation{}, err
	}
	record := eventRecord{TaskID: next.TaskID, WorkItemID: next.WorkItemID, AttemptID: next.AttemptID}
	if previous != nil {
		if previous.Items[len(previous.Items)-1].Sequence != next.Sequence-1 {
			return keyvalue.Condition{}, keyvalue.Mutation{}, invalid()
		}
		record.Items = previous.Items
	} else if next.Sequence != 1 {
		return keyvalue.Condition{}, keyvalue.Mutation{}, invalid()
	}
	record.Items = append(record.Items, Event{Sequence: next.Sequence, RecordedAt: next.UpdatedAt, Stage: stage})
	if len(record.Items) > maximumEvents {
		record.Items = record.Items[len(record.Items)-maximumEvents:]
	}
	for {
		value, err := recordcodec.Encode("execution_events", record)
		if err != nil {
			return keyvalue.Condition{}, keyvalue.Mutation{}, err
		}
		if len(value) <= maximumBytes {
			return keyvalue.Condition{
					Key:         EventsKey(next.AttemptID),
					ModRevision: modRevision,
				}, keyvalue.Mutation{
					Type:  keyvalue.MutationPut,
					Key:   EventsKey(next.AttemptID),
					Value: value,
				}, nil
		}
		clear(value)
		if len(record.Items) == 1 {
			return keyvalue.Condition{}, keyvalue.Mutation{}, invalid()
		}
		record.Items = record.Items[1:]
	}
}
