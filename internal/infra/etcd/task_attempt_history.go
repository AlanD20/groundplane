package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
)

func (repository *TaskRepository) prepareTrimmedAttemptHistory(ctx context.Context, task TaskRecord,
	event taskjournal.TaskEventRecord, revision int64,
) ([]keyvalue.Condition, []keyvalue.Mutation, error) {
	key := taskjournal.TaskAttemptHistoryKey(task.ID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return nil, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return nil, nil, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	history := taskjournal.AttemptHistory{TaskID: task.ID}
	if read.Values[0] != nil {
		if read.Values[0].Key != key {
			return nil, nil, recordcodec.CorruptRecord()
		}
		history, err = taskjournal.DecodeAttemptHistory(read.Values[0].Value)
		if err != nil {
			return nil, nil, err
		}
	}
	_, value, err := taskjournal.AppendEvictedAttemptEvent(history, event)
	if err != nil {
		return nil, nil, err
	}
	return []keyvalue.Condition{{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])}},
		[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: value}}, nil
}

func (repository *TaskRepository) GetTaskAttemptHistory(ctx context.Context,
	task TaskRecord, snapshot TaskEventSnapshot,
) (*taskjournal.AttemptHistory, error) {
	if snapshot.Revision <= 0 || snapshot.Task.ID != task.ID ||
		snapshot.Task.NextEventSequence != task.NextEventSequence {
		return nil, recordcodec.CorruptRecord()
	}
	key := taskjournal.TaskAttemptHistoryKey(task.ID)
	read, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{key}, Revision: snapshot.Revision},
	)
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != snapshot.Revision || len(read.Values) != 1 {
		return nil, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	trimmed := firstTaskEventSequence(task) - 1
	if read.Values[0] == nil {
		if trimmed != 0 {
			return nil, recordcodec.CorruptRecord()
		}
		return nil, nil
	}
	if read.Values[0].Key != key {
		return nil, recordcodec.CorruptRecord()
	}
	history, err := taskjournal.DecodeAttemptHistory(read.Values[0].Value)
	if err != nil || history.TaskID != task.ID || history.ThroughSequence != trimmed {
		return nil, recordcodec.CorruptRecord()
	}
	for _, summary := range history.Summaries {
		if !taskEventBelongsToTask(task, summary.Identity) || summary.LastReceivedAt.After(task.UpdatedAt) {
			return nil, recordcodec.CorruptRecord()
		}
	}
	projected, err := taskjournal.ProjectAttemptHistory(history, snapshot.Events)
	return &projected, err
}
