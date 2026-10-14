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
	history, cursorRevision, err := repository.readAttemptArchive(ctx, task.ID, revision)
	if err != nil {
		return nil, nil, err
	}
	next, validation, err := taskjournal.AppendEvictedAttemptEvent(history, event)
	clear(validation) // Validate the bounded projection; never persist a whole archive.
	if err != nil {
		return nil, nil, err
	}
	cursor, err := taskjournal.EncodeAttemptArchiveCursor(taskjournal.AttemptArchiveCursor{
		TaskID: task.ID, ThroughSequence: next.ThroughSequence, DroppedSummaries: next.DroppedSummaries,
		SummaryCount: len(next.Summaries),
	})
	if err != nil {
		return nil, nil, err
	}
	key := taskjournal.TaskAttemptHistoryKey(task.ID)
	mutations := []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: cursor}}
	previous := make(map[uint64]bool, len(history.Summaries))
	retained := make(map[uint64]bool, len(next.Summaries))
	for _, summary := range history.Summaries {
		previous[summary.LastSequence] = true
	}
	for _, summary := range next.Summaries {
		retained[summary.LastSequence] = true
		if previous[summary.LastSequence] {
			continue
		}
		value, err := taskjournal.EncodeAttemptSummary(summary)
		if err != nil {
			for _, mutation := range mutations {
				clear(mutation.Value)
			}
			return nil, nil, err
		}
		mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationPut,
			Key: taskjournal.TaskAttemptSummaryKey(task.ID, summary.LastSequence), Value: value})
	}
	for _, summary := range history.Summaries {
		if !retained[summary.LastSequence] {
			mutations = append(mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete,
				Key: taskjournal.TaskAttemptSummaryKey(task.ID, summary.LastSequence)})
		}
	}
	return []keyvalue.Condition{{Key: key, ModRevision: cursorRevision}}, mutations, nil
}

func (repository *TaskRepository) GetTaskAttemptHistory(ctx context.Context,
	task TaskRecord, snapshot TaskEventSnapshot,
) (*taskjournal.AttemptHistory, error) {
	if snapshot.DiagnosticsExpiredAt != nil {
		return nil, nil
	}
	if snapshot.Revision <= 0 || snapshot.Task.ID != task.ID ||
		snapshot.Task.NextEventSequence != task.NextEventSequence {
		return nil, recordcodec.CorruptRecord()
	}
	history, cursorRevision, err := repository.readAttemptArchive(ctx, task.ID, snapshot.Revision)
	if err != nil {
		return nil, err
	}
	trimmed := firstTaskEventSequence(task) - 1
	if cursorRevision == 0 {
		if trimmed != 0 {
			return nil, recordcodec.CorruptRecord()
		}
		return nil, nil
	}
	if history.TaskID != task.ID || history.ThroughSequence != trimmed {
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

func (repository *TaskRepository) readAttemptArchive(
	ctx context.Context, taskID string, revision int64,
) (taskjournal.AttemptHistory, int64, error) {
	history := taskjournal.AttemptHistory{TaskID: taskID}
	key := taskjournal.TaskAttemptHistoryKey(taskID)
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{key}, Revision: revision})
	if err != nil {
		return history, 0, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 {
		return history, 0, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return history, 0, nil
	}
	cursor, err := taskjournal.DecodeAttemptArchiveCursor(read.Values[0].Value)
	if err != nil || cursor.TaskID != taskID {
		return history, 0, recordcodec.CorruptRecord()
	}
	history.ThroughSequence, history.DroppedSummaries = cursor.ThroughSequence, cursor.DroppedSummaries
	start := ""
	for {
		page, err := repository.store.Range(ctx, keyvalue.RangeRequest{
			Prefix: taskjournal.TaskAttemptSummaryPrefix(taskID), StartExclusive: start, Revision: revision, Limit: 64,
		})
		if err != nil {
			return history, 0, err
		}
		if page == nil || page.ReadRevision != revision || page.More && len(page.Values) == 0 {
			return history, 0, recordcodec.CorruptRecord()
		}
		for _, item := range page.Values {
			summary, err := taskjournal.DecodeAttemptSummary(item.Value)
			if err != nil || summary.Identity.TaskID != taskID ||
				item.Key != taskjournal.TaskAttemptSummaryKey(taskID, summary.LastSequence) ||
				len(history.Summaries) >= cursor.SummaryCount {
				keyvalue.ClearRangeValues(page.Values)
				return history, 0, recordcodec.CorruptRecord()
			}
			history.Summaries = append(history.Summaries, summary)
			start = item.Key
		}
		keyvalue.ClearRangeValues(page.Values)
		if !page.More {
			break
		}
	}
	if len(history.Summaries) != cursor.SummaryCount || taskjournal.ValidateAttemptHistory(history) != nil {
		return history, 0, recordcodec.CorruptRecord()
	}
	return history, read.Values[0].ModRevision, nil
}
