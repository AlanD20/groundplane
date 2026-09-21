package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"time"
)

// nextTaskRetentionPruneCandidate leaves retained Script indexes intact while
// advancing discovery past attempts owned by a live or newer retry. Reads stay
// bounded and use one MVCC view; no durable deadline or discovery key is moved.
func (repository *TaskRepository) nextTaskRetentionPruneCandidate(
	ctx context.Context, now time.Time,
) (*etcdstore.RangeResult, error) {
	request := etcdstore.RangeRequest{Prefix: taskjournal.TaskRetentionIndexPrefix, Limit: 1}
	for {
		page, err := repository.store.Range(ctx, request)
		if err != nil {
			return nil, err
		}
		if page == nil || page.ReadRevision <= 0 || len(page.Values) > 1 {
			return nil, corruptTaskPruneIntent()
		}
		if len(page.Values) == 0 {
			return page, nil
		}
		entry := page.Values[0]
		taskID, deadline, err := taskjournal.ParseTaskRetentionIndexKey(entry.Key)
		if err != nil {
			clear(entry.Value)
			return nil, err
		}
		if deadline.After(now) {
			return page, nil
		}
		blocked, err := repository.manualScriptRetentionCandidateBlocked(ctx, taskID, deadline, page.ReadRevision)
		if err != nil {
			clear(entry.Value)
			return nil, err
		}
		if !blocked {
			return page, nil
		}
		request.StartExclusive, request.Revision = entry.Key, page.ReadRevision
		clear(entry.Value)
	}
}

func (repository *TaskRepository) manualScriptRetentionCandidateBlocked(
	ctx context.Context, taskID string, deadline time.Time, revision int64,
) (bool, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, corruptTaskPruneIntent()
	}
	defer etcdstore.ClearValues(read.Values)
	task, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != taskID || !taskjournal.IsTerminalTaskStatus(task.Status) || task.RetainUntil == nil ||
		!task.RetainUntil.Equal(deadline) {
		return false, corruptTaskPruneIntent()
	}
	if task.Type != taskjournal.TaskScript {
		return false, nil
	}
	sources, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		scriptSourceRootKey(task.OperationID), scriptexecutions.ScriptExecutionKey(task.Params[scriptexecutions.ScriptExecutionIDParam]),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if sources == nil || sources.ReadRevision != revision || len(sources.Values) != 2 {
		return false, corruptTaskPruneIntent()
	}
	defer etcdstore.ClearValues(sources.Values)
	if sources.Values[0] == nil {
		return false, nil
	}
	if sources.Values[1] == nil {
		return false, corruptTaskPruneIntent()
	}
	root, err := decodeScriptOperationSourceRoot(sources.Values[0].Value)
	if err != nil {
		return false, err
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](sources.Values[1].Value, "script-execution")
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil || !taskOwnsScriptExecution(task, execution) ||
		!manualScriptRootMatches(
			execution,
			root,
		) || execution.PlanHash != task.PlanHash || execution.OperationID != task.OperationID {
		return false, corruptTaskPruneIntent()
	}
	return execution.CurrentTaskID != task.ID || (root.Phase == ScriptOperationSourceActive &&
		(root.RetryDisposition == sourceref.RetryDispositionUndecided || root.RetryDisposition == sourceref.RetryDispositionTransferred)), nil
}
