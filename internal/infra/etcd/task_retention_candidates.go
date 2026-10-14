package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintretention"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptexecutions "github.com/AlanD20/groundplane/internal/infra/etcd/scriptexecutions"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	sourceref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"time"
)

// nextTaskRetentionPruneCandidate leaves retained Script and Blueprint child
// indexes intact while advancing discovery past attempts still owned by a
// live claim or newer retry. Reads stay bounded and use one MVCC view; no
// durable deadline or discovery key is moved.
func (repository *TaskRepository) nextTaskRetentionPruneCandidate(
	ctx context.Context, now time.Time,
) (*etcdstore.RangeResult, error) {
	scan, err := repository.readTaskCleanupPage(ctx, taskRetentionScanKey, taskjournal.TaskRetentionIndexPrefix, 8)
	if err != nil {
		return nil, err
	}
	if len(scan.page.Values) == 0 && scan.after != "" {
		if err := repository.advanceTaskCleanupPage(ctx, scan, ""); err != nil {
			return nil, err
		}
		scan, err = repository.readTaskCleanupPage(ctx, taskRetentionScanKey, taskjournal.TaskRetentionIndexPrefix, 8)
		if err != nil {
			return nil, err
		}
	}
	page := scan.page
	for index, entry := range page.Values {
		taskID, deadline, err := taskjournal.ParseTaskRetentionIndexKey(entry.Key)
		if err != nil {
			etcdstore.ClearRangeValues(page.Values)
			if advanceErr := repository.advanceTaskCleanupPage(ctx, scan, entry.Key); advanceErr != nil {
				return nil, advanceErr
			}
			return nil, err
		}
		if deadline.After(now) {
			etcdstore.ClearRangeValues(page.Values)
			if err := repository.advanceTaskCleanupPage(ctx, scan, ""); err != nil {
				return nil, err
			}
			return &etcdstore.RangeResult{ReadRevision: page.ReadRevision}, nil
		}
		blocked, err := repository.taskRetentionCandidateBlocked(ctx, taskID, deadline, page.ReadRevision)
		if err != nil {
			etcdstore.ClearRangeValues(page.Values)
			if advanceErr := repository.advanceTaskCleanupPage(ctx, scan, entry.Key); advanceErr != nil {
				return nil, advanceErr
			}
			return nil, err
		}
		if !blocked {
			if err := repository.advanceTaskCleanupPage(ctx, scan, entry.Key); err != nil {
				etcdstore.ClearRangeValues(page.Values)
				return nil, err
			}
			etcdstore.ClearRangeValues(page.Values[:index])
			etcdstore.ClearRangeValues(page.Values[index+1:])
			return &etcdstore.RangeResult{ReadRevision: page.ReadRevision, Values: []etcdstore.KeyValue{entry}}, nil
		}
		clear(entry.Value)
	}
	after := ""
	if page.More {
		after = page.Values[len(page.Values)-1].Key
	}
	if err := repository.advanceTaskCleanupPage(ctx, scan, after); err != nil {
		return nil, err
	}
	return &etcdstore.RangeResult{ReadRevision: page.ReadRevision}, nil
}

func (repository *TaskRepository) taskRetentionCandidateBlocked(
	ctx context.Context, taskID string, deadline time.Time, revision int64,
) (bool, error) {
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision},
	)
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return false, taskjournal.CorruptPruneIntent()
	}
	defer etcdstore.ClearValues(read.Values)
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != taskID || !taskjournal.IsTerminalTaskStatus(task.Status) || task.RetainUntil == nil ||
		!task.RetainUntil.Equal(deadline) {
		return false, taskjournal.CorruptPruneIntent()
	}
	if held, err := repository.blueprintChildPruneHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, _, err := repository.releaseNetworkPruneAuthority(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, _, err := repository.resolverTaskPruneAuthority(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, err := repository.connectorCheckRetentionHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, err := repository.backingCheckRetentionHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, err := repository.dnsCheckRetentionHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, err := repository.edgeCheckRetentionHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if held, err := repository.runnerCheckRetentionHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if taskOwnsBlueprintRevision(task) {
		_, held, err := blueprintretention.Guards(ctx, repository.store, task.Owner.EnvironmentID, task.ID, revision)
		if held || err != nil {
			return held, err
		}
	}
	if held, err := repository.hierarchyDeletionChildPruneHeld(ctx, task, revision); held || err != nil {
		return held, err
	}
	if task.Type != taskjournal.TaskScript {
		return false, nil
	}
	sources, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		scriptsourceevidence.ScriptSourceRootKey(
			task.OperationID,
		), scriptexecutions.ScriptExecutionKey(task.Params[scriptexecutions.ScriptExecutionIDParam]),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if sources == nil || sources.ReadRevision != revision || len(sources.Values) != 2 {
		return false, taskjournal.CorruptPruneIntent()
	}
	defer etcdstore.ClearValues(sources.Values)
	if sources.Values[0] == nil {
		return false, nil
	}
	if sources.Values[1] == nil {
		return false, taskjournal.CorruptPruneIntent()
	}
	root, err := scriptsourceevidence.DecodeScriptOperationSourceRoot(sources.Values[0].Value)
	if err != nil {
		return false, err
	}
	execution, err := recordcodec.Decode[scriptexecutions.ScriptExecutionRecord](
		sources.Values[1].Value,
		"script-execution",
	)
	if err != nil || scriptexecutions.ValidateScriptExecutionRecord(execution) != nil ||
		!taskOwnsScriptExecution(task, execution) ||
		!manualScriptRootMatches(
			execution,
			root,
		) ||
		execution.PlanHash != task.PlanHash ||
		execution.OperationID != task.OperationID {
		return false, taskjournal.CorruptPruneIntent()
	}
	return execution.CurrentTaskID != task.ID || (root.Phase == scriptsourceevidence.ScriptOperationSourceActive &&
		(root.RetryDisposition == sourceref.RetryDispositionUndecided || root.RetryDisposition == sourceref.RetryDispositionTransferred)), nil
}
