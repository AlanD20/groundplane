package etcd

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"
)

const diagnosticCleanupCursorKey = "/v1/runtime/task-diagnostic-cleanup-cursor"

type diagnosticCleanupCursor struct {
	LastKey string `json:"last_key"`
}

// ExpireTaskDiagnostics only removes disposable, terminal diagnostics. The
// immutable Task and its receipt hash, replay marker, execution plans and
// captured configuration are not rewritten or retired here.
func (repository *TaskRepository) ExpireTaskDiagnostics(
	ctx context.Context,
	now time.Time,
	retention time.Duration,
	remaining int,
) (count int, resultErr error) {
	if err := keyvalue.ValidateContext(ctx); err != nil {
		return 0, err
	}
	if retention < time.Hour || retention > taskjournal.TaskRetention || remaining < 1 {
		return 0, recordcodec.CorruptRecord()
	}
	cursorRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{diagnosticCleanupCursorKey}},
	)
	if err != nil {
		return 0, err
	}
	if cursorRead == nil || len(cursorRead.Values) != 1 {
		return 0, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(cursorRead.Values)
	cursor := diagnosticCleanupCursor{}
	if cursorRead.Values[0] != nil {
		cursor, err = recordcodec.Decode[diagnosticCleanupCursor](
			cursorRead.Values[0].Value,
			"task-diagnostic-cleanup-cursor",
		)
		if err != nil {
			return 0, err
		}
	}
	previousCursor := cursor.LastKey
	// A malformed/held Task must not starve later diagnostics. Advancing the
	// discovery cursor deletes no authority; the row is revisited next cycle.
	defer func() {
		if resultErr == nil || ctx.Err() != nil || cursor.LastKey == previousCursor {
			return
		}
		value, err := recordcodec.Encode("task-diagnostic-cleanup-cursor", cursor)
		if err != nil {
			resultErr = errors.Join(resultErr, err)
			return
		}
		defer clear(value)
		_, err = repository.store.Transact(
			ctx,
			[]keyvalue.Condition{
				{Key: diagnosticCleanupCursorKey, ModRevision: keyvalue.RevisionOf(cursorRead.Values[0])},
			},
			[]keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: diagnosticCleanupCursorKey, Value: value}},
		)
		resultErr = errors.Join(resultErr, err)
	}()
	page, err := repository.store.Range(
		ctx,
		keyvalue.RangeRequest{
			Prefix:         taskjournal.TaskRetentionIndexPrefix,
			StartExclusive: cursor.LastKey,
			Revision:       cursorRead.ReadRevision,
			Limit:          int64(min(8, remaining)),
		},
	)
	if err != nil {
		return 0, err
	}
	if page == nil || page.ReadRevision != cursorRead.ReadRevision {
		return 0, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearRangeValues(page.Values)
	conditions := []keyvalue.Condition{
		{Key: diagnosticCleanupCursorKey, ModRevision: keyvalue.RevisionOf(cursorRead.Values[0])},
	}
	mutations := []keyvalue.Mutation{}
	defer func() {
		for _, mutation := range mutations {
			clear(mutation.Value)
		}
	}()
	expired := 0
	for _, item := range page.Values {
		cursor.LastKey = item.Key
		taskID, retainUntil, err := taskjournal.ParseTaskRetentionIndexKey(item.Key)
		if err != nil {
			return 0, err
		}
		if retainUntil.After(now.Add(taskjournal.TaskRetention - retention)) {
			cursor.LastKey = ""
			break
		}
		read, err := repository.store.GetMany(
			ctx,
			keyvalue.GetManyRequest{
				Keys:     []string{taskjournal.TaskStorageKey(taskID), taskjournal.TaskDiagnosticExpiryKey(taskID)},
				Revision: page.ReadRevision,
			},
		)
		if err != nil {
			return 0, err
		}
		if read == nil || len(read.Values) != 2 || read.Values[0] == nil {
			return 0, recordcodec.CorruptRecord()
		}
		task, decodeErr := DecodeTaskRecord(read.Values[0].Value)
		taskRevision := read.Values[0].ModRevision
		alreadyExpired := read.Values[1] != nil
		keyvalue.ClearValues(read.Values)
		if decodeErr != nil {
			return 0, decodeErr
		}
		if alreadyExpired || !taskjournal.IsTerminalTaskStatus(task.Status) || task.FinishedAt == nil ||
			task.FinishedAt.Add(retention).After(now) {
			continue
		}
		held, err := repository.taskRetentionCandidateBlocked(ctx, taskID, retainUntil, page.ReadRevision)
		if err != nil {
			return 0, err
		}
		if held {
			continue
		}
		if held, err := repository.taskDiagnosticAuthorityHeld(ctx, task, taskRevision, page.ReadRevision); err != nil ||
			held {
			if err != nil {
				return 0, err
			}
			continue
		}
		expiry, err := taskjournal.EncodeDiagnosticExpiry(
			taskjournal.DiagnosticExpiry{
				TaskID:          taskID,
				TaskRevision:    taskRevision,
				ThroughSequence: task.NextEventSequence - 1,
				ExpiredAt:       now.UTC(),
			},
		)
		if err != nil {
			return 0, err
		}
		conditions = append(
			conditions,
			keyvalue.Condition{Key: taskjournal.TaskStorageKey(taskID), ModRevision: taskRevision},
			keyvalue.Condition{Key: taskjournal.TaskDiagnosticExpiryKey(taskID)},
		)
		conditions = append(
			conditions,
			keyvalue.Condition{Key: taskjournal.TaskAssignmentIndexKey(taskID)},
			keyvalue.Condition{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		)
		mutations = append(
			mutations,
			keyvalue.Mutation{
				Type:  keyvalue.MutationPut,
				Key:   taskjournal.TaskDiagnosticExpiryKey(taskID),
				Value: expiry,
			},
			keyvalue.Mutation{
				Type:   keyvalue.MutationDelete,
				Key:    taskjournal.TaskEventScopePrefix(taskID),
				Prefix: true,
			},
			keyvalue.Mutation{
				Type:   keyvalue.MutationDelete,
				Key:    taskjournal.TaskEventDedupScopePrefix(taskID),
				Prefix: true,
			},
			keyvalue.Mutation{
				Type:   keyvalue.MutationDelete,
				Key:    taskjournal.TaskAttemptSummaryPrefix(taskID),
				Prefix: true,
			},
			keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: taskjournal.TaskAttemptHistoryKey(taskID)},
		)
		expired++
	}
	if !page.More {
		cursor.LastKey = ""
	}
	if expired == 0 && cursor.LastKey == previousCursor {
		return 0, nil
	}
	cursorBytes, err := recordcodec.Encode("task-diagnostic-cleanup-cursor", cursor)
	if err != nil {
		return 0, err
	}
	mutations = append(
		mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: diagnosticCleanupCursorKey, Value: cursorBytes},
	)
	defer func() {
		for _, mutation := range mutations {
			clear(mutation.Value)
		}
	}()
	result, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return 0, err
	}
	if !result.Succeeded {
		return 0, nil
	}
	return expired, nil
}

func (repository *TaskRepository) taskDiagnosticAuthorityHeld(
	ctx context.Context,
	task TaskRecord,
	taskRevision, revision int64,
) (bool, error) {
	if task.Result != nil && task.Result.ReconciliationRequired {
		return true, nil
	}
	checks := []func() (bool, error){
		func() (bool, error) { return repository.backupHierarchyDeletionTaskPruneHeld(ctx, task, revision) },
		func() (bool, error) { return repository.backupRestoreTaskPruneHeld(ctx, task, taskRevision, revision) },
		func() (bool, error) {
			return repository.taskTerminalDeliveryPruneHeld(ctx, task, taskRevision, revision)
		},
		func() (bool, error) {
			return repository.taskStagingDeliveryPruneHeld(ctx, task, taskRevision, revision)
		},
	}
	for _, check := range checks {
		if held, err := check(); err != nil || held {
			return held, err
		}
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskjournal.TaskAssignmentIndexKey(task.ID), taskjournal.TaskActiveOperationKey(task.OperationID),
		taskClosingReportKey(
			task,
		), taskassignments.ReleaseRecoveryKey(task.ID),
		taskassignments.ServiceAbsenceRecoveryKey(
			task.ID,
		), taskassignments.ManagedNetworkRecoveryKey(task.ID), taskassignments.AttachRemovalRecoveryKey(task.ID),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || len(read.Values) != 7 {
		return false, recordcodec.CorruptRecord()
	}
	defer keyvalue.ClearValues(read.Values)
	for _, value := range read.Values {
		if value != nil {
			return true, nil
		}
	}
	return false, nil
}
