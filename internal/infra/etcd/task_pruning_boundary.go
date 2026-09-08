package etcd

import (
	"context"
	"time"

	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareTaskPruneBoundary(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retentionEntry KeyValue,
	readRevision int64,
	now time.Time,
) (bool, error) {
	if task.Type == TaskScript {
		return repository.prepareManualScriptExpiry(ctx, task, taskRevision, retentionEntry, readRevision, now)
	}
	if task.Params[TaskResourceKindParam] != TaskResourceHierarchyDeletion {
		return false, nil
	}
	changed, ready, err := repository.prepareHierarchyDeletionTaskPrune(
		ctx, task, taskRevision, retentionEntry, readRevision, now,
	)
	if err != nil {
		return false, err
	}
	return changed || !ready, nil
}

func scriptTaskPruneConditions(task TaskRecord) []Condition {
	if task.Type != TaskScript {
		return nil
	}
	return []Condition{
		{Key: scriptSourceRootKey(task.OperationID)},
		{Key: ref.ReversePrefix(task.OperationID), Prefix: true},
	}
}

func taskPruneConflict(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}

func corruptTaskPruneIntent() error {
	return errs.New(errs.KindInternal, "task prune state is corrupt")
}
