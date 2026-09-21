package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"time"

	ref "github.com/AlanD20/groundplane/internal/infra/scriptsourcereference"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareTaskPruneBoundary(
	ctx context.Context,
	task TaskRecord,
	taskRevision int64,
	retentionEntry etcdstore.KeyValue,
	readRevision int64,
	now time.Time,
) (bool, error) {
	if stop, err := repository.prepareRecoverySecretPinExpiry(ctx, task, taskRevision, retentionEntry, now); stop ||
		err != nil {
		return stop, err
	}
	if task.Type == taskjournal.TaskScript {
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

func taskSourcePruneConditions(task TaskRecord) []etcdstore.Condition {
	pins := recoverySecretPinPruneConditions(task)
	if task.Configuration != nil && task.Configuration.BackingHookInputs != nil {
		pins = append(pins, etcdstore.Condition{Key: taskconfiguration.BackingHookTaskInputKey(task.OperationID)})
	}
	if task.Type != taskjournal.TaskScript {
		return pins
	}
	return append(pins, []etcdstore.Condition{
		{Key: scriptSourceRootKey(task.OperationID)},
		{Key: ref.ReversePrefix(task.OperationID), Prefix: true},
	}...)
}

func taskPruneConflict(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}

func corruptTaskPruneIntent() error {
	return errs.New(errs.KindInternal, "task prune state is corrupt")
}
