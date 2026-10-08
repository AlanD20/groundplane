package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionretention"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	scriptsourceevidence "github.com/AlanD20/groundplane/internal/infra/etcd/scriptsourceevidence"
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
	if held, _, err := repository.softwareChildPruneAuthority(ctx, task, readRevision); held || err != nil {
		return held, err
	}
	if held, err := repository.hierarchyDeletionChildPruneHeld(ctx, task, readRevision); held || err != nil {
		return held, err
	}
	if held, err := repository.backupHierarchyDeletionTaskPruneHeld(ctx, task, readRevision); held || err != nil {
		return held, err
	}
	if held, err := repository.backupRestoreTaskPruneHeld(ctx, task, taskRevision, readRevision); held || err != nil {
		return held, err
	}
	if held, err := repository.taskTerminalDeliveryPruneHeld(ctx, task, taskRevision, readRevision); held ||
		err != nil {
		return held, err
	}
	if held, err := repository.taskStagingDeliveryPruneHeld(ctx, task, taskRevision, readRevision); held || err != nil {
		return held, err
	}
	if held, _, err := repository.taskOrphanCleanupPruneConditions(ctx, task, readRevision); held || err != nil {
		return held, err
	}
	if held, err := repository.blueprintChildPruneHeld(ctx, task, readRevision); held || err != nil {
		return held, err
	}
	if stop, err := repository.pruneParentOwnedBlueprintAttachInput(
		ctx, task, taskRevision, retentionEntry, readRevision,
	); stop || err != nil {
		return stop, err
	}
	if stop, err := repository.prepareRecoverySecretPinExpiry(ctx, task, taskRevision, retentionEntry, now); stop ||
		err != nil {
		return stop, err
	}
	if stop, err := repository.pruneChildOwnedBlueprintAttachInput(
		ctx, task, taskRevision, retentionEntry, readRevision,
	); stop || err != nil {
		return stop, err
	}
	if stop, err := repository.pruneChildOwnedBlueprintAttachOutput(
		ctx, task, taskRevision, retentionEntry, readRevision,
	); stop || err != nil {
		return stop, err
	}
	if task.Type == taskjournal.TaskScript {
		return repository.prepareManualScriptExpiry(ctx, task, taskRevision, retentionEntry, readRevision, now)
	}
	if task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceHierarchyDeletion ||
		task.Params[taskjournal.TaskHierarchyDeletionParentParam] != "" {
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
	pins = append(pins, hierarchydeletionretention.ChildRetiredConditions(
		task.Params[taskjournal.TaskHierarchyDeletionParentParam],
	)...)
	if task.Configuration != nil && task.Configuration.BackingHookInputs != nil {
		pins = append(pins, etcdstore.Condition{Key: taskconfiguration.BackingHookTaskInputKey(task.OperationID)})
	}
	if task.Type != taskjournal.TaskScript {
		return pins
	}
	return append(pins, []etcdstore.Condition{
		{Key: scriptsourceevidence.ScriptSourceRootKey(task.OperationID)},
		{Key: ref.ReversePrefix(task.OperationID), Prefix: true},
	}...)
}

func taskPruneConflict(err error) bool {
	kind, ok := errs.KindOf(err)
	return ok && kind == errs.KindStateConflict
}
