package backup

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Snapshot writes use captured native work and resource ownership at one MVCC
// revision. They never reconstruct authority from diagnostic Task history.
func NewConfigSnapshotWriteGuard(store keyvalue.Store) (backupconfiguration.CaptureSnapshotAuthorityGuard, error) {
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return nil, err
	}
	inputs, err := backupwork.NewInputRepository(store)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, input backupconfiguration.CaptureSnapshotWriteAuthority, revision int64) ([]keyvalue.Condition, error) {
		if revision <= 0 || ids.Validate(ids.KindTask, input.TaskID) != nil ||
			input.SnapshotID != input.TaskID || ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
			ids.Validate(
				ids.KindBackupSource,
				input.SourceID,
			) != nil || input.ReadRevision <= 0 || input.Expected == nil {
			return nil, configSnapshotGuardConflict()
		}
		page, err := queue.ListWork(ctx, input.TaskID, "", 1, revision)
		if err != nil {
			return nil, err
		}
		if page.More || len(page.Items) != 1 {
			return nil, configSnapshotGuardConflict()
		}
		view, err := queue.ReadWork(ctx, input.TaskID, page.Items[0].Item.ID, revision)
		if err != nil {
			return nil, err
		}
		task, item := view.Task.Task, view.Item.Item
		now := time.Now().UTC()
		if task.Action != workexecution.ActionBackup || task.ExpectedItems != 1 || !task.PublicationComplete ||
			task.FinishedAt != nil || task.CollectionStartedAt != nil || task.Stop != nil ||
			task.Scope.EnvironmentID != input.EnvironmentID || item.Operation != workexecution.Backup ||
			item.ForwardRevoked || item.Result != nil || !now.Before(task.Deadline) || !now.Before(item.Deadline) {
			return nil, configSnapshotGuardConflict()
		}
		captured, change, err := inputs.PrepareAdmission(ctx, view)
		if err != nil {
			return nil, err
		}
		run, runGuard, err := inputs.LoadStagingCapture(ctx, backupwork.StagingSource{View: view, Input: captured})
		if err != nil {
			return nil, err
		}
		if run.State != backupruntime.BackupRunQueued && run.State != backupruntime.BackupRunRunning {
			return nil, configSnapshotGuardConflict()
		}
		found := false
		for ordinal, source := range run.Sources {
			if source.SourceID != input.SourceID {
				continue
			}
			if found || source.Kind != backupruntime.BackupRuntimeSourceConfig || source.Snapshot.Config == nil ||
				source.Snapshot.Config.ConfigSnapshotID != input.SnapshotID || source.Snapshot.Config.ReadRevision != input.ReadRevision {
				return nil, configSnapshotGuardConflict()
			}
			step := captured.Steps[ordinal]
			if !configSnapshotSameAuthority(step.GetCapture().GetConfig(), input.Expected) ||
				step.StepDeadlineUnixNano <= uint64(now.UnixNano()) {
				return nil, configSnapshotGuardConflict()
			}
			found = true
		}
		if !found {
			return nil, configSnapshotGuardConflict()
		}
		return append(change.Conditions, runGuard,
			keyvalue.Condition{Key: workqueue.TaskKey(task.ID), ModRevision: view.Task.Revision},
			keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: view.Item.Revision}), nil
	}, nil
}

func configSnapshotGuardConflict() error {
	return errs.New(errs.KindStateConflict, "Config snapshot captured work or Environment ownership changed")
}
