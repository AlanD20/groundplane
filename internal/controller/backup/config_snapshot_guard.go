package backup

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type configSnapshotLiveReader interface {
	ReadFixedKeys(context.Context, []string, int64) (*etcdstore.GetManyResult, error)
}

// NewConfigSnapshotWriteGuard uses existing strict Task/run/procedure codecs;
// it does not move cross-capability publication into the snapshot repository.
func NewConfigSnapshotWriteGuard(
	reader configSnapshotLiveReader,
) (backupconfiguration.CaptureSnapshotAuthorityGuard, error) {
	if reader == nil {
		return nil, errs.New(errs.KindInternal, "Config snapshot live authority reader is required")
	}
	return func(ctx context.Context, input backupconfiguration.CaptureSnapshotWriteAuthority, revision int64) ([]etcdstore.Condition, error) {
		if revision <= 0 || ids.Validate(ids.KindTask, input.TaskID) != nil ||
			ids.Validate(ids.KindTask, input.SnapshotID) != nil ||
			ids.Validate(ids.KindEnvironment, input.EnvironmentID) != nil ||
			ids.Validate(ids.KindBackupSource, input.SourceID) != nil ||
			input.ReadRevision <= 0 ||
			input.Expected == nil {
			return nil, configSnapshotGuardConflict()
		}
		keys := []string{taskjournal.TaskStorageKey(input.TaskID), backupruntime.BackupRunKey(input.TaskID),
			backupruntime.BackupExecutionPlanKey(
				input.TaskID,
			), hierarchy.EnvironmentOperationLockKey(input.EnvironmentID)}
		read, err := reader.ReadFixedKeys(ctx, keys, revision)
		if err != nil {
			return nil, err
		}
		if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
			return nil, configSnapshotGuardConflict()
		}
		defer etcdstore.ClearValues(read.Values)
		for index, value := range read.Values {
			if value == nil || value.Key != keys[index] || value.ModRevision <= 0 {
				return nil, configSnapshotGuardConflict()
			}
		}
		task, taskErr := etcd.DecodeTaskRecord(read.Values[0].Value)
		run, runErr := backupruntime.DecodeBackupRunRecord(read.Values[1].Value)
		plan, planErr := backupruntime.DecodeBackupExecutionPlan(read.Values[2].Value)
		lock, lockErr := backupruntime.DecodeBackupOperationLockRecord(read.Values[3].Value)
		if taskErr != nil || runErr != nil || planErr != nil || lockErr != nil || task.ID != input.TaskID ||
			etcd.ValidateBackupRunTaskBinding(
				task,
				run,
			) != nil || backupplanning.ValidateBackupRunExecutionPlan(run, plan) != nil ||
			(task.Status != taskjournal.TaskStatusPending && task.Status != taskjournal.TaskStatusRunning) ||
			(task.Status == taskjournal.TaskStatusPending && run.State != backupruntime.BackupRunQueued) ||
			(task.Status == taskjournal.TaskStatusRunning && run.State != backupruntime.BackupRunRunning) ||
			task.PlanID != plan.PlanId || task.PlanHash != hex.EncodeToString(plan.PlanHash) || uint64(task.RenderGeneration) != plan.RenderGeneration ||
			run.EnvironmentID != input.EnvironmentID || lock.EnvironmentID != input.EnvironmentID ||
			lock.TaskID != input.TaskID || lock.OperationID != task.OperationID || lock.Kind != backupruntime.BackupOperationBackup {
			return nil, configSnapshotGuardConflict()
		}
		found := false
		for index, source := range run.Sources {
			if source.SourceID != input.SourceID {
				continue
			}
			if found || source.Snapshot.Config == nil || source.Kind != backupruntime.BackupRuntimeSourceConfig ||
				source.Snapshot.Config.ConfigSnapshotID != input.SnapshotID || source.Snapshot.Config.ReadRevision != input.ReadRevision ||
				index >= len(plan.Steps) || index >= len(task.Steps) {
				return nil, configSnapshotGuardConflict()
			}
			step := plan.Steps[index]
			if step == nil || step.StepId != task.Steps[index].ID ||
				!configSnapshotSameAuthority(step.GetBackupStep().GetCapture().GetConfig(), input.Expected) ||
				step.GetBackupStep().GetStepDeadlineUnixNano() <= uint64(time.Now().UnixNano()) {
				return nil, configSnapshotGuardConflict()
			}
			found = true
		}
		if !found {
			return nil, configSnapshotGuardConflict()
		}
		conditions := make([]etcdstore.Condition, len(keys))
		for index, value := range read.Values {
			conditions[index] = etcdstore.Condition{Key: keys[index], ModRevision: value.ModRevision}
		}
		return conditions, nil
	}, nil
}

func configSnapshotGuardConflict() error {
	return errs.New(errs.KindStateConflict, "Config snapshot live Task, procedure or Environment ownership changed")
}
