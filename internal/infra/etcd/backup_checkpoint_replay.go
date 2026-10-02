package etcd

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReplayBackupCheckpoint authenticates an existing receipt without replaying
// its domain transition. Later progress does not replace the original commit
// revision returned to a caller whose acknowledgement was lost.
func (repository *BackupRuntimeRepository) ReplayBackupCheckpoint(
	ctx context.Context,
	input backupruntime.BackupCheckpointInput,
) (int64, bool, error) {
	if err := backupruntime.ValidateBackupCheckpointInput(input); err != nil {
		return 0, false, err
	}
	keys := []string{taskjournal.TaskStorageKey(input.TaskID), backupruntime.BackupExecutionPlanKey(input.TaskID)}
	read, err := repository.ReadCurrentKeys(ctx, keys)
	if err != nil {
		return 0, false, err
	}
	defer etcdstore.ClearValues(read.Values)
	if len(read.Values) != 2 || read.Values[0] == nil || read.Values[1] == nil {
		return 0, false, errs.New(errs.KindStateConflict, "backup checkpoint Task authority is unavailable")
	}
	task, err := DecodeTaskRecord(read.Values[0].Value)
	if err != nil {
		return 0, false, err
	}
	sealed, err := backupruntime.DecodeBackupExecutionPlan(read.Values[1].Value)
	if err != nil {
		return 0, false, err
	}
	if sealed.PlanId != task.PlanID || hex.EncodeToString(sealed.PlanHash) != task.PlanHash {
		return 0, false, backupruntime.CorruptBackupRuntimeRecord()
	}
	binding := backupCheckpointBinding{taskType: task.Type}
	found := false
	for index, step := range sealed.Steps {
		if step.StepId != input.StepID || step.GetBackupStep().ExecutionId != input.ExecutionID {
			continue
		}
		found, binding.ordinal = true, uint32(index)
		authority := step.GetBackupStep()
		if capture := authority.GetCapture(); capture != nil {
			binding.pointID = capture.PointId
		}
		if restore := authority.GetRestore(); restore != nil {
			binding.pointID = restore.PointId
		}
		if prune := authority.GetPrune(); prune != nil && len(prune.Objects) == 1 {
			binding.pointID = prune.Objects[0].PointId
		}
		break
	}
	if !found {
		return 0, false, errs.New(errs.KindValidationFailed, "backup checkpoint is outside its sealed procedure")
	}
	plan, err := repository.loadBackupCheckpointPlan(ctx, input, read.ReadRevision, binding)
	if err != nil {
		return 0, false, err
	}
	defer plan.clear()
	return plan.commitRevision, plan.duplicate, nil
}
