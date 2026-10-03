package etcd

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type BackupStagingSource struct {
	Index             etcdstore.Versioned[backupruntime.BackupStagingIndexRecord]
	Task              etcdstore.Versioned[TaskRecord]
	Step              *agentpb.BackupStepAuthority
	Plan              *agentpb.ExecutionPlan
	Assignment        *etcdstore.Versioned[taskassignments.TaskAssignmentRecord]
	ProcedureRevision int64
	TerminalRestore   *backupruntime.BackupRestoreRecord
	VolumeRestore     *etcdstore.Versioned[backupruntime.BackupRestoreRecord]
}

// ReadBackupStagingSource resolves one opaque key at a consistent view. An
// unknown or pruned key is not permission to delete a local stage.
func (repository *TaskRepository) ReadBackupStagingSource(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	recoveryKey []byte,
) (BackupStagingSource, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return BackupStagingSource{}, err
	}
	if len(recoveryKey) != 32 {
		return BackupStagingSource{}, backupStagingConflict()
	}
	read, err := repository.store.Get(ctx, backupruntime.BackupStagingIndexKey(recoveryKey))
	if err != nil {
		return BackupStagingSource{}, err
	}
	if read == nil || read.Entry == nil || read.Entry.Version != 1 || read.ReadRevision <= 0 {
		return BackupStagingSource{}, backupStagingConflict()
	}
	defer clear(read.Entry.Value)
	index, err := backupruntime.DecodeBackupStagingIndex(read.Entry.Value)
	if err != nil {
		return BackupStagingSource{}, err
	}
	expectedKey, err := index.RecoveryKey()
	if err != nil || !bytes.Equal(expectedKey, recoveryKey) || index.AgentID != agentID ||
		index.AgentGeneration > agentGeneration || agentGeneration == 0 {
		return BackupStagingSource{}, backupStagingConflict()
	}
	keys := []string{
		taskjournal.TaskStorageKey(index.TaskID),
		backupruntime.BackupExecutionPlanKey(index.TaskID),
		taskjournal.TaskAssignmentIndexKey(index.TaskID),
	}
	snapshot, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: read.ReadRevision})
	if err != nil {
		return BackupStagingSource{}, err
	}
	if snapshot == nil || snapshot.ReadRevision != read.ReadRevision || len(snapshot.Values) != 3 ||
		snapshot.Values[0] == nil ||
		snapshot.Values[1] == nil ||
		snapshot.Values[1].Version != 1 {
		return BackupStagingSource{}, backupStagingConflict()
	}
	defer etcdstore.ClearValues(snapshot.Values)
	task, err := DecodeTaskRecord(snapshot.Values[0].Value)
	if err != nil {
		return BackupStagingSource{}, err
	}
	sealed, err := backupruntime.DecodeBackupExecutionPlan(snapshot.Values[1].Value)
	if err != nil {
		return BackupStagingSource{}, err
	}
	if !index.AllowsAgentGeneration(agentGeneration, taskjournal.IsTerminalTaskStatus(task.Status)) ||
		task.ID != index.TaskID || task.PlanHash != index.PlanSHA256 || sealed.PlanId != task.PlanID ||
		hex.EncodeToString(sealed.PlanHash) != index.PlanSHA256 || sealed.TargetId != task.Target {
		return BackupStagingSource{}, backupStagingConflict()
	}
	result := BackupStagingSource{
		Index: etcdstore.Versioned[backupruntime.BackupStagingIndexRecord]{
			Record:       index,
			Revision:     read.Entry.ModRevision,
			ReadRevision: read.ReadRevision,
		},
		Task: etcdstore.Versioned[TaskRecord]{
			Record:       task,
			Revision:     snapshot.Values[0].ModRevision,
			ReadRevision: read.ReadRevision,
		},
		ProcedureRevision: snapshot.Values[1].ModRevision,
		Plan:              sealed,
	}
	for _, step := range sealed.Steps {
		if step.StepId == index.StepID && backupStagingStepPoint(step.GetBackupStep()) == index.PointID {
			result.Step = step.GetBackupStep()
			break
		}
	}
	if result.Step == nil {
		return BackupStagingSource{}, backupStagingConflict()
	}
	if value := snapshot.Values[2]; value != nil {
		if taskjournal.IsTerminalTaskStatus(task.Status) {
			return BackupStagingSource{}, backupStagingConflict()
		}
		assignment, err := taskassignments.DecodeTaskAssignment(value.Value)
		if err != nil {
			return BackupStagingSource{}, err
		}
		if assignment.TaskID != task.ID || assignment.AgentID != agentID ||
			assignment.AgentGeneration != agentGeneration ||
			assignment.BackupAuthorityFence == nil {
			return BackupStagingSource{}, backupStagingConflict()
		}
		result.Assignment = &etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
			Record:       assignment,
			Revision:     value.ModRevision,
			ReadRevision: read.ReadRevision,
		}
	} else if !taskjournal.IsTerminalTaskStatus(task.Status) {
		return BackupStagingSource{}, backupStagingConflict()
	}
	if task.Type == taskjournal.TaskRestore && taskjournal.IsTerminalTaskStatus(task.Status) {
		if err := repository.validateBackupTerminalReceiptReplay(ctx, result.Task); err != nil {
			return BackupStagingSource{}, err
		}
		native, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{backupruntime.BackupRestoreKey(task.ID)}, Revision: read.ReadRevision,
		})
		if err != nil {
			return BackupStagingSource{}, err
		}
		if native == nil || native.ReadRevision != read.ReadRevision || len(native.Values) != 1 ||
			native.Values[0] == nil || native.Values[0].ModRevision != result.Task.Revision {
			return BackupStagingSource{}, backupStagingConflict()
		}
		defer etcdstore.ClearValues(native.Values)
		restored, err := backupruntime.DecodeBackupRestoreRecord(native.Values[0].Value)
		if err != nil {
			return BackupStagingSource{}, err
		}
		validPlan := backupruntime.ValidateConfigRestoreExecutionPlan(restored, sealed) == nil
		if restored.Point.SourceKind == backupruntime.BackupRuntimeSourceVolume {
			validPlan = backupruntime.ValidateVolumeRestoreExecutionPlan(restored, sealed) == nil
		}
		if restored.Point.SourceKind == backupruntime.BackupRuntimeSourceAttach {
			validPlan = backupruntime.ValidatePostgresRestoreExecutionPlan(restored, sealed) == nil
		}
		if restored.TaskID != task.ID || restored.OperationID != task.OperationID ||
			restored.EnvironmentID != task.Owner.EnvironmentID || !validPlan {
			return BackupStagingSource{}, backupStagingConflict()
		}
		result.TerminalRestore = &restored
		if restored.Point.SourceKind == backupruntime.BackupRuntimeSourceVolume {
			result.VolumeRestore = &etcdstore.Versioned[backupruntime.BackupRestoreRecord]{
				Record: restored, Revision: native.Values[0].ModRevision, ReadRevision: read.ReadRevision}
		}
	}
	if task.Type == taskjournal.TaskRestore &&
		(result.Step.GetRestore().GetVolume() != nil || result.Step.GetRestore().GetPostgres() != nil) &&
		!taskjournal.IsTerminalTaskStatus(task.Status) {
		native, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{backupruntime.BackupRestoreKey(task.ID)}, Revision: read.ReadRevision})
		if err != nil {
			return BackupStagingSource{}, err
		}
		if native == nil || native.ReadRevision != read.ReadRevision || len(native.Values) != 1 ||
			native.Values[0] == nil {
			return BackupStagingSource{}, backupStagingConflict()
		}
		defer etcdstore.ClearValues(native.Values)
		restored, err := backupruntime.DecodeBackupRestoreRecord(native.Values[0].Value)
		if err != nil {
			return BackupStagingSource{}, backupStagingConflict()
		}
		validPlan := backupruntime.ValidateVolumeRestoreExecutionPlan(restored, sealed) == nil
		if result.Step.GetRestore().GetPostgres() != nil {
			validPlan = backupruntime.ValidatePostgresRestoreExecutionPlan(restored, sealed) == nil
		}
		if restored.TaskID != task.ID || restored.OperationID != task.OperationID ||
			restored.EnvironmentID != task.Owner.EnvironmentID || !validPlan {
			return BackupStagingSource{}, backupStagingConflict()
		}
		if result.Step.GetRestore().GetVolume() != nil {
			result.VolumeRestore = &etcdstore.Versioned[backupruntime.BackupRestoreRecord]{
				Record: restored, Revision: native.Values[0].ModRevision, ReadRevision: read.ReadRevision}
		}
	}
	return result, nil
}

func backupStagingConflict() error {
	return errs.New(errs.KindStateConflict, "Backup staging native authority is unavailable or changed")
}
