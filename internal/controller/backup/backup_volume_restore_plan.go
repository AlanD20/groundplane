package backup

import (
	"bytes"
	"encoding/hex"
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type VolumeRestorePlanInput struct {
	Task      etcd.TaskRecord
	Restore   backupruntime.BackupRestoreRecord
	Scope     *agentpb.BackupPlanScope
	Authority *agentpb.BackupStepAuthority
	Artifacts []*agentpb.ComposeArtifact
}

func BuildVolumeRestorePlan(input VolumeRestorePlanInput) (*agentpb.ExecutionPlan, error) {
	task, restore := input.Task, input.Restore
	if backupruntime.ValidateBackupRestoreRecord(restore) != nil ||
		restore.Point.SourceKind != backupruntime.BackupRuntimeSourceVolume ||
		task.Type != taskjournal.TaskRestore || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Actor != taskjournal.TaskActorOperator || task.ID != restore.TaskID ||
		task.OperationID != restore.OperationID || task.Target != restore.EnvironmentID ||
		task.Owner.EnvironmentID != restore.EnvironmentID || task.Owner.ProjectID != input.Scope.GetProjectId() ||
		ids.Validate(ids.KindTask, task.ID) != nil || ids.Validate(ids.KindPlan, task.PlanID) != nil ||
		ids.Validate(ids.KindOperation, task.OperationID) != nil ||
		task.TimeoutSeconds != backupRunTaskTimeoutSeconds || task.RenderGeneration != 0 ||
		len(task.Params) != 0 || len(task.Materializations) != 0 || len(task.Steps) != 1 ||
		!task.CreatedAt.Equal(
			restore.CreatedAt,
		) || input.Scope == nil || input.Authority == nil || len(input.Artifacts) == 0 {
		return nil, errs.New(errs.KindValidationFailed, "Volume Restore Task authority is invalid")
	}
	if task.Status != taskjournal.TaskStatusPending && task.Status != taskjournal.TaskStatusRunning ||
		task.Status == taskjournal.TaskStatusPending && restore.State != backupruntime.BackupRestoreQueued {
		return nil, errs.New(errs.KindValidationFailed, "Volume Restore Task state is invalid")
	}
	step := task.Steps[0]
	if step.Kind != taskjournal.TaskStepOperation || ids.Validate(ids.KindStep, step.ID) != nil ||
		step.ID != input.Authority.StepId || input.Authority.GetRestore().GetVolume() == nil ||
		input.Authority.ExecutionId != restore.RestoreGenerationID ||
		input.Authority.StepDeadlineUnixNano != uint64(task.CreatedAt.Add(6*time.Hour).UnixNano()) {
		return nil, errs.New(errs.KindValidationFailed, "Volume Restore Step authority is invalid")
	}
	authority := input.Authority
	var err error
	if len(authority.StepDigest) == 0 {
		authority, err = executionplan.SealBackupStepAuthority(authority)
	} else {
		authority, err = executionplan.ValidateBackupStepAuthority(authority)
	}
	if err != nil {
		return nil, err
	}
	sealed, err := executionplan.Seal(&agentpb.ExecutionPlan{Schema: executionplan.SchemaVersion,
		PlanId: task.PlanID, Operation: agentpb.PlanOperation_PLAN_OPERATION_RESTORE,
		TargetId: restore.EnvironmentID, BackupScope: proto.CloneOf(input.Scope),
		Artifacts: proto.CloneOf(&agentpb.ExecutionPlan{Artifacts: input.Artifacts}).Artifacts,
		Steps: []*agentpb.ExecutionStep{{StepId: step.ID, TimeoutSeconds: uint32(backupRunTaskTimeoutSeconds),
			Payload: &agentpb.ExecutionStep_BackupStep{BackupStep: proto.CloneOf(authority)}}}})
	if err != nil {
		return nil, err
	}
	if err := backupruntime.ValidateVolumeRestoreExecutionPlan(restore, sealed); err != nil {
		return nil, err
	}
	if task.PlanHash != "" {
		digest, err := hex.DecodeString(task.PlanHash)
		if err != nil || !bytes.Equal(digest, sealed.PlanHash) {
			return nil, errs.New(errs.KindStateConflict, "Volume Restore sealed plan changed")
		}
	}
	return sealed, nil
}
