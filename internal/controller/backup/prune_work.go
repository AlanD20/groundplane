package backup

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupplanning"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func buildPruneWork(prepared etcd.PreparedBackupPrune) (workexecution.Task, workexecution.Item, *agentpb.BackupWorkInput, error) {
	if prepared.Scope == nil || prepared.HostID == "" || len(prepared.Evidence) != len(prepared.Dispatch.RecoveryPointIDs) {
		return workexecution.Task{}, workexecution.Item{}, nil, errs.New(errs.KindValidationFailed, "Prune captured input is incomplete")
	}
	dispatch, scope := prepared.Dispatch, prepared.Scope
	deadline := dispatch.CreatedAt.Add(time.Duration(executionplan.MaximumBackupPruneStepTimeoutSeconds) * time.Second)
	input := &agentpb.BackupWorkInput{Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_PRUNE,
		ProjectId: scope.ProjectId, Project: scope.Project, EnvironmentId: scope.EnvironmentId, Environment: scope.Environment}
	for index, evidence := range prepared.Evidence {
		object, err := buildBackupPruneObject(evidence, uint32(index+1))
		if err != nil {
			return workexecution.Task{}, workexecution.Item{}, nil, err
		}
		step, err := executionplan.SealBackupStepAuthority(&agentpb.BackupStepAuthority{
			StepId: ids.New(ids.KindStep), ExecutionId: ids.NewULID(), StepDeadlineUnixNano: uint64(deadline.UnixNano()),
			Operation: &agentpb.BackupStepAuthority_Prune{Prune: &agentpb.BackupPruneAuthority{
				RetentionPolicy: proto.CloneOf(evidence.RetentionPolicy), Objects: []*agentpb.BackupPruneObject{object}}}})
		if err != nil {
			return workexecution.Task{}, workexecution.Item{}, nil, err
		}
		input.Steps = append(input.Steps, step)
	}
	if err := backupplanning.ValidateBackupPruneWorkInput(dispatch, prepared.Evidence, input); err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	owned, raw, reference, err := executionplan.CaptureBackupWorkInput(ids.New(ids.KindPlan), input)
	defer clear(raw)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	task := workexecution.Task{ID: dispatch.TaskID, Actor: workexecution.SystemActor,
		Scope:  workexecution.Scope{TenantID: prepared.Owner.TenantID, ProjectID: prepared.Owner.ProjectID, EnvironmentID: prepared.Owner.EnvironmentID},
		Action: workexecution.ActionBackupPrune, TargetID: dispatch.EnvironmentID, TargetName: dispatch.EnvironmentID,
		Intent: reference, ExpectedItems: 1, CreatedAt: dispatch.CreatedAt, UpdatedAt: dispatch.CreatedAt, Deadline: deadline}
	item, err := backupwork.NewItem(task, ids.New(ids.KindWorkItem), prepared.HostID, owned)
	return task, item, owned, err
}
