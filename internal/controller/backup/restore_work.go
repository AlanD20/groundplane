package backup

import (
	"time"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	"github.com/AlanD20/groundplane/internal/infra/etcd/backupwork"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func buildRestoreWork(prepared preparedRestore, inputID string) (workexecution.Task, workexecution.Item, *agentpb.BackupWorkInput, error) {
	if prepared.Scope == nil || prepared.HostID == "" || prepared.Authority == nil {
		return workexecution.Task{}, workexecution.Item{}, nil, errs.New(errs.KindValidationFailed, "Restore captured input is incomplete")
	}
	scope := prepared.Scope
	input := &agentpb.BackupWorkInput{Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_RESTORE,
		ProjectId: scope.ProjectId, Project: scope.Project, EnvironmentId: scope.EnvironmentId,
		Environment: scope.Environment, Services: scope.Services,
		Steps: []*agentpb.BackupStepAuthority{prepared.Authority}, Artifacts: prepared.Artifacts}
	if err := backupruntime.ValidateRestoreWorkInput(prepared.Restore, input); err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	owned, raw, reference, err := executionplan.CaptureBackupWorkInput(inputID, input)
	defer clear(raw)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	restore := prepared.Restore
	task := workexecution.Task{ID: restore.TaskID, Actor: workexecution.OperatorActor,
		Scope: workexecution.Scope{TenantID: prepared.Owner.TenantID, ProjectID: prepared.Owner.ProjectID,
			EnvironmentID: prepared.Owner.EnvironmentID},
		Action: workexecution.ActionRestore, TargetID: restore.EnvironmentID, TargetName: restore.EnvironmentID,
		Intent: reference, ExpectedItems: 1, CreatedAt: restore.CreatedAt, UpdatedAt: restore.CreatedAt,
		Deadline: restore.CreatedAt.Add(6 * time.Hour)}
	item, err := backupwork.NewItem(task, ids.New(ids.KindWorkItem), prepared.HostID, owned)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	return task, item, owned, nil
}
