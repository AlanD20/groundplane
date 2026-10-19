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

func buildBackupRunWork(
	prepared BackupRunPrepared,
	inputID string,
) (workexecution.Task, workexecution.Item, *agentpb.BackupWorkInput, error) {
	if prepared.Scope == nil || prepared.HostID == "" {
		return workexecution.Task{}, workexecution.Item{}, nil, errs.New(
			errs.KindValidationFailed,
			"Backup run capture is incomplete",
		)
	}
	scope := prepared.Scope
	input := &agentpb.BackupWorkInput{Schema: 1, Operation: agentpb.BackupWorkOperation_BACKUP_WORK_OPERATION_CAPTURE,
		ProjectId: scope.ProjectId, Project: scope.Project, EnvironmentId: scope.EnvironmentId,
		Environment: scope.Environment, Services: scope.Services, Steps: prepared.Authority, Artifacts: prepared.Artifacts}
	owned, raw, reference, err := executionplan.CaptureBackupWorkInput(inputID, input)
	defer clear(raw)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	actor := workexecution.OperatorActor
	if prepared.Run.Initiator == backupruntime.BackupRunInitiatorSchedule {
		actor = workexecution.SystemActor
	}
	task := workexecution.Task{ID: prepared.Run.TaskID, Actor: actor,
		Scope: workexecution.Scope{TenantID: prepared.Owner.TenantID, ProjectID: prepared.Owner.ProjectID,
			EnvironmentID: prepared.Owner.EnvironmentID},
		Action: workexecution.ActionBackup, TargetID: prepared.Run.EnvironmentID, TargetName: prepared.Run.EnvironmentID,
		Intent: reference, ExpectedItems: 1, CreatedAt: prepared.Run.CreatedAt, UpdatedAt: prepared.Run.CreatedAt,
		Deadline: prepared.Run.CreatedAt.Add(6 * time.Hour)}
	item, err := backupwork.NewItem(task, ids.New(ids.KindWorkItem), prepared.HostID, owned)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	return task, item, owned, nil
}
