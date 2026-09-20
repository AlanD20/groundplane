package controller

import (
	"context"
	backupcapability "github.com/AlanD20/groundplane/internal/controller/backup"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupRunPlanReader interface {
	GetBackupRun(context.Context, string) (etcd.Versioned[etcd.BackupRunRecord], error)
}

// EnableBackupPlans connects the task-plan resolver to the durable Backup run
// records needed when the Agent claims or reconnects a task. The resolver
// reconstructs the plan from the closed run snapshot, never from request input.
func (resolver *TaskPlanResolver) EnableBackupPlans(reader backupRunPlanReader) error {
	if resolver == nil || reader == nil {
		return errs.New(errs.KindInternal, "backup plan resolver requires a run reader")
	}
	resolver.backupRuns = reader
	return nil
}

func (resolver *TaskPlanResolver) resolveBackupRunPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || resolver.backupRuns == nil {
		return nil, errs.New(errs.KindInternal, "backup plan resolver is not configured")
	}
	run, err := resolver.backupRuns.GetBackupRun(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	return backupcapability.BuildBackupRunPlan(backupcapability.BackupRunPlanInput{
		Task:   task,
		Run:    run.Record,
		Upload: backupcapability.BackupRunUploadAuthorities(run.Record),
	})
}
