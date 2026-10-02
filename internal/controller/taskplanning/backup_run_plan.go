package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type backupRunPlanReader interface {
	GetBackupExecutionPlan(context.Context, string) (etcdstore.Versioned[*agentpb.ExecutionPlan], error)
}

// EnableBackupPlans connects the task-plan resolver to the durable Backup run
// procedures needed when the Agent claims or reconnects a task. Progress and
// current desired state cannot alter the procedure selected at publication.
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
	stored, err := resolver.backupRuns.GetBackupExecutionPlan(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	return stored.Record, nil
}
