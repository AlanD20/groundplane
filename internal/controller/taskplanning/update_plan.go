package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (resolver *TaskPlanResolver) resolveUpdatePlan(ctx context.Context, task etcd.TaskRecord) (*agentpb.ExecutionPlan, error) {
	if taskjournal.IsBlueprintChild(task.Params) {
		return resolver.resolveCapturedBlueprintServicePlan(ctx, task)
	}
	return resolver.resolveEnvironmentBlueprintPlan(ctx, task)
}
