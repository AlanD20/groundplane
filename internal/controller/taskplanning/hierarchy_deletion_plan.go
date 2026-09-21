package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/hierarchyplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
)

func (resolver *TaskPlanResolver) resolveHierarchyDeletionPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.blueprints == nil || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || task.RenderGeneration != 1 ||
		task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 ||
		len(task.Params) != 9 ||
		task.Params[taskjournal.TaskHierarchyDeletionProcedureParam] != "environment.cleanup" ||
		(len(task.Steps) != 1 && len(task.Steps) != 2) {
		return nil, errs.New(errs.KindInternal, "durable hierarchy Environment cleanup Task is invalid")
	}
	for _, step := range task.Steps {
		if ids.Validate(ids.KindStep, step.ID) != nil {
			return nil, errs.New(errs.KindInternal, "durable hierarchy Environment cleanup Task is invalid")
		}
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	projection, found, err := resolver.blueprints.GetEnvironmentComposeProjection(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	composeStepID := ""
	directoryStepID := task.Steps[0].ID
	var composeArtifact []byte
	if found {
		if len(task.Steps) != 2 {
			return nil, errs.New(errs.KindInternal, "durable hierarchy Environment cleanup lost its Compose step")
		}
		composeStepID = task.Steps[0].ID
		directoryStepID = task.Steps[1].ID
		composeArtifact = projection.Record.ComposeArtifact
	} else if len(task.Steps) != 1 {
		return nil, errs.New(errs.KindInternal, "artifact-free hierarchy Environment cleanup has a Compose step")
	}
	plan, err := hierarchyplan.EnvironmentCleanup(
		task.PlanID,
		uint64(task.RenderGeneration),
		task.Target,
		composeStepID,
		directoryStepID,
		uint32(task.TimeoutSeconds),
		environment.Record.VolumeDir,
		composeArtifact,
	)
	if err != nil {
		return nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, resolver.volumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
}
