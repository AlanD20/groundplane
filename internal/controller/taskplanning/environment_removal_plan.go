package taskplanning

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"math"
)

func (resolver *TaskPlanResolver) resolveEnvironmentRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.blueprints == nil || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		ids.Validate(ids.KindEnvironment, task.Target) != nil || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 || len(task.Materializations) != 0 ||
		(len(task.Params) != 1 && len(task.Params) != 4) {
		return nil, errs.New(errs.KindInternal, "durable Environment removal Task shape is invalid")
	}
	volumeDirectory, exists := task.Params[taskcontract.EnvironmentRemoveVolumeDirectoryParam]
	if !exists {
		return nil, errs.New(errs.KindInternal, "durable Environment removal directory is missing")
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, task.Target)
	if err != nil {
		return nil, err
	}
	if environment.Record.VolumeDir != volumeDirectory {
		return nil, errs.New(errs.KindInternal, "durable Environment removal directory changed")
	}
	directoryStep := func(stepID string) *agentpb.ExecutionStep {
		return &agentpb.ExecutionStep{
			StepId: stepID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_EnvironmentDirectoryRemove{
				EnvironmentDirectoryRemove: &agentpb.EnvironmentDirectoryRemove{
					EnvironmentId: task.Target, ExpectedVolumeDir: volumeDirectory,
				},
			},
		}
	}
	if len(task.Params) == 1 {
		if len(task.Steps) != 1 || ids.Validate(ids.KindStep, task.Steps[0].ID) != nil {
			return nil, errs.New(errs.KindInternal, "artifact-free Environment removal procedure is invalid")
		}
		_, found, err := resolver.blueprints.GetEnvironmentComposeProjection(ctx, task.Target)
		if err != nil {
			return nil, err
		}
		if found {
			return nil, errs.New(errs.KindInternal, "artifact-free Environment removal lost its pinned desired state")
		}
		return taskplan.Build(taskplan.BuildInput{
			VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
			RenderGeneration: uint64(task.RenderGeneration),
			Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
			Steps: []*agentpb.ExecutionStep{directoryStep(task.Steps[0].ID)},
		})
	}
	if len(task.Steps) != 2 || ids.Validate(ids.KindStep, task.Steps[0].ID) != nil ||
		ids.Validate(ids.KindStep, task.Steps[1].ID) != nil {
		return nil, errs.New(errs.KindInternal, "Environment removal procedure is invalid")
	}
	revisionID := task.Params[etcd.EnvironmentDesiredRevisionParam]
	artifactID := task.Params[taskcontract.EnvironmentBlueprintArtifactParam]
	if task.Params[etcd.TaskMaterializationEnvironmentParam] != task.Target ||
		ids.Validate(ids.KindTask, revisionID) != nil || ids.Validate(ids.KindConfig, artifactID) != nil {
		return nil, errs.New(errs.KindInternal, "Environment removal Blueprint parameters are invalid")
	}
	pinned, err := resolver.renderPinnedEnvironmentBlueprintArtifact(ctx, task, revisionID, artifactID)
	if err != nil {
		return nil, err
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration),
		Operation:        agentpb.PlanOperation_PLAN_OPERATION_REMOVE, TargetID: task.Target,
		Artifacts: []*agentpb.ComposeArtifact{pinned.artifact},
		Steps: []*agentpb.ExecutionStep{
			{
				StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
				Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
					ArtifactId: artifactID, WholeProject: true,
				}},
			},
			directoryStep(task.Steps[1].ID),
		},
	})
}
