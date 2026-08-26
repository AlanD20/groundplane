package controller

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const ServiceStopGraceSeconds = uint32(30)

type serviceLifecyclePlanReader interface {
	GetServiceLifecycleRenderInput(
		context.Context,
		string,
	) (etcd.Versioned[etcd.ServiceLifecycleRenderInput], bool, error)
}

func (resolver *TaskPlanResolver) PrepareServiceLifecycleTask(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
	stepID string,
) (etcd.TaskRecord, error) {
	if resolver == nil || ctx == nil || ids.Validate(ids.KindStep, stepID) != nil ||
		task.Executor != etcd.TaskExecutorAgent || task.PlanID != input.PlanID || task.Target != input.ServiceID {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Service lifecycle Task preparation is invalid")
	}
	if input.Projection.RenderGeneration == 0 || input.Projection.RenderGeneration > uint64(^uint32(0)>>1) {
		return etcd.TaskRecord{}, errs.New(errs.KindStateConflict, "Service render generation exceeds Task limits")
	}
	prepared := task
	prepared.RenderGeneration = int32(input.Projection.RenderGeneration)
	prepared.Params = map[string]string{
		etcd.TaskServiceEnvironmentParam: input.EnvironmentID,
		etcd.TaskComposeArtifactParam:    input.ArtifactID,
	}
	prepared.Steps = []etcd.TaskStepRecord{{ID: stepID}}
	plan, err := resolver.buildServiceLifecyclePlan(ctx, prepared, input)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	return prepared, nil
}

func (resolver *TaskPlanResolver) resolveServiceLifecyclePlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.services.(serviceLifecyclePlanReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "Service lifecycle render input reader is not configured")
	}
	input, found, err := reader.GetServiceLifecycleRenderInput(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found || input.Record.PlanID != task.PlanID || input.Record.ServiceID != task.Target ||
		input.Record.Projection.RenderGeneration != uint64(task.RenderGeneration) {
		return nil, errs.New(errs.KindInternal, "Service lifecycle render input does not match Task")
	}
	return resolver.buildServiceLifecyclePlan(ctx, task, input.Record)
}

func (resolver *TaskPlanResolver) buildServiceLifecyclePlan(
	ctx context.Context,
	task etcd.TaskRecord,
	input etcd.ServiceLifecycleRenderInput,
) (*agentpb.ExecutionPlan, error) {
	if len(task.Steps) != 1 || task.Params[etcd.TaskServiceEnvironmentParam] != input.EnvironmentID ||
		task.Params[etcd.TaskComposeArtifactParam] != input.ArtifactID {
		return nil, errs.New(errs.KindInternal, "Service lifecycle Task procedure changed")
	}
	phase := core.ServiceLifecyclePhase("")
	if task.Type == etcd.TaskStart {
		phase = core.ServiceLifecycleStart
	}
	artifact, err := resolver.renderPinnedEnvironmentArtifactForPhase(
		ctx,
		task,
		pinnedEnvironmentIdentity{
			TenantID: input.TenantID, TenantSlug: input.TenantSlug,
			ProjectID: input.ProjectID, ProjectSlug: input.ProjectSlug,
			EnvironmentID:       input.EnvironmentID,
			EnvironmentName:     input.EnvironmentName,
			AuthorizedVolumeDir: input.AuthorizedVolumeDir,
		},
		input.Projection.BlueprintRevisionID,
		input.ArtifactID,
		input.Projection,
		phase,
		nil,
	)
	if err != nil {
		return nil, err
	}
	operation, step, err := serviceLifecycleProcedure(task, input.ArtifactID)
	if err != nil {
		return nil, err
	}
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: operation,
		TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{step},
	})
}

func serviceLifecycleProcedure(
	task etcd.TaskRecord,
	artifactID string,
) (agentpb.PlanOperation, *agentpb.ExecutionStep, error) {
	step := &agentpb.ExecutionStep{StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds)}
	switch task.Type {
	case etcd.TaskStart:
		step.Payload = &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
			ArtifactId: artifactID, ServiceIds: []string{task.Target}, FullReconcile: false,
		}}
		return agentpb.PlanOperation_PLAN_OPERATION_START, step, nil
	case etcd.TaskStop:
		step.Payload = &agentpb.ExecutionStep_ComposeStop{ComposeStop: &agentpb.ComposeStop{
			ArtifactId: artifactID, ServiceIds: []string{task.Target}, GraceSeconds: ServiceStopGraceSeconds,
		}}
		return agentpb.PlanOperation_PLAN_OPERATION_STOP, step, nil
	case etcd.TaskDestroy:
		step.Payload = &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
			ArtifactId: artifactID, ServiceIds: []string{task.Target}, WholeProject: false,
		}}
		return agentpb.PlanOperation_PLAN_OPERATION_DESTROY, step, nil
	default:
		return agentpb.PlanOperation_PLAN_OPERATION_UNSPECIFIED, nil,
			errs.New(errs.KindInternal, "Service lifecycle Task type is invalid")
	}
}
