package taskplanning

import (
	"context"
	"encoding/hex"
	taskplan "github.com/AlanD20/groundplane/internal/controller/taskplan"
	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type serviceRemovalPlanReader interface {
	GetServiceRemovalIntent(
		context.Context,
		string,
	) (etcdstore.Versioned[environmentchanges.ServiceRemovalIntent], bool, error)
}

func (resolver *TaskPlanResolver) PrepareServiceRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.ServiceRemovalIntent,
	artifactID string,
	stepID string,
) (etcd.TaskRecord, error) {
	if resolver == nil ||
		ctx == nil ||
		ids.Validate(ids.KindConfig, artifactID) != nil ||
		ids.Validate(ids.KindStep, stepID) != nil ||
		task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskRemove ||
		task.Target != intent.ServiceID ||
		task.PlanID == "" {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Service removal Task preparation is invalid")
	}
	prepared := task
	prepared.RenderGeneration = int32(intent.CurrentProjection.RenderGeneration)
	prepared.Params = map[string]string{
		taskjournal.TaskResourceKindParam:          taskjournal.TaskResourceService,
		taskjournal.TaskServiceEnvironmentParam:    intent.EnvironmentID,
		taskjournal.TaskComposeArtifactParam:       artifactID,
		blueprints.EnvironmentDesiredRevisionParam: intent.Claim.RevisionID,
	}
	prepared.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: stepID}}
	plan, err := resolver.buildServiceRemovalPlan(ctx, prepared, intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	return prepared, nil
}

func (resolver *TaskPlanResolver) resolveServiceRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.services.(serviceRemovalPlanReader)
	if !ok ||
		reader == nil {
		return nil, errs.New(errs.KindInternal, "Service removal intent reader is not configured")
	}
	stored, found, err := reader.GetServiceRemovalIntent(ctx, task.ID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "Service removal intent was not found")
	}
	return resolver.buildServiceRemovalPlan(ctx, task, stored.Record)
}

func (resolver *TaskPlanResolver) buildServiceRemovalPlan(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.ServiceRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	if err := validateServiceRemovalPlanTask(task, intent); err != nil {
		return nil, err
	}
	environment, err := resolver.blueprints.GetEnvironment(ctx, intent.EnvironmentID)
	if err != nil {
		return nil, err
	}
	project, err := resolver.blueprints.GetProject(ctx, environment.Record.ProjectID)
	if err != nil {
		return nil, err
	}
	tenant, err := resolver.blueprints.GetTenant(ctx, project.Record.TenantID)
	if err != nil {
		return nil, err
	}
	artifactID := task.Params[taskjournal.TaskComposeArtifactParam]
	artifact, err := resolver.renderPinnedEnvironmentArtifact(
		ctx, task,
		pinnedEnvironmentIdentity{
			TenantID: tenant.Record.ID, TenantSlug: tenant.Record.Slug,
			ProjectID: project.Record.ID, ProjectSlug: project.Record.Slug,
			EnvironmentID: environment.Record.ID, EnvironmentName: environment.Record.Name,
			AuthorizedVolumeDir: environment.Record.VolumeDir,
		},
		intent.CurrentProjection.RevisionID, artifactID, intent.CurrentProjection, nil,
	)
	if err != nil {
		return nil, err
	}
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact},
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ComposeRemove{ComposeRemove: &agentpb.ComposeRemove{
				ArtifactId: artifactID, ServiceIds: []string{intent.ServiceID}, WholeProject: false,
			}},
		}},
	})
}

func validateServiceRemovalPlanTask(task etcd.TaskRecord, intent environmentchanges.ServiceRemovalIntent) error {
	if task.ID != intent.TaskID ||
		task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskRemove ||
		task.Target != intent.ServiceID ||
		len(task.Params) != 4 ||
		len(task.Steps) != 1 ||
		task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceService ||
		task.Params[taskjournal.TaskServiceEnvironmentParam] != intent.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID ||
		ids.Validate(ids.KindConfig, task.Params[taskjournal.TaskComposeArtifactParam]) != nil ||
		ids.Validate(ids.KindStep, task.Steps[0].ID) != nil ||
		task.TimeoutSeconds <= 0 ||
		uint64(task.RenderGeneration) != intent.CurrentProjection.RenderGeneration {
		return errs.New(errs.KindInternal, "durable Service removal Task changed")
	}
	return nil
}
