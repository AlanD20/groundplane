package taskplanning

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	materializationrecord "github.com/AlanD20/groundplane/internal/common/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/controller/taskmaterialization"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (resolver *TaskPlanResolver) validateSealedRouteTask(
	task etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
	provider *environmentchanges.RouteProviderPin,
	candidate *projectionrecord.EnvironmentComposeProjection,
	removal bool,
) (*agentpb.ExecutionPlan, error) {
	if plan == nil || task.Executor != taskjournal.TaskExecutorAgent || task.PlanID != plan.GetPlanId() ||
		task.PlanHash != hex.EncodeToString(plan.GetPlanHash()) || task.Target != plan.GetTargetId() ||
		uint64(task.RenderGeneration) != plan.GetRenderGeneration() || len(task.Steps) != len(plan.GetSteps()) {
		return nil, errs.New(errs.KindStateConflict, "Route Task differs from its sealed execution plan")
	}
	parameterCount := 5
	if removal {
		parameterCount = 4
	}
	if provider == nil || candidate == nil || len(task.Params) != parameterCount ||
		task.Owner.EnvironmentID != candidate.EnvironmentID ||
		task.Params[taskjournal.TaskRouteEnvironmentParam] != candidate.EnvironmentID ||
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] != candidate.EnvironmentID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != candidate.RevisionID ||
		task.Params[taskcontract.EnvironmentBlueprintArtifactParam] != plan.GetArtifacts()[0].GetArtifactId() ||
		!removal && task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceRoute ||
		len(task.Materializations) != 1 || task.Configuration == nil || task.TimeoutSeconds <= 0 {
		return nil, errs.New(errs.KindStateConflict, "Route Task inputs differ from their captured authority")
	}
	reference := task.Materializations[0]
	source := reference.Source.ComponentFile
	if reference.Source.Kind != materializationrecord.SourceComponentFile || source == nil ||
		source.RevisionID != candidate.RevisionID || source.RouteTaskID != candidate.RevisionID ||
		ids.Validate(ids.KindTask, source.RouteTaskID) != nil || source.ComponentID != provider.ComponentID ||
		source.Path != provider.Destination {
		return nil, errs.New(errs.KindStateConflict, "Route Task file source differs from its captured provider")
	}
	materialization, err := taskmaterialization.BuildTaskMaterializationStep(
		reference, plan.GetArtifacts()[0].GetArtifactId(), uint32(task.TimeoutSeconds),
	)
	if err != nil {
		return nil, err
	}
	materialization.Policy = agentpb.ExecutionStepPolicy_EXECUTION_STEP_POLICY_RELEASE_FORWARD
	if !proto.Equal(materialization, plan.GetSteps()[0]) {
		return nil, errs.New(errs.KindStateConflict, "Route Task file metadata differs from its sealed step")
	}
	for index, step := range plan.GetSteps() {
		if task.Steps[index].ID != step.GetStepId() || task.Steps[index].Kind != taskjournal.TaskStepOperation {
			return nil, errs.New(errs.KindStateConflict, "Route Task steps differ from their sealed authority")
		}
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, resolver.volumeRoot); err != nil {
		return nil, err
	}
	return plan, nil
}
