package taskplanning

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/controller/taskcontract"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// A Service child dispatches its immutable native preparation. Reconnection
// never rerenders a rollout from current Service state, images or hook inputs.
func (resolver *TaskPlanResolver) resolveCapturedBlueprintServicePlan(ctx context.Context,
	task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	if resolver.releases == nil || !taskjournal.IsBlueprintChild(task.Params) ||
		task.Params[taskcontract.EnvironmentBlueprintProcedureParam] != string(
			taskcontract.BlueprintComposeProcedureCandidateReleases,
		) ||
		ids.Validate(ids.KindConfig, task.Params[taskcontract.EnvironmentBlueprintArtifactParam]) != nil {
		return nil, errs.New(errs.KindInternal, "Blueprint Service child has no configured captured rollout")
	}
	// Visibility inputs precede hook preparation. Validate their publication,
	// but replay hooks and effects only from the complete native capture.
	visibility, err := resolver.releases.GetBlueprintTaskRenderInput(ctx, task)
	if err != nil {
		return nil, err
	}
	if len(visibility.Members) != 1 {
		return nil, errs.New(errs.KindInternal, "Blueprint Service child must own exactly one Release")
	}
	stored, err := resolver.releases.GetBlueprintServiceRollout(ctx, task)
	if err != nil {
		return nil, err
	}
	defer clear(stored.Record.NativePlan)
	runtime, err := RestoreBlueprintReleaseRuntime(stored.Record)
	if err != nil {
		return nil, err
	}
	member := runtime.plan.GetCandidateReleaseProcedure().GetMembers()[0]
	if member.GetServiceId() != visibility.Members[0].Render.ServiceID ||
		member.GetCandidateArtifactId() != task.Params[taskcontract.EnvironmentBlueprintArtifactParam] {
		return nil, errs.New(errs.KindStateConflict, "captured Blueprint rollout differs from its published Service")
	}
	expectedParams := 6
	for _, step := range runtime.plan.Steps {
		if step.GetRunScript() != nil {
			expectedParams += 2
		}
	}
	if len(task.Params) != expectedParams {
		return nil, errs.New(errs.KindInternal, "captured Blueprint Service Task contains unrecognized parameters")
	}
	if err := resolver.configurationRecoverySuffix(ctx, task, len(stored.Record.Steps)); err != nil {
		return nil, err
	}
	_, plan, err := resolver.SealBlueprintReleaseRuntime(ctx, task, runtime)
	if err != nil {
		return nil, err
	}
	if hex.EncodeToString(plan.PlanHash) != task.PlanHash {
		return nil, errs.New(
			errs.KindStateConflict,
			"captured Blueprint rollout no longer matches the published Task plan",
		)
	}
	return plan, nil
}
