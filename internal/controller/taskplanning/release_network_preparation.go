package taskplanning

import (
	"context"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (resolver *TaskPlanResolver) PrepareReleaseNetworkTask(ctx context.Context,
	child, parent etcd.TaskRecord, artifact *agentpb.ComposeArtifact,
) (etcd.TaskRecord, *agentpb.ExecutionPlan, []string, error) {
	if ctx == nil || resolver == nil || len(child.Steps) != 0 || child.PlanHash != "" {
		return etcd.TaskRecord{}, nil, nil, errs.New(
			errs.KindValidationFailed,
			"release network Task preparation is invalid",
		)
	}
	plan, upstreams, err := resolver.buildReleaseNetworkPlan(child, parent, artifact)
	if err != nil {
		return etcd.TaskRecord{}, nil, nil, err
	}
	prepared := child
	prepared.PlanHash = hex.EncodeToString(plan.GetPlanHash())
	for _, step := range plan.GetSteps() {
		prepared.Steps = append(
			prepared.Steps,
			taskjournal.TaskStepRecord{Kind: taskjournal.TaskStepOperation, ID: step.GetStepId()},
		)
	}
	prepared.Steps = taskjournal.CaptureStepDescriptions(prepared.Steps, plan)
	return prepared, plan, upstreams, nil
}

func (resolver *TaskPlanResolver) buildReleaseNetworkPlan(child, parent etcd.TaskRecord,
	artifact *agentpb.ComposeArtifact,
) (*agentpb.ExecutionPlan, []string, error) {
	if child.Type != taskjournal.TaskUpdate || child.Actor != taskjournal.TaskActorSystem ||
		child.Executor != taskjournal.TaskExecutorAgent || child.Params[taskjournal.TaskReleaseNetworkParentParam] != parent.ID ||
		len(child.Params) != 1 || ids.Validate(ids.KindTask, parent.ID) != nil || child.ID == parent.ID ||
		parent.Executor != taskjournal.TaskExecutorAgent ||
		(parent.Type != taskjournal.TaskDeploy && parent.Type != taskjournal.TaskRollback) ||
		child.Owner != parent.Owner || child.Target != parent.Owner.EnvironmentID ||
		child.RenderGeneration != parent.RenderGeneration || child.RenderGeneration <= 0 ||
		child.TimeoutSeconds <= 0 || child.TimeoutSeconds > math.MaxUint32 ||
		len(child.Materializations) != 0 || child.Configuration != nil {
		return nil, nil, errs.New(errs.KindValidationFailed, "release network Task ownership is invalid")
	}
	planTime, err := ids.Timestamp(ids.KindPlan, child.PlanID)
	if err != nil {
		return nil, nil, err
	}
	artifactID := ids.DeriveAt(ids.KindConfig, planTime, child.PlanID, "release-network-preparation")
	preparation, err := executionplan.CaptureReleaseNetworkPreparation(artifact, artifactID)
	if err != nil {
		return nil, nil, err
	}
	plan, err := executionplan.BuildReleaseNetworkPreparation(
		child.PlanID,
		uint64(child.RenderGeneration),
		uint32(child.TimeoutSeconds),
		preparation,
	)
	if err != nil {
		return nil, nil, err
	}
	if err := executionplan.AuthorizeVolumeDirectories(plan, resolver.volumeRoot); err != nil {
		return nil, nil, err
	}
	return plan, preparation.Upstreams, nil
}
