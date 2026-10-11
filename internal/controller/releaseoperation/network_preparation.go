package releaseoperation

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// Direct releases retain their native operation. This prerequisite creates only
// the selected private DNS bridges; it does not apply an Environment Blueprint.
func (service *Service) prepareNetworkTask(ctx context.Context, parent etcd.TaskRecord,
	plan *agentpb.ExecutionPlan,
) (*etcd.TaskRecord, *agentpb.ExecutionPlan, error) {
	var artifact *agentpb.ComposeArtifact
	for _, candidate := range plan.GetArtifacts() {
		if candidate.GetArtifactId() == parent.Params[taskjournal.TaskComposeArtifactParam] {
			artifact = candidate
			break
		}
	}
	if artifact == nil {
		return nil, nil, errs.New(errs.KindInternal, "release network source artifact is missing")
	}
	preparation, err := executionplan.CaptureReleaseNetworkPreparation(artifact, ids.New(ids.KindConfig))
	if err != nil || preparation.Artifact == nil {
		return nil, nil, err
	}
	child := etcd.TaskRecord{
		ID: ids.New(ids.KindTask), OperationID: ids.New(ids.KindOperation), PlanID: ids.New(ids.KindPlan),
		Owner: parent.Owner, Actor: taskjournal.TaskActorSystem, Executor: taskjournal.TaskExecutorAgent,
		Type: taskjournal.TaskUpdate, Target: parent.Owner.EnvironmentID, RenderGeneration: parent.RenderGeneration,
		Params:         map[string]string{taskjournal.TaskReleaseNetworkParentParam: parent.ID},
		TimeoutSeconds: parent.TimeoutSeconds, Status: taskjournal.TaskStatusPending, NextEventSequence: 1,
		CreatedAt: parent.CreatedAt, UpdatedAt: parent.CreatedAt,
	}
	child, childPlan, _, err := service.plans.PrepareReleaseNetworkTask(ctx, child, parent, artifact)
	if err != nil {
		return nil, nil, err
	}
	return &child, childPlan, nil
}
