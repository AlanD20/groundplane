package taskplanning

import (
	"context"
	"encoding/hex"
	"math"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/controller/taskplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type zoneRemovalPlanReader interface {
	GetZoneRemovalIntent(
		context.Context,
		string,
	) (etcdstore.Versioned[environmentchanges.ZoneRemovalIntent], bool, error)
}

func (resolver *TaskPlanResolver) PrepareZoneRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent environmentchanges.ZoneRemovalIntent,
	networkStepID string,
) (etcd.TaskRecord, error) {
	if resolver == nil || ctx == nil || task.Executor != taskjournal.TaskExecutorAgent ||
		task.Type != taskjournal.TaskRemove || task.Target != intent.ZoneID || task.PlanID == "" ||
		ids.Validate(ids.KindStep, networkStepID) != nil ||
		intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Zone removal Task preparation is invalid")
	}
	prepared := task
	prepared.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	prepared.Params = map[string]string{
		taskjournal.TaskZoneEnvironmentParam:       intent.EnvironmentID,
		taskjournal.TaskZoneRemovalOperationParam:  intent.OperationID,
		blueprints.EnvironmentDesiredRevisionParam: intent.Claim.RevisionID,
	}
	prepared.Steps = []taskjournal.TaskStepRecord{{Kind: taskjournal.TaskStepOperation, ID: networkStepID}}
	plan, err := resolver.buildZoneRemovalPlan(prepared, intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	prepared.Steps = taskjournal.CaptureStepDescriptions(prepared.Steps, plan.Steps)
	return prepared, nil
}

func (resolver *TaskPlanResolver) resolveZoneRemovalPlan(
	ctx context.Context, task etcd.TaskRecord,
) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(zoneRemovalPlanReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "Zone removal intent reader is not configured")
	}
	stored, found, err := reader.GetZoneRemovalIntent(ctx, task.Params[taskjournal.TaskZoneRemovalOperationParam])
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "Zone removal intent was not found")
	}
	return resolver.buildZoneRemovalPlan(task, stored.Record)
}

func (resolver *TaskPlanResolver) buildZoneRemovalPlan(
	task etcd.TaskRecord, intent environmentchanges.ZoneRemovalIntent,
) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || task.Executor != taskjournal.TaskExecutorAgent || task.Type != taskjournal.TaskRemove ||
		task.Target != intent.ZoneID || task.ID != intent.ActiveTaskID || intent.Status != taskjournal.TaskStatusPending ||
		len(task.Params) != 3 || len(task.Materializations) != 0 || len(task.Steps) != 1 ||
		task.TimeoutSeconds <= 0 || task.TimeoutSeconds > math.MaxUint32 ||
		uint64(task.RenderGeneration) != intent.CandidateProjection.RenderGeneration ||
		task.Params[taskjournal.TaskZoneEnvironmentParam] != intent.EnvironmentID ||
		task.Params[taskjournal.TaskZoneRemovalOperationParam] != intent.OperationID ||
		task.Params[blueprints.EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID ||
		ids.Validate(ids.KindStep, task.Steps[0].ID) != nil {
		return nil, errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	dockerName, err := networkname.New(intent.ZoneID)
	if err != nil {
		return nil, errs.New(errs.KindInternal, "Zone removal Network identity is invalid")
	}
	// Disconnect the existing members, including proxy and retained workload
	// slots. Reapplying desired Compose here would deploy pending edits and
	// invent a regular workload beside the acknowledged blue-green slot.
	return taskplan.Build(taskplan.BuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		TargetID: task.Target,
		Steps: []*agentpb.ExecutionStep{{
			StepId: task.Steps[0].ID, TimeoutSeconds: uint32(task.TimeoutSeconds),
			Payload: &agentpb.ExecutionStep_ManagedNetworkRemove{ManagedNetworkRemove: &agentpb.ManagedNetworkRemove{
				NetworkId: intent.ZoneID, EnvironmentId: intent.EnvironmentID, DockerName: dockerName,
			}},
		}},
	})
}
