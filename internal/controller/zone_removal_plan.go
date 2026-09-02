package controller

import (
	"context"
	"encoding/hex"
	"math"
	"strings"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

type zoneRemovalPlanReader interface {
	GetZoneRemovalIntent(context.Context, string) (etcd.Versioned[etcd.ZoneRemovalIntent], bool, error)
}

type ZoneRemovalTaskProcedureIDs struct {
	ArtifactID     string
	ServiceStepIDs []string
	NetworkStepID  string
}

func (resolver *TaskPlanResolver) PrepareZoneRemovalTask(
	ctx context.Context,
	task etcd.TaskRecord,
	intent etcd.ZoneRemovalIntent,
	procedure ZoneRemovalTaskProcedureIDs,
) (etcd.TaskRecord, error) {
	if resolver == nil || ctx == nil || task.Executor != etcd.TaskExecutorAgent ||
		task.Type != etcd.TaskRemove || task.Target != intent.ZoneID || task.PlanID == "" ||
		ids.Validate(ids.KindConfig, procedure.ArtifactID) != nil ||
		ids.Validate(ids.KindStep, procedure.NetworkStepID) != nil ||
		len(procedure.ServiceStepIDs) != len(intent.AffectedServiceIDs) ||
		intent.CandidateProjection.RenderGeneration > math.MaxInt32 {
		return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Zone removal Task preparation is invalid")
	}
	prepared := task
	prepared.RenderGeneration = int32(intent.CandidateProjection.RenderGeneration)
	prepared.Params = map[string]string{
		etcd.TaskZoneEnvironmentParam:        intent.EnvironmentID,
		etcd.TaskZoneRemovalOperationParam:   intent.OperationID,
		etcd.EnvironmentDesiredRevisionParam: intent.Claim.RevisionID,
		etcd.TaskComposeArtifactParam:        procedure.ArtifactID,
	}
	prepared.Steps = make([]etcd.TaskStepRecord, 0, len(procedure.ServiceStepIDs)+1)
	for _, stepID := range procedure.ServiceStepIDs {
		if ids.Validate(ids.KindStep, stepID) != nil {
			return etcd.TaskRecord{}, errs.New(errs.KindValidationFailed, "Zone removal Service step id is invalid")
		}
		prepared.Steps = append(prepared.Steps, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: stepID})
	}
	prepared.Steps = append(prepared.Steps, etcd.TaskStepRecord{Kind: etcd.TaskStepOperation, ID: procedure.NetworkStepID})
	plan, err := resolver.buildZoneRemovalPlan(prepared, intent)
	if err != nil {
		return etcd.TaskRecord{}, err
	}
	prepared.PlanHash = hex.EncodeToString(plan.PlanHash)
	return prepared, nil
}

func (resolver *TaskPlanResolver) resolveZoneRemovalPlan(ctx context.Context, task etcd.TaskRecord) (*agentpb.ExecutionPlan, error) {
	reader, ok := resolver.blueprints.(zoneRemovalPlanReader)
	if !ok || reader == nil {
		return nil, errs.New(errs.KindInternal, "Zone removal intent reader is not configured")
	}
	operationID := task.Params[etcd.TaskZoneRemovalOperationParam]
	stored, found, err := reader.GetZoneRemovalIntent(ctx, operationID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, errs.New(errs.KindInternal, "Zone removal intent was not found")
	}
	return resolver.buildZoneRemovalPlan(task, stored.Record)
}

func (resolver *TaskPlanResolver) buildZoneRemovalPlan(task etcd.TaskRecord, intent etcd.ZoneRemovalIntent) (*agentpb.ExecutionPlan, error) {
	if resolver == nil || task.Executor != etcd.TaskExecutorAgent || task.Type != etcd.TaskRemove ||
		task.Target != intent.ZoneID || task.ID != intent.ActiveTaskID || intent.Status != etcd.TaskStatusPending ||
		len(task.Params) != 4 || len(task.Materializations) != 0 ||
		len(task.Steps) != len(intent.AffectedServiceIDs)+1 || task.TimeoutSeconds <= 0 ||
		task.TimeoutSeconds > math.MaxUint32 || uint64(task.RenderGeneration) != intent.CandidateProjection.RenderGeneration ||
		task.Params[etcd.TaskZoneEnvironmentParam] != intent.EnvironmentID ||
		task.Params[etcd.TaskZoneRemovalOperationParam] != intent.OperationID ||
		task.Params[etcd.EnvironmentDesiredRevisionParam] != intent.Claim.RevisionID {
		return nil, errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	artifactID := task.Params[etcd.TaskComposeArtifactParam]
	artifact := &agentpb.ComposeArtifact{}
	if ids.Validate(ids.KindConfig, artifactID) != nil ||
		(proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(intent.CandidateProjection.ComposeArtifact, artifact) != nil ||
		artifact.GetArtifactId() != artifactID {
		return nil, errs.New(errs.KindInternal, "Zone removal candidate artifact changed")
	}
	steps := make([]*agentpb.ExecutionStep, 0, len(task.Steps))
	previous := ""
	for index, serviceID := range intent.AffectedServiceIDs {
		if ids.Validate(ids.KindService, serviceID) != nil || ids.Validate(ids.KindStep, task.Steps[index].ID) != nil {
			return nil, errs.New(errs.KindInternal, "Zone removal Service procedure changed")
		}
		step := &agentpb.ExecutionStep{
			StepId: task.Steps[index].ID, TimeoutSeconds: uint32(task.TimeoutSeconds), PrerequisiteStepId: previous,
			Payload: &agentpb.ExecutionStep_ComposeApply{ComposeApply: &agentpb.ComposeApply{
				ArtifactId: artifactID, ServiceIds: []string{serviceID}, ForceRecreate: true, NoDependencies: true,
			}},
		}
		steps = append(steps, step)
		previous = step.StepId
	}
	networkStep := task.Steps[len(task.Steps)-1]
	if ids.Validate(ids.KindStep, networkStep.ID) != nil {
		return nil, errs.New(errs.KindInternal, "Zone removal network procedure changed")
	}
	steps = append(steps, &agentpb.ExecutionStep{
		StepId: networkStep.ID, TimeoutSeconds: uint32(task.TimeoutSeconds), PrerequisiteStepId: previous,
		Payload: &agentpb.ExecutionStep_ManagedNetworkRemove{ManagedNetworkRemove: &agentpb.ManagedNetworkRemove{
			NetworkId: intent.ZoneID, EnvironmentId: intent.EnvironmentID, DockerName: "gp_net_" + strings.ToLower(task.Target),
		}},
	})
	return BuildPlan(PlanBuildInput{
		VolumeRoot: resolver.volumeRoot, PlanID: task.PlanID,
		RenderGeneration: uint64(task.RenderGeneration), Operation: agentpb.PlanOperation_PLAN_OPERATION_REMOVE,
		TargetID: task.Target, Artifacts: []*agentpb.ComposeArtifact{artifact}, Steps: steps,
	})
}
