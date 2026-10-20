package volume

import (
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/volumeexecution"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func buildVolumeWork(scope workexecution.Scope, placement workexecution.Placement,
	claim blueprints.EnvironmentBlueprintStageClaim, request volumeMutationRequest,
	baseline, candidate *agentpb.ComposeArtifact, intent []byte, consumers []string,
) (workexecution.Task, workexecution.Item, *agentpb.VolumeWorkInput, error) {
	input := &agentpb.VolumeWorkInput{EnvironmentId: request.environmentID, VolumeId: request.volumeID,
		ComposeKey: request.key, IntentSha256: append([]byte(nil), intent...), Candidate: proto.CloneOf(candidate)}
	action := workexecution.ActionCreate
	switch request.action {
	case volumeMutationActionAdd:
		input.Action = agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE
	case volumeMutationActionEdit:
		action, input.Action = workexecution.ActionUpdate, agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY
	case volumeMutationActionRemove:
		action = workexecution.ActionRemove
		input.Action = agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE
		input.Baseline, input.ConsumerIds = proto.CloneOf(baseline), append([]string(nil), consumers...)
		digest, err := hex.DecodeString(request.impactToken)
		if err != nil {
			return workexecution.Task{}, workexecution.Item{}, nil, err
		}
		input.RemovalImpactSha256 = digest
	default:
		return workexecution.Task{}, workexecution.Item{}, nil, errs.New(
			errs.KindValidationFailed,
			"Volume mutation action is invalid",
		)
	}
	workID := ids.DeriveAt(ids.KindWorkItem, claim.CreatedAt, claim.TaskID, "volume")
	timeout, err := volumeexecution.ExecutionBudget(input.Action)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	inputID := ids.DeriveAt(ids.KindPlan, claim.CreatedAt, workID, "captured-input")
	owned, raw, reference, err := executionplan.CaptureVolumeWorkInput(inputID, input)
	clear(raw)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	task := workexecution.Task{ID: claim.TaskID, Actor: workexecution.OperatorActor, Scope: scope,
		Action: action, TargetID: request.volumeID, TargetName: request.slug,
		Intent: workexecution.Input{
			ID:     ids.DeriveAt(ids.KindPlan, claim.CreatedAt, claim.TaskID, "request-intent"),
			SHA256: claim.Intent.CiphertextDigest,
		},
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt, Deadline: claim.CreatedAt.Add(timeout), ExpectedItems: 1}
	item := workexecution.Item{ID: workID, TaskID: task.ID, Operation: workexecution.Volume, TargetID: request.volumeID,
		TargetName: task.TargetName,
		Placement:  placement, Input: &reference, Phase: workexecution.Ready, Cleanup: workexecution.CleanupNone,
		CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
		Claims: []workexecution.Claim{{ScopeID: request.environmentID},
			{
				HostID:  placement.HostID,
				ScopeID: request.environmentID,
			}, {HostID: placement.HostID, ScopeID: request.volumeID}}}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	return task, item, owned, nil
}
