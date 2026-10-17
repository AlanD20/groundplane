package network

import (
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/internal/common/networkexecution"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// buildZoneRemovalWork captures one direct Zone operation. Existing consumers
// lose only this network connection; pending Service edits are never deployed
// as an incidental part of removing a Zone.
func buildZoneRemovalWork(scope workexecution.Scope, placement workexecution.Placement,
	claim blueprints.EnvironmentBlueprintStageClaim, zoneID, name string, consumers []string,
) (workexecution.Task, workexecution.Item, *agentpb.NetworkWorkInput, error) {
	if scope.EnvironmentID != claim.EnvironmentID || claim.TaskID != claim.RevisionID ||
		placement.Executor != workexecution.Agent {
		return workexecution.Task{}, workexecution.Item{}, nil, errs.New(errs.KindValidationFailed, "Zone removal requires its captured Environment and host")
	}
	dockerName, err := networkname.New(zoneID)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	intent, err := hex.DecodeString(claim.Intent.CiphertextDigest)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, errs.New(errs.KindValidationFailed, "Zone removal intent digest is invalid")
	}
	consumerIDs := slices.Clone(consumers)
	slices.Sort(consumerIDs)
	input := &agentpb.NetworkWorkInput{Action: agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE,
		EnvironmentId: scope.EnvironmentID, NetworkId: zoneID, DockerName: dockerName,
		IntentSha256: intent, DisconnectServiceIds: consumerIDs}
	workID := ids.DeriveAt(ids.KindWorkItem, claim.CreatedAt, claim.TaskID, "network")
	inputID := ids.DeriveAt(ids.KindPlan, claim.CreatedAt, workID, "captured-input")
	owned, raw, reference, err := executionplan.CaptureNetworkWorkInput(inputID, input)
	clear(raw)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	budget, err := networkexecution.ExecutionBudget(input.Action)
	if err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	task := workexecution.Task{ID: claim.TaskID, Actor: workexecution.OperatorActor, Scope: scope,
		Action: workexecution.ActionRemove, TargetID: zoneID, TargetName: name,
		Intent: workexecution.Input{ID: ids.DeriveAt(ids.KindPlan, claim.CreatedAt, claim.TaskID, "request-intent"),
			SHA256: claim.Intent.CiphertextDigest},
		CreatedAt: claim.CreatedAt, UpdatedAt: claim.CreatedAt, Deadline: claim.CreatedAt.Add(budget), ExpectedItems: 1}
	item := workexecution.Item{ID: workID, TaskID: task.ID, Operation: workexecution.Network,
		TargetID: zoneID, Placement: placement, Input: &reference, Phase: workexecution.Ready,
		Cleanup: workexecution.CleanupNone, CreatedAt: task.CreatedAt, UpdatedAt: task.CreatedAt, Deadline: task.Deadline,
		Claims: []workexecution.Claim{{ScopeID: scope.EnvironmentID},
			{HostID: placement.HostID, ScopeID: scope.EnvironmentID},
			{HostID: placement.HostID, ScopeID: zoneID}}}
	if err := workexecution.ValidateGraph(task, []workexecution.Item{item}); err != nil {
		return workexecution.Task{}, workexecution.Item{}, nil, err
	}
	return task, item, owned, nil
}
