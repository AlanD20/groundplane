package networkwork

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// CaptureRemovalInput uses stable ownership, never a desired declaration that
// has already been omitted. The caller must join the accepted removal graph and
// completed consumer dependencies to publication of this input.
func (repository *InputRepository) CaptureRemovalInput(
	ctx context.Context,
	view workqueue.View,
) (*agentpb.NetworkWorkInput, []keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || item.Operation != workexecution.Network ||
		item.TaskID != task.ID || item.Placement.Executor != workexecution.Agent ||
		view.ReadRevision <= 0 {
		return nil, nil, conflict("Network removal requires its captured Blueprint work")
	}
	name, err := networkname.New(item.TargetID)
	if err != nil {
		return nil, nil, err
	}
	intent, err := hex.DecodeString(task.Intent.SHA256)
	if err != nil {
		return nil, nil, conflict("Network removal intent digest is invalid")
	}
	key := AppliedKey(item.TargetID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return nil, nil, err
	}
	defer keyvalue.ClearValues(read.Values)
	var revision int64
	if value := read.Values[0]; value != nil {
		applied, err := decodeApplied(value.Value)
		if err != nil {
			return nil, nil, err
		}
		if applied.NetworkID != item.TargetID || applied.EnvironmentID != task.Scope.EnvironmentID ||
			applied.HostID != item.Placement.HostID || applied.DockerName != name {
			return nil, nil, conflict("Network removal differs from its applied host ownership")
		}
		revision = value.ModRevision
	}
	input := &agentpb.NetworkWorkInput{Action: agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE,
		EnvironmentId: task.Scope.EnvironmentID, NetworkId: item.TargetID, DockerName: name, IntentSha256: intent}
	if err := executionplan.ValidateNetworkWorkInput(input); err != nil {
		return nil, nil, err
	}
	return input, []keyvalue.Condition{{Key: key, ModRevision: revision}}, nil
}
