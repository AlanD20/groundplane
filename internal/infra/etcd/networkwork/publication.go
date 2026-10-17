package networkwork

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreparePublication stages immutable input and prepares the visible direct
// removal Task. The Environment publisher must atomically join its revision,
// impact checks, deletion fence and replay marker. Staging grants no execution.
func (repository *InputRepository) PreparePublication(ctx context.Context, task workexecution.Task,
	item workexecution.Item, input *agentpb.NetworkWorkInput, marker idempotency.IdempotencyMarker,
	admission []keyvalue.Condition, desiredRevision string,
) (workqueue.Change, error) {
	if err := executionplan.ValidateNetworkWorkInput(input); err != nil {
		return workqueue.Change{}, err
	}
	if task.Action != workexecution.ActionRemove || input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE ||
		item.Operation != workexecution.Network || item.Input == nil || item.Phase != workexecution.Ready ||
		item.Placement.Executor != workexecution.Agent || item.TargetID != input.NetworkId ||
		task.TargetID != input.NetworkId || task.Scope.EnvironmentID != input.EnvironmentId ||
		task.Intent.SHA256 != hex.EncodeToString(input.IntentSha256) ||
		marker.Intent.CiphertextDigest != task.Intent.SHA256 || len(item.Dependencies) != 0 || input.RemovalSubnet == "" {
		return workqueue.Change{}, conflict("Zone publication differs from its captured removal")
	}
	want := []workexecution.Claim{{ScopeID: input.EnvironmentId},
		{HostID: item.Placement.HostID, ScopeID: input.EnvironmentId},
		{HostID: item.Placement.HostID, ScopeID: input.NetworkId}}
	if len(item.Claims) != len(want) {
		return workqueue.Change{}, conflict("Zone removal requires its complete resource claims")
	}
	for _, claim := range want {
		if !slices.Contains(item.Claims, claim) {
			return workqueue.Change{}, conflict("Zone removal is missing a resource claim")
		}
	}
	desired, err := idempotency.EncodeTaskReference(desiredRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return workqueue.Change{}, err
	}
	_, raw, expected, err := executionplan.CaptureNetworkWorkInput(item.Input.ID, input)
	clear(raw)
	if err != nil || expected != *item.Input {
		keyvalue.ZeroMutationBytes(change.Mutations)
		if err != nil {
			return workqueue.Change{}, err
		}
		return workqueue.Change{}, conflict("Zone removal reference differs from its captured input")
	}
	reference, captured, err := repository.Stage(ctx, task.ID, item.ID, item.Input.ID, input, admission)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	if reference != *item.Input {
		keyvalue.ZeroMutationBytes(change.Mutations)
		keyvalue.ZeroMutationBytes(captured.Mutations)
		return workqueue.Change{}, conflict("Zone removal staged different executable input")
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: DesiredKey(item.ID)})
	change.Mutations = append(change.Mutations, captured.Mutations...)
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: DesiredKey(item.ID), Value: desired})
	return change, nil
}
