package volumework

import (
	"context"
	"encoding/hex"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

// PreparePublication captures the executable input and prepares one visible
// Task with one native Volume work item. The Environment publisher joins this
// change with its desired revision, impact/policy guards and replay marker.
// It cannot publish a Task independently from those resource decisions.
func (repository *InputRepository) PreparePublication(ctx context.Context, task workexecution.Task,
	item workexecution.Item, input *agentpb.VolumeWorkInput, marker idempotency.IdempotencyMarker,
	admission []keyvalue.Condition, desiredRevision string,
) (workqueue.Change, error) {
	desired, err := idempotency.EncodeTaskReference(desiredRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	if err := executionplan.ValidateVolumeWorkInput(input); err != nil {
		return workqueue.Change{}, err
	}
	if item.Operation != workexecution.Volume || item.Input == nil || item.Phase != workexecution.Ready ||
		item.Placement.Executor != workexecution.Agent || item.TargetID != input.VolumeId ||
		task.TargetID != input.VolumeId || task.Scope.EnvironmentID != input.EnvironmentId ||
		task.Intent.SHA256 != hex.EncodeToString(input.IntentSha256) ||
		marker.Intent.CiphertextDigest != task.Intent.SHA256 || len(item.Dependencies) != 0 {
		return workqueue.Change{}, conflict("Volume publication does not match its captured operator action")
	}
	var action workexecution.Action
	switch input.Action {
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_CREATE:
		action = workexecution.ActionCreate
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_VERIFY:
		action = workexecution.ActionUpdate
	case agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE:
		action = workexecution.ActionRemove
	}
	if task.Action != action {
		return workqueue.Change{}, conflict("Volume action differs from its captured operation")
	}
	// Consumer detachment mutates the Environment's captured Compose deployment.
	// Keep both its control-plane and host-local scope, plus the Volume itself,
	// owned until the operation has accounted for its effects.
	want := []workexecution.Claim{{ScopeID: input.EnvironmentId},
		{HostID: item.Placement.HostID, ScopeID: input.EnvironmentId},
		{HostID: item.Placement.HostID, ScopeID: input.VolumeId}}
	if len(item.Claims) != len(want) {
		return workqueue.Change{}, conflict("Volume publication requires its complete resource claims")
	}
	for _, required := range want {
		found := false
		for _, claim := range item.Claims {
			if claim == required {
				found = true
				break
			}
		}
		if !found {
			return workqueue.Change{}, conflict("Volume publication is missing a resource claim")
		}
	}
	change, err := workqueue.PrepareSingleTask(task, item, marker)
	if err != nil {
		return workqueue.Change{}, err
	}
	ownership, err := prepareRemovalOwnership(task, item)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, ownership.Conditions...)
	change.Mutations = append(change.Mutations, ownership.Mutations...)
	_, raw, expected, err := executionplan.CaptureVolumeWorkInput(item.Input.ID, input)
	clear(raw)
	if err != nil || expected != *item.Input {
		keyvalue.ZeroMutationBytes(change.Mutations)
		if err != nil {
			return workqueue.Change{}, err
		}
		return workqueue.Change{}, conflict("Volume work reference differs from its captured input")
	}
	reference, captured, err := repository.Stage(ctx, task.ID, item.ID, item.Input.ID, input, admission)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	if reference != *item.Input {
		keyvalue.ZeroMutationBytes(change.Mutations)
		keyvalue.ZeroMutationBytes(captured.Mutations)
		return workqueue.Change{}, conflict("Volume published input differs from its work reference")
	}
	change.Conditions = append(change.Conditions, captured.Conditions...)
	change.Mutations = append(change.Mutations, captured.Mutations...)
	change.Conditions = append(change.Conditions, keyvalue.Condition{Key: desiredKey(item.ID)})
	change.Mutations = append(
		change.Mutations,
		keyvalue.Mutation{Type: keyvalue.MutationPut, Key: desiredKey(item.ID), Value: desired},
	)
	return change, nil
}
