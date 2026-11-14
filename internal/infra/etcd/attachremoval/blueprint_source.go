package attachremoval

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runtimeprovenance"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
)

func (repository *InputRepository) LoadBlueprintInput(ctx context.Context, view workqueue.View,
) (BlueprintInput, keyvalue.Condition, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionApply || item.Operation != workexecution.Detach ||
		item.TaskID != task.ID || view.ReadRevision <= 0 {
		return BlueprintInput{}, keyvalue.Condition{}, checkpointConflict("Detach has no captured Apply owner")
	}
	key := BlueprintInputKey(task.Scope.EnvironmentID, task.ID, item.ID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return BlueprintInput{}, keyvalue.Condition{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	value := read.Values[0]
	if value == nil {
		return BlueprintInput{}, keyvalue.Condition{}, checkpointConflict(
			"Blueprint Detach admission capture is unavailable",
		)
	}
	input, err := DecodeBlueprintInput(ctx, value.Value)
	if err != nil {
		return BlueprintInput{}, keyvalue.Condition{}, err
	}
	if input.TaskID != task.ID || input.WorkItemID != item.ID || input.AttachID != item.TargetID ||
		input.EnvironmentID != task.Scope.EnvironmentID || input.OperationID != DetachOperation(view) ||
		input.HasHostEffects() != (item.Placement.Executor == workexecution.Agent) {
		input.Clear()
		return BlueprintInput{}, keyvalue.Condition{}, checkpointConflict(
			"Blueprint Detach differs from its admission capture",
		)
	}
	return input, keyvalue.Condition{Key: key, ModRevision: value.ModRevision}, nil
}

// RefreshBlueprintSnapshot derives only consumer state changed by captured
// prerequisite removals. Operator-owned facts, backing configuration and hook
// envelopes remain the admission input, not a new scheduler-time selection.
func (repository *InputRepository) RefreshBlueprintSnapshot(ctx context.Context, view workqueue.View,
	frozen BlueprintInput, graph []workexecution.Item,
) (_ DetachSnapshot, _ []keyvalue.Condition, resultErr error) {
	if frozen.Snapshot == nil {
		return DetachSnapshot{}, nil, checkpointConflict("Declared-only Detach has no host snapshot")
	}
	current, err := ReadDetachSnapshot(ctx, repository.store, view.ReadRevision, frozen.AttachID)
	if err != nil {
		return DetachSnapshot{}, nil, err
	}
	defer func() {
		if resultErr != nil {
			current.Clear()
		}
	}()
	comparison := current.Source
	comparison.SnapshotRevision = frozen.Snapshot.Source.SnapshotRevision
	actual, err := Digest(comparison)
	if err != nil {
		return DetachSnapshot{}, nil, err
	}
	expected, err := Digest(frozen.Snapshot.Source)
	if err != nil {
		return DetachSnapshot{}, nil, err
	}
	if actual != expected || current.ServiceRuntimeRevision != frozen.Snapshot.ServiceRuntimeRevision ||
		current.RuntimeIntent != frozen.Snapshot.RuntimeIntent || current.LifecycleTaskID != frozen.Snapshot.LifecycleTaskID ||
		current.ReleaseProjectionRevision != frozen.Snapshot.ReleaseProjectionRevision {
		return DetachSnapshot{}, nil, checkpointConflict(
			"Detach source changed after Apply admission; submit a new Apply",
		)
	}
	guards, err := repository.blueprintPeerRemovalConditions(ctx, view, frozen, graph)
	if err != nil {
		return DetachSnapshot{}, nil, err
	}
	if !sameBlueprintConsumerMembership(*frozen.Snapshot, current, frozen.RemovedBefore) {
		return DetachSnapshot{}, nil, checkpointConflict(
			"Detach consumer connections changed outside its captured predecessors",
		)
	}
	if current.RuntimeRevision != frozen.Snapshot.RuntimeRevision {
		if current.Runtime == nil {
			return DetachSnapshot{}, nil, checkpointConflict("Detach consumer runtime disappeared after admission")
		}
		proof, matched, err := runtimeprovenance.PredecessorConditions(ctx, repository.store, view,
			keyvalue.Versioned[serviceruntimerecord.Record]{Record: *current.Runtime, Revision: current.RuntimeRevision,
				ReadRevision: view.ReadRevision}, graph)
		if err != nil {
			return DetachSnapshot{}, nil, err
		}
		if !matched {
			return DetachSnapshot{}, nil, checkpointConflict("Detach consumer runtime changed outside this Apply")
		}
		guards = append(guards, proof...)
	}
	return current, append(guards, current.ConsumerConditions()...), nil
}

func sameBlueprintConsumerMembership(frozen, current DetachSnapshot, removed []string) bool {
	target := frozen.Source.Attach
	selectMembers := func(snapshot DetachSnapshot, exclude bool) []Membership {
		var members []Membership
		for _, member := range snapshot.Members {
			if member.ServiceID == target.ServiceID && member.NetworkID == target.BackingNetworkID &&
				(!exclude || !slices.Contains(removed, member.AttachID)) {
				members = append(members, member)
			}
		}
		return members
	}
	return slices.Equal(selectMembers(frozen, true), selectMembers(current, false))
}

func (repository *InputRepository) blueprintPeerRemovalConditions(ctx context.Context, view workqueue.View,
	frozen BlueprintInput, graph []workexecution.Item,
) ([]keyvalue.Condition, error) {
	guards := make([]keyvalue.Condition, 0, len(frozen.RemovedBefore))
	for _, id := range frozen.RemovedBefore {
		var captured *workexecution.Item
		for index := range graph {
			if graph[index].TargetID == id && graph[index].Operation == workexecution.Detach {
				captured = &graph[index]
				break
			}
		}
		if captured == nil || captured.TaskID != view.Task.Task.ID {
			return nil, checkpointConflict("Detach peer removal is absent from its captured graph")
		}

		ancestor, err := runtimeprovenance.CapturedPrerequisite(view, captured.ID, graph)
		if err != nil {
			return nil, err
		}
		if !ancestor {
			return nil, checkpointConflict("Detach peer is not a captured prerequisite")
		}
		current, err := repository.transactions.ReadWork(ctx, view.Task.Task.ID, captured.ID, view.ReadRevision)
		if err != nil {
			return nil, err
		}
		item := current.Item.Item
		if item.TargetID != id || item.Operation != captured.Operation || item.Placement != captured.Placement ||
			!slices.Equal(item.Dependencies, captured.Dependencies) || !slices.Equal(item.Claims, captured.Claims) ||
			item.Phase != workexecution.Settled || item.Outcome != workexecution.Succeeded ||
			item.Result == nil || item.ActiveAttemptID != "" || item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete {
			return nil, checkpointConflict("Detach peer removal has no settled result")
		}
		guards = append(guards, keyvalue.Condition{Key: workqueue.ItemKey(item.ID), ModRevision: current.Item.Revision})

	}
	return guards, nil
}
