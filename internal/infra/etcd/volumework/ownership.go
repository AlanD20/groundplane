package volumework

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func removalOwner(task workexecution.Task, item workexecution.Item) removalrecord.Owner {
	return removalrecord.Owner{VolumeID: item.TargetID, EnvironmentID: task.Scope.EnvironmentID,
		TaskID: task.ID, WorkItemID: item.ID}
}

// Admission owns both identities before the work can be assigned. Script
// capture compares the Volume key; desired writers and deletion compare the
// Environment key. Unknown effects therefore cannot lose protection merely
// because an executor slot was released or diagnostic history expired.
func prepareRemovalOwnership(task workexecution.Task, item workexecution.Item) (workqueue.Change, error) {
	if task.Action != workexecution.ActionRemove {
		return workqueue.Change{}, nil
	}
	if task.Validate() != nil || item.Validate() != nil || item.TaskID != task.ID ||
		item.Operation != workexecution.Volume || task.TargetID != item.TargetID {
		return workqueue.Change{}, conflict("Volume removal ownership differs from its native work")
	}
	owner := removalOwner(task, item)
	value, err := removalrecord.EncodeOwner(owner)
	if err != nil {
		return workqueue.Change{}, err
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: removalrecord.OwnerKey(owner.VolumeID)}, {Key: removalrecord.EnvironmentLockKey(owner.EnvironmentID)},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: removalrecord.OwnerKey(owner.VolumeID), Value: value},
		{
			Type:  keyvalue.MutationPut,
			Key:   removalrecord.EnvironmentLockKey(owner.EnvironmentID),
			Value: append([]byte(nil), value...),
		},
	}}, nil
}

// Release joins visible completion only after the operation has proved its
// effects settled. A later explicit Retry must reacquire both keys atomically.
func (repository *InputRepository) PrepareOwnershipRelease(
	ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
	task, item := view.Task.Task, view.Item.Item
	if task.Action != workexecution.ActionRemove {
		return workqueue.Change{}, nil
	}
	if item.Operation != workexecution.Volume || item.TaskID != task.ID || item.TargetID != task.TargetID ||
		item.Phase != workexecution.Settled || item.ActiveAttemptID != "" || item.Result == nil ||
		(item.Cleanup != workexecution.CleanupNone && item.Cleanup != workexecution.CleanupComplete) {
		return workqueue.Change{}, conflict("Volume ownership requires settled host effects before release")
	}
	owner := removalOwner(task, item)
	keys := []string{removalrecord.OwnerKey(owner.VolumeID), removalrecord.EnvironmentLockKey(owner.EnvironmentID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{}
	for index, value := range read.Values {
		if value == nil {
			return workqueue.Change{}, conflict("Volume removal ownership is missing before settlement")
		}
		actual, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || actual != owner {
			return workqueue.Change{}, conflict("Volume removal ownership belongs to different work")
		}
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision},
		)
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: keys[index]})
	}
	return change, nil
}

// Retry preparation does not mutate storage. Its caller joins these resource
// guards and writes with the same-Task Retry and request replay transaction.
func (repository *InputRepository) PrepareRetryOwnership(
	ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
	if view.Task.Task.Action != workexecution.ActionRemove {
		return workqueue.Change{}, nil
	}
	input, _, err := repository.Load(ctx, view)
	if err != nil {
		return workqueue.Change{}, err
	}
	if input.Action != agentpb.VolumeWorkAction_VOLUME_WORK_ACTION_REMOVE {
		return workqueue.Change{}, conflict("Volume Retry removal differs from captured input")
	}
	// Cleanup Retry still holds the original ownership. Only a settled forward
	// Retry reacquires ownership released by visible completion.
	if view.Task.Task.FinishedAt == nil {
		return repository.readRemovalOwnership(ctx, view)
	}
	change, err := prepareRemovalOwnership(view.Task.Task, view.Item.Item)
	if err != nil {
		return workqueue.Change{}, err
	}
	guards, mutations, err := repository.prepareRemovalRetryGuards(ctx, view)
	if err != nil {
		keyvalue.ClearMutationValues(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Conditions = append(change.Conditions, guards...)
	change.Mutations = append(change.Mutations, mutations...)
	return change, nil
}

func (repository *InputRepository) readRemovalOwnership(
	ctx context.Context,
	view workqueue.View,
) (workqueue.Change, error) {
	owner := removalOwner(view.Task.Task, view.Item.Item)
	keys := []string{removalrecord.OwnerKey(owner.VolumeID), removalrecord.EnvironmentLockKey(owner.EnvironmentID)}
	read, err := repository.read(ctx, keys, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	change := workqueue.Change{}
	for index, value := range read.Values {
		if value == nil {
			return workqueue.Change{}, conflict("Volume cleanup lost its resource ownership")
		}
		actual, err := removalrecord.DecodeOwner(value.Value)
		if err != nil || actual != owner {
			return workqueue.Change{}, conflict("Volume cleanup ownership differs from its native work")
		}
		change.Conditions = append(
			change.Conditions,
			keyvalue.Condition{Key: keys[index], ModRevision: value.ModRevision},
		)
	}
	return change, nil
}
