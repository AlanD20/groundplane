package networkwork

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

// Callers must prove no effects started or that recovery settled all effects.
// Releasing the fence keeps the bridge reservation. Explicit Retry reacquires
// captured authority under the unchanged desired-head guard.
func (repository *InputRepository) prepareRemovalFenceRelease(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	if view.Task.Task.Action != workexecution.ActionRemove {
		return workqueue.Change{}, nil
	}
	key := deletions.TombstoneKey(string(deletions.DeletionTargetZone), view.Item.Item.TargetID)
	read, err := repository.read(ctx, []string{key}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[0] == nil {
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key}}}, nil
	}
	fence, err := deletions.DecodeDeletionTombstone(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if !matchesRemovalFence(view, fence) {
		return workqueue.Change{}, conflict("unassigned Zone removal does not own its deletion fence")
	}
	if view.Task.Task.Scope.TenantID == "" && view.Task.Task.ExpectedItems > 1 {
		// This fence also owns consumer removals. Stopping the bridge says
		// nothing about an earlier Detach's external effects; the cascade
		// owner must account for the whole graph before releasing it.
		return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}}}, nil
	}
	return workqueue.Change{Conditions: []keyvalue.Condition{{Key: key, ModRevision: read.Values[0].ModRevision}},
		Mutations: []keyvalue.Mutation{{Type: keyvalue.MutationDelete, Key: key}}}, nil
}

func (repository *InputRepository) prepareRemovalRetryFence(ctx context.Context, view workqueue.View) (workqueue.Change, error) {
	if view.Task.Task.Action != workexecution.ActionRemove {
		return workqueue.Change{}, nil
	}
	item := view.Item.Item
	key := deletions.TombstoneKey(string(deletions.DeletionTargetZone), item.TargetID)
	authorityKey, receiptKey := RemovalAuthorityKey(item.ID), removalFinalizationKey(item.ID)
	read, err := repository.read(ctx, []string{key, authorityKey, receiptKey}, view.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if read.Values[1] == nil {
		return workqueue.Change{}, conflict("Zone Retry has no captured deletion authority")
	}
	authority, err := deletions.DecodeDeletionTombstone(read.Values[1].Value)
	if err != nil || !matchesRemovalFence(view, authority) {
		return workqueue.Change{}, conflict("Zone Retry deletion authority belongs to another action")
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[0])},
		{Key: authorityKey, ModRevision: read.Values[1].ModRevision},
		{Key: receiptKey, ModRevision: keyvalue.RevisionOf(read.Values[2])},
	}}
	if saved := read.Values[0]; saved != nil {
		fence, err := deletions.DecodeDeletionTombstone(saved.Value)
		if err != nil || !matchesRemovalFence(view, fence) || fence.TargetRevision != authority.TargetRevision {
			return workqueue.Change{}, conflict("Zone Retry cannot replace another deletion fence")
		}
		return change, nil
	}
	if read.Values[2] != nil {
		receipt, err := recordcodec.Decode[removalFinalization](read.Values[2].Value, "network-removal-finalization")
		if err != nil || item.Input == nil || receipt.TaskID != view.Task.Task.ID ||
			receipt.WorkID != item.ID || receipt.NetworkID != item.TargetID || receipt.Input != *item.Input {
			return workqueue.Change{}, conflict("Zone Retry finalization receipt differs from its action")
		}
		return change, nil
	}
	change.Mutations = []keyvalue.Mutation{{Type: keyvalue.MutationPut, Key: key, Value: append([]byte(nil), read.Values[1].Value...)}}
	return change, nil
}

func matchesRemovalFence(view workqueue.View, fence deletions.DeletionTombstoneRecord) bool {
	return fence.TaskID == view.Task.Task.ID && fence.TargetID == view.Item.Item.TargetID &&
		fence.TargetKind == deletions.DeletionTargetZone && fence.Phase == deletions.DeletionPhaseHostEffects &&
		fence.Checkpoint == (deletions.DeletionCheckpoint{}) && fence.CreatedAt.Equal(view.Task.Task.CreatedAt)
}
