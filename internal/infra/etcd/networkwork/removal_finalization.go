package networkwork

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/workexecution"
	"github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

type removalFinalization struct {
	TaskID    string              `json:"task_id"`
	WorkID    string              `json:"work_id"`
	NetworkID string              `json:"network_id"`
	Input     workexecution.Input `json:"input"`
}

func RemovalAuthorityKey(workID string) string { return InputPrefix(workID) + "removal-authority" }

func removalFinalizationKey(workID string) string {
	return InputPrefix(workID) + "removal-finalization"
}

// prepareDirectRemovalFinalization joins an acknowledged absence proof. The
// bridge reservation remains held through an interrupted or failed mutation
// until forward completion or recovery has proved that exact bridge absent.
func (repository *InputRepository) prepareDirectRemovalFinalization(ctx context.Context,
	view workqueue.ResumeView, input *agentpb.NetworkWorkInput,
) (workqueue.Change, error) {
	task, item := view.Work.Task.Task, view.Work.Item.Item
	if task.Action != workexecution.ActionRemove || input.Action != agentpb.NetworkWorkAction_NETWORK_WORK_ACTION_REMOVE ||
		input.RemovalSubnet == "" || item.Operation != workexecution.Network || item.TargetID != task.TargetID ||
		input.NetworkId != task.TargetID || input.EnvironmentId != task.Scope.EnvironmentID {
		return workqueue.Change{}, conflict("direct Zone finalization requires its captured removal")
	}
	tombstoneKey := deletions.TombstoneKey(string(deletions.DeletionTargetZone), input.NetworkId)
	poolKey := networkreservations.ZonePoolRegistryKey(input.EnvironmentId)
	addressesKey := networkreservations.ComponentAddressRegistryKey(input.NetworkId)
	receiptKey := removalFinalizationKey(item.ID)
	read, err := repository.read(ctx, []string{tombstoneKey, poolKey, addressesKey, receiptKey}, view.Work.ReadRevision)
	if err != nil {
		return workqueue.Change{}, err
	}
	defer keyvalue.ClearValues(read.Values)
	if saved := read.Values[3]; saved != nil {
		receipt, err := recordcodec.Decode[removalFinalization](saved.Value, "network-removal-finalization")
		if err != nil || item.Input == nil || receipt.TaskID != task.ID || receipt.WorkID != item.ID ||
			receipt.NetworkID != input.NetworkId || receipt.Input != *item.Input ||
			read.Values[0] != nil || read.Values[2] != nil {
			return workqueue.Change{}, conflict("Zone finalization replay differs from its saved removal")
		}
		if current := read.Values[1]; current != nil {
			pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](current.Value, "zone_pool_registry")
			if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil || pool.Reservations[input.NetworkId] != "" {
				return workqueue.Change{}, conflict("removed Zone has acquired a new subnet reservation")
			}
		}
		return workqueue.Change{Conditions: []keyvalue.Condition{
			{Key: receiptKey, ModRevision: saved.ModRevision},
			{Key: tombstoneKey}, {Key: addressesKey},
			{Key: poolKey, ModRevision: keyvalue.RevisionOf(read.Values[1])},
		}}, nil
	}
	if read.Values[0] == nil || read.Values[1] == nil {
		return workqueue.Change{}, conflict("Zone removal lost its deletion fence or subnet reservation")
	}
	tombstone, err := deletions.DecodeDeletionTombstone(read.Values[0].Value)
	if err != nil {
		return workqueue.Change{}, err
	}
	if tombstone.TaskID != task.ID || tombstone.TargetID != input.NetworkId ||
		tombstone.TargetKind != deletions.DeletionTargetZone || tombstone.Phase != deletions.DeletionPhaseHostEffects ||
		tombstone.Checkpoint != (deletions.DeletionCheckpoint{}) || !tombstone.CreatedAt.Equal(task.CreatedAt) {
		return workqueue.Change{}, conflict("Zone deletion fence belongs to a different operation")
	}
	pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](read.Values[1].Value, "zone_pool_registry")
	if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil ||
		pool.Reservations[input.NetworkId] != input.RemovalSubnet {
		return workqueue.Change{}, conflict("Zone removal subnet reservation changed")
	}
	if read.Values[2] != nil {
		addresses, err := recordcodec.Decode[networkreservations.ComponentAddressRegistry](read.Values[2].Value, "component_address_registry")
		if err != nil {
			return workqueue.Change{}, err
		}
		if len(addresses.Reservations) != 0 {
			return workqueue.Change{}, conflict("Zone removal gained a Component address reservation")
		}
		for serviceID := range addresses.ServiceReservations {
			if _, found := slices.BinarySearch(input.DisconnectServiceIds, serviceID); !found {
				return workqueue.Change{}, conflict("Zone address belongs to a Service outside this removal")
			}
		}
	}
	delete(pool.Reservations, input.NetworkId)
	if item.Input == nil {
		return workqueue.Change{}, conflict("Zone finalization has no captured input")
	}
	receipt, err := recordcodec.Encode("network-removal-finalization", removalFinalization{
		TaskID: task.ID, WorkID: item.ID, NetworkID: input.NetworkId, Input: *item.Input})
	if err != nil {
		return workqueue.Change{}, err
	}
	change := workqueue.Change{Conditions: []keyvalue.Condition{
		{Key: receiptKey},
		{Key: tombstoneKey, ModRevision: read.Values[0].ModRevision},
		{Key: poolKey, ModRevision: read.Values[1].ModRevision},
		{Key: addressesKey, ModRevision: keyvalue.RevisionOf(read.Values[2])},
		{Key: workqueue.TaskKey(task.ID), ModRevision: view.Work.Task.Revision},
		{Key: workqueue.ItemKey(item.ID), ModRevision: view.Work.Item.Revision},
		{Key: workqueue.AttemptKey(view.Attempt.Attempt.ID), ModRevision: view.Attempt.Revision},
	}, Mutations: []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: receiptKey, Value: receipt},
		{Type: keyvalue.MutationDelete, Key: tombstoneKey},
		{Type: keyvalue.MutationDelete, Key: addressesKey},
	}}
	if len(pool.Reservations) == 0 {
		change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationDelete, Key: poolKey})
		return change, nil
	}
	value, err := recordcodec.Encode("zone_pool_registry", pool)
	if err != nil {
		keyvalue.ZeroMutationBytes(change.Mutations)
		return workqueue.Change{}, err
	}
	change.Mutations = append(change.Mutations, keyvalue.Mutation{Type: keyvalue.MutationPut, Key: poolKey, Value: value})
	return change, nil
}
