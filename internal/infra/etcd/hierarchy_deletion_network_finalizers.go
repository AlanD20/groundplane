package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	hierarchydeletionplanning "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletionplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRouteFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	result, err := repository.store.Get(ctx, routerecord.ObservationKey(action.TargetID))
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	conditions := []etcdstore.Condition{{Key: routerecord.ObservationKey(action.TargetID)}}
	mutations := []etcdstore.Mutation{}
	digest := hierarchydeletion.HierarchyDeletionBytesDigest([]byte(action.TargetID))
	if result.Entry != nil {
		if result.Entry.Key != routerecord.ObservationKey(action.TargetID) {
			clear(result.Entry.Value)
			return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		if _, decodeErr := routerecord.DecodeObservation(result.Entry.Value); decodeErr != nil {
			clear(result.Entry.Value)
			return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
		}
		conditions[0].ModRevision = result.Entry.ModRevision
		digest = hierarchydeletion.HierarchyDeletionBytesDigest(result.Entry.Value)
		mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: routerecord.ObservationKey(action.TargetID)})
		clear(result.Entry.Value)
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: digest,
		conditions:       conditions,
		mutations:        mutations,
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionComponentFinalizer(
	ctx context.Context,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, componentrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := componentrecord.DecodeRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	keys := []string{
		componentrecord.EnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID),
		componentrecord.EnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.mutations = append(effects.mutations, componentrecord.WriteFenceMutation(action.TargetID))
	return effects, nil
}
func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionZoneFinalizer(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action hierarchydeletion.HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	evidence, zoneValue, err := hierarchydeletionplanning.HierarchyDeletionZoneEvidenceAtRevision(
		ctx, repository.store, action.TargetID, operation.Tombstone.SnapshotRevision, action.TargetRevision,
	)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(zoneValue)
	zone := zonerecord.Record{EnvironmentID: evidence.EnvironmentID, Desired: evidence.Desired}
	poolKey, addressesKey := networkreservations.ZonePoolRegistryKey(evidence.EnvironmentID), networkreservations.ComponentAddressRegistryKey(action.TargetID)
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		poolKey, addressesKey,
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), evidence.ZoneID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(evidence.EnvironmentID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if values == nil || len(values.Values) != 4 || values.Values[0] == nil ||
		values.Values[2] != nil || values.Values[3] != nil {
		if values != nil {
			etcdstore.ClearValues(values.Values)
		}
		return hierarchyDeletionControllerEffects{}, hierarchydeletion.CorruptHierarchyDeletion()
	}
	defer etcdstore.ClearValues(values.Values)
	pool, err := recordcodec.Decode[networkreservations.ZonePoolRegistry](values.Values[0].Value, "zone_pool_registry")
	if err != nil || networkreservations.ValidateZonePoolRegistry(pool) != nil ||
		pool.Reservations[action.TargetID] != evidence.Desired.Subnet {
		return hierarchyDeletionControllerEffects{}, networkreservations.CorruptZonePoolRegistry()
	}
	if values.Values[1] != nil {
		addresses, decodeErr := recordcodec.Decode[networkreservations.ComponentAddressRegistry](
			values.Values[1].Value,
			"component_address_registry",
		)
		if decodeErr != nil || networkreservations.ValidateComponentAddressRegistry(zone, addresses) != nil {
			return hierarchyDeletionControllerEffects{}, networkreservations.CorruptComponentAddressRegistry()
		}
		if len(addresses.Reservations) != 0 {
			return hierarchyDeletionControllerEffects{}, errs.New(errs.KindResourceInUse,
				"Zone gained a Component address reservation")
		}
	}
	nextPool, err := pool.Release(zone)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects := hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchydeletion.HierarchyDeletionBytesDigest(zoneValue),
		conditions: []etcdstore.Condition{
			{Key: poolKey, ModRevision: values.Values[0].ModRevision},
			{Key: addressesKey, ModRevision: etcdstore.RevisionOf(values.Values[1])},
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), evidence.ZoneID)},
			{Key: environmentchanges.ComponentTaskActiveEnvironmentKey(evidence.EnvironmentID)},
		},
		mutations: []etcdstore.Mutation{{Type: etcdstore.MutationDelete, Key: addressesKey}},
	}
	if len(nextPool.Reservations) == 0 {
		effects.mutations = append(effects.mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: poolKey})
		return effects, nil
	}
	poolValue, err := recordcodec.Encode("zone_pool_registry", nextPool)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.values = append(effects.values, poolValue)
	effects.mutations = append(effects.mutations, etcdstore.Mutation{Type: etcdstore.MutationPut, Key: poolKey, Value: poolValue})
	return effects, nil
}
