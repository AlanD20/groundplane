package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const zoneServiceFinalizationBatchSize int64 = 24

func (repository *TaskRepository) finalizeZoneServiceMembershipBatch(
	ctx context.Context,
	task TaskRecord,
	updatedAt time.Time,
) (bool, error) {
	environmentID, err := zoneRemovalEnvironmentID(task)
	if err != nil {
		return false, err
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneKey(task.Target), deletionTombstoneKey(string(DeletionTargetZone), task.Target),
	}})
	if err != nil {
		return false, err
	}
	if state == nil || len(state.Values) != 2 || state.Values[0] == nil || state.Values[1] == nil {
		return false, errs.New(errs.KindStateConflict, "Zone deletion state is missing")
	}
	zone, err := decodeZoneRecord(state.Values[0].Value)
	if err != nil {
		return false, err
	}
	tombstone, err := decodeDeletionTombstone(state.Values[1].Value)
	if err != nil {
		return false, err
	}
	if zone.Desired.ID != task.Target || zone.EnvironmentID != environmentID ||
		tombstone.TargetKind != DeletionTargetZone || tombstone.TargetID != task.Target ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return false, errs.New(errs.KindStateConflict, "Zone deletion tombstone does not match its Task")
	}
	start := ""
	if tombstone.Checkpoint.ResourceKind != "" {
		if tombstone.Checkpoint.ResourceKind != "service" ||
			ids.Validate(ids.KindService, tombstone.Checkpoint.StableID) != nil {
			return false, errs.New(errs.KindInternal, "Zone deletion checkpoint is invalid")
		}
		start = serviceOwnerKey(environmentID, tombstone.Checkpoint.StableID)
	}
	page, err := repository.store.Range(ctx, RangeRequest{
		Prefix: serviceOwnerPrefix(environmentID), StartExclusive: start,
		Limit: zoneServiceFinalizationBatchSize, Revision: state.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision != state.ReadRevision ||
		len(page.Values) > int(zoneServiceFinalizationBatchSize) {
		return false, errs.New(errs.KindInternal, "Zone Service finalization page is invalid")
	}
	if len(page.Values) == 0 {
		return false, nil
	}
	serviceIDs := make([]string, len(page.Values))
	keys := make([]string, len(page.Values))
	for index, item := range page.Values {
		serviceID := string(item.Value)
		if ids.Validate(ids.KindService, serviceID) != nil || item.Key != serviceOwnerKey(environmentID, serviceID) {
			return false, errs.New(errs.KindInternal, "Zone Service membership index is corrupt")
		}
		serviceIDs[index] = serviceID
		keys[index] = serviceKey(serviceID)
	}
	records, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: state.ReadRevision})
	if err != nil {
		return false, err
	}
	if records == nil || len(records.Values) != len(keys) || records.ReadRevision != state.ReadRevision {
		return false, errs.New(errs.KindInternal, "Zone Service finalization records are incomplete")
	}
	conditions := []Condition{{
		Key:         deletionTombstoneKey(string(DeletionTargetZone), task.Target),
		ModRevision: state.Values[1].ModRevision,
	}}
	mutations := make([]Mutation, 0, len(records.Values)+1)
	for index, value := range records.Values {
		if value == nil {
			return false, errs.New(errs.KindInternal, "Zone Service membership index is orphaned")
		}
		record, err := decodeServiceRecord(value.Value)
		if err != nil {
			return false, err
		}
		if record.EnvironmentID != environmentID || record.Desired.ID != serviceIDs[index] {
			return false, errs.New(errs.KindInternal, "Zone Service membership record is corrupt")
		}
		zones := make([]string, 0, len(record.Desired.Zones))
		for _, name := range record.Desired.Zones {
			if name != zone.Desired.Name {
				zones = append(zones, name)
			}
		}
		if len(zones) == len(record.Desired.Zones) {
			continue
		}
		desired := record.Desired
		desired.Zones = zones
		replacement, err := ReplaceServiceDesired(record, desired)
		if err != nil {
			return false, err
		}
		encoded, err := encodeServiceRecord(replacement)
		if err != nil {
			return false, err
		}
		conditions = append(conditions, Condition{Key: serviceKey(record.Desired.ID), ModRevision: value.ModRevision})
		mutations = append(mutations, Mutation{Type: MutationPut, Key: serviceKey(record.Desired.ID), Value: encoded})
	}
	tombstone.Phase = DeletionPhaseFinalizing
	tombstone.Checkpoint = DeletionCheckpoint{ResourceKind: "service", StableID: serviceIDs[len(serviceIDs)-1]}
	tombstone.UpdatedAt = updatedAt
	tombstoneValue, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		clearMutationValues(mutations)
		return false, err
	}
	mutations = append(mutations, Mutation{
		Type: MutationPut, Key: deletionTombstoneKey(string(DeletionTargetZone), task.Target), Value: tombstoneValue,
	})
	defer clearMutationValues(mutations)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(errs.KindStateConflict, "Zone Service finalization state changed")
	}
	return true, nil
}

func (repository *TaskRepository) prepareZoneRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) ([]Condition, []Mutation, error) {
	environmentID, err := zoneRemovalEnvironmentID(task)
	if err != nil {
		return nil, nil, err
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneKey(task.Target),
		deletionTombstoneKey(string(DeletionTargetZone), task.Target),
		zonePoolRegistryKey(environmentID),
		componentAddressRegistryKey(task.Target),
	}, Revision: readRevision})
	if err != nil {
		return nil, nil, err
	}
	if state == nil || len(state.Values) != 4 || state.Values[0] == nil || state.Values[1] == nil ||
		state.Values[2] == nil {
		return nil, nil, errs.New(errs.KindInternal, "Zone deletion state is inconsistent")
	}
	zoneValue, tombstoneValue := state.Values[0], state.Values[1]
	zone, err := decodeZoneRecord(zoneValue.Value)
	if err != nil {
		return nil, nil, err
	}
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return nil, nil, err
	}
	if zone.Desired.ID != task.Target || zone.EnvironmentID != environmentID ||
		tombstone.TargetKind != DeletionTargetZone || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != zoneValue.ModRevision || tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone deletion tombstone does not match its Task")
	}
	pool, err := decodeEnvelope[zonePoolRegistry](state.Values[2].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(pool) != nil ||
		pool.Reservations[zone.Desired.ID] != zone.Desired.Subnet {
		return nil, nil, errs.New(errs.KindInternal, "Zone subnet reservation is inconsistent")
	}
	if state.Values[3] != nil {
		addresses, err := decodeEnvelope[componentAddressRegistry](
			state.Values[3].Value,
			"component_address_registry",
		)
		if err != nil || validateComponentAddressRegistry(zone, addresses) != nil || len(addresses.Reservations) != 0 {
			return nil, nil, errs.New(errs.KindResourceInUse, "Zone gained a Component address reservation")
		}
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneNameKey(environmentID, zone.Desired.Name), zoneOwnerKey(environmentID, zone.Desired.ID),
	}, Revision: readRevision})
	if err != nil {
		return nil, nil, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != zone.Desired.ID || string(indexes.Values[1].Value) != zone.Desired.ID {
		return nil, nil, errs.New(errs.KindInternal, "Zone deletion indexes are corrupt")
	}
	conditions := []Condition{
		{Key: zoneKey(zone.Desired.ID), ModRevision: zoneValue.ModRevision},
		{
			Key:         deletionTombstoneKey(string(DeletionTargetZone), zone.Desired.ID),
			ModRevision: tombstoneValue.ModRevision,
		},
	}
	mutations := []Mutation{{
		Type: MutationDelete, Key: deletionTombstoneKey(string(DeletionTargetZone), zone.Desired.ID),
	}}
	if terminalStatus != TaskStatusCompleted {
		return conditions, mutations, nil
	}
	start := ""
	if tombstone.Checkpoint.ResourceKind != "" {
		if tombstone.Checkpoint.ResourceKind != "service" ||
			ids.Validate(ids.KindService, tombstone.Checkpoint.StableID) != nil {
			return nil, nil, errs.New(errs.KindInternal, "Zone deletion checkpoint is invalid")
		}
		start = serviceOwnerKey(environmentID, tombstone.Checkpoint.StableID)
	}
	remaining, err := repository.store.Range(ctx, RangeRequest{
		Prefix: serviceOwnerPrefix(environmentID), StartExclusive: start, Limit: 1, Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if remaining == nil || remaining.ReadRevision != readRevision || len(remaining.Values) != 0 {
		return nil, nil, errs.New(errs.KindStateConflict, "Zone Service memberships remain during finalization")
	}
	nextPool, err := pool.release(zone)
	if err != nil {
		return nil, nil, err
	}
	conditions = append(conditions,
		Condition{Key: zoneNameKey(environmentID, zone.Desired.Name), ModRevision: indexes.Values[0].ModRevision},
		Condition{Key: zoneOwnerKey(environmentID, zone.Desired.ID), ModRevision: indexes.Values[1].ModRevision},
		Condition{Key: zonePoolRegistryKey(environmentID), ModRevision: state.Values[2].ModRevision},
		Condition{Key: componentAddressRegistryKey(zone.Desired.ID), ModRevision: keyValueRevision(state.Values[3])},
	)
	mutations = append(mutations,
		Mutation{Type: MutationDelete, Key: zoneNameKey(environmentID, zone.Desired.Name)},
		Mutation{Type: MutationDelete, Key: zoneOwnerKey(environmentID, zone.Desired.ID)},
		Mutation{Type: MutationDelete, Key: zoneKey(zone.Desired.ID)},
	)
	if state.Values[3] != nil {
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: componentAddressRegistryKey(zone.Desired.ID)})
	}
	if len(nextPool.Reservations) == 0 {
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: zonePoolRegistryKey(environmentID)})
	} else {
		encoded, err := encodeEnvelope("zone_pool_registry", nextPool)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: zonePoolRegistryKey(environmentID), Value: encoded,
		})
	}
	return conditions, mutations, nil
}

func (repository *TaskRepository) validateZoneRemovalReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) error {
	environmentID, err := zoneRemovalEnvironmentID(task)
	if err != nil {
		return err
	}
	state, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		zoneKey(task.Target), deletionTombstoneKey(string(DeletionTargetZone), task.Target),
		zonePoolRegistryKey(environmentID),
	}, Revision: readRevision})
	if err != nil {
		return err
	}
	if state == nil || len(state.Values) != 3 || state.Values[1] != nil {
		return errs.New(errs.KindStateConflict, "Zone deletion terminal state does not match its Task")
	}
	if terminalStatus == TaskStatusCompleted {
		if state.Values[0] != nil {
			return errs.New(errs.KindStateConflict, "completed Zone deletion retained its target")
		}
		if state.Values[2] != nil {
			pool, err := decodeEnvelope[zonePoolRegistry](state.Values[2].Value, "zone_pool_registry")
			if err != nil || validateZonePoolRegistry(pool) != nil {
				return errs.New(errs.KindInternal, "Zone pool registry is inconsistent")
			}
			if _, retained := pool.Reservations[task.Target]; retained {
				return errs.New(errs.KindStateConflict, "completed Zone deletion retained its subnet")
			}
		}
		return nil
	}
	if state.Values[0] == nil || state.Values[2] == nil {
		return errs.New(errs.KindStateConflict, "failed Zone deletion lost durable state")
	}
	zone, err := decodeZoneRecord(state.Values[0].Value)
	if err != nil {
		return err
	}
	pool, err := decodeEnvelope[zonePoolRegistry](state.Values[2].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(pool) != nil || pool.Reservations[task.Target] != zone.Desired.Subnet {
		return errs.New(errs.KindStateConflict, "failed Zone deletion changed its subnet reservation")
	}
	return nil
}

func zoneRemovalEnvironmentID(task TaskRecord) (string, error) {
	environmentID := task.Params[TaskZoneEnvironmentParam]
	if task.Executor != TaskExecutorAgent || task.Type != TaskRemove ||
		ids.Validate(ids.KindNetwork, task.Target) != nil || len(task.Params) != 1 ||
		ids.Validate(ids.KindEnvironment, environmentID) != nil {
		return "", errs.New(errs.KindInternal, "durable Zone removal Task shape is invalid")
	}
	return environmentID, nil
}

func serviceIDFromOwnerKey(environmentID string, key string) (string, error) {
	value := strings.TrimPrefix(key, serviceOwnerPrefix(environmentID))
	if value == key || ids.Validate(ids.KindService, value) != nil {
		return "", errs.New(errs.KindInternal, "Service owner index key is corrupt")
	}
	return value, nil
}
