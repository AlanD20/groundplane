package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"reflect"
	"sort"
	"time"
)

func (repository *TaskRepository) prepareComponentTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (componentTaskChange, error) {
	intentResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{componentTaskIntentKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return componentTaskChange{}, err
	}
	if intentResult == nil || len(intentResult.Values) != 1 {
		return componentTaskChange{}, errs.New(errs.KindInternal, "Component candidate read is incomplete")
	}
	intentValue := intentResult.Values[0]
	if intentValue == nil {
		return componentTaskChange{}, nil
	}
	intent, err := decodeComponentTaskIntent(intentValue.Value)
	if err != nil {
		return componentTaskChange{}, err
	}
	if err := validateComponentTaskOwner(task, intent); err != nil {
		return componentTaskChange{}, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component candidate is not pending")
	}

	zoneSet := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		for _, record := range []componentrecord.Record{candidate.Current, candidate.Candidate} {
			binding, present, bindingErr := componentTaskAddress(record)
			if bindingErr != nil {
				return componentTaskChange{}, bindingErr
			}
			if present {
				zoneSet[binding.zoneID] = struct{}{}
			}
		}
	}
	zones := make([]string, 0, len(zoneSet))
	for zoneID := range zoneSet {
		zones = append(zones, zoneID)
	}
	sort.Strings(zones)
	desiredRevisionID, desiredZones, err := componentTaskDesiredProjectionZones(
		ctx, repository.store, task, intent, zones,
	)
	if err != nil {
		return componentTaskChange{}, err
	}

	keys := make([]string, 0, 3+len(intent.Candidates)+(2*len(zones)))
	keys = append(
		keys,
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		environmentBlueprintHeadKey(intent.EnvironmentID),
		environmentBlueprintRootKey(intent.EnvironmentID, desiredRevisionID),
	)
	for _, candidate := range intent.Candidates {
		keys = append(keys, componentrecord.RecordKey(candidate.Current.Desired.ID))
	}
	for _, zoneID := range zones {
		keys = append(keys, componentAddressRegistryKey(zoneID))
	}
	for _, zoneID := range zones {
		keys = append(keys, deletionTombstoneKey("zone", zoneID))
	}
	state, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return componentTaskChange{}, err
	}
	if state == nil || len(state.Values) != len(keys) || state.Values[0] == nil ||
		state.Values[1] == nil || state.Values[2] == nil ||
		string(state.Values[0].Value) != task.ID {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component candidate ownership changed")
	}
	headRevisionID, err := idempotencyrecord.DecodeTaskReference(state.Values[1].Value)
	if err != nil || headRevisionID != desiredRevisionID {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component candidate Environment head changed")
	}

	change := componentTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: componentTaskIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), ModRevision: state.Values[0].ModRevision},
			{Key: environmentBlueprintHeadKey(intent.EnvironmentID), ModRevision: state.Values[1].ModRevision},
		},
	}
	if componentTaskAcknowledgementRequiresBlueprintRootCondition(task, terminalStatus, intent.EnvironmentID) {
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: environmentBlueprintRootKey(
				intent.EnvironmentID,
				desiredRevisionID,
			), ModRevision: state.Values[2].ModRevision,
		})
	}
	for index, candidate := range intent.Candidates {
		value := state.Values[index+3]
		if value == nil || value.ModRevision != candidate.CurrentRevision {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"active Component changed during reconciliation",
			)
		}
		active, decodeErr := componentrecord.DecodeRecord(value.Value)
		if decodeErr != nil {
			return componentTaskChange{}, decodeErr
		}
		if !reflect.DeepEqual(active, candidate.Current) {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"active Component no longer matches candidate base",
			)
		}
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: componentrecord.RecordKey(active.Desired.ID), ModRevision: value.ModRevision,
		})
	}

	zoneRecords := make(map[string]zonerecord.Record, len(zones))
	registries := make(map[string]componentAddressRegistry, len(zones))
	registryValues := make(map[string]*etcdstore.KeyValue, len(zones))
	registryOffset := 3 + len(intent.Candidates)
	tombstoneOffset := registryOffset + len(zones)
	for index, zoneID := range zones {
		if state.Values[tombstoneOffset+index] != nil {
			return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component candidate Zone was removed")
		}
		zone := desiredZones[zoneID]
		registryValue := state.Values[registryOffset+index]
		var decodeErr error
		registry := componentAddressRegistry{Reservations: map[string]string{}}
		if registryValue != nil {
			registry, decodeErr = recordcodec.Decode[componentAddressRegistry](
				registryValue.Value,
				"component_address_registry",
			)
			if decodeErr != nil || validateComponentAddressRegistry(zone, registry) != nil {
				return componentTaskChange{}, corruptComponentAddressRegistry()
			}
		}
		zoneRecords[zoneID] = zone
		registries[zoneID] = registry
		registryValues[zoneID] = registryValue
		change.conditions = append(change.conditions, etcdstore.Condition{Key: deletionTombstoneKey("zone", zoneID)})
		registryCondition := etcdstore.Condition{Key: componentAddressRegistryKey(zoneID)}
		if registryValue != nil {
			registryCondition.ModRevision = registryValue.ModRevision
		}
		change.conditions = append(change.conditions, registryCondition)
	}

	if err := validateComponentTaskReservations(intent, registries); err != nil {
		return componentTaskChange{}, err
	}
	changedRegistries := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		current, currentPresent, _ := componentTaskAddress(candidate.Current)
		next, nextPresent, _ := componentTaskAddress(candidate.Candidate)
		if componentTaskBindingsEqual(current, currentPresent, next, nextPresent) {
			continue
		}
		removed := next
		removedPresent := nextPresent
		if terminalStatus == taskjournal.TaskStatusCompleted {
			removed = current
			removedPresent = currentPresent
		}
		if !removedPresent {
			continue
		}
		registry := registries[removed.zoneID]
		replacement, address, found, releaseErr := registry.release(
			zoneRecords[removed.zoneID],
			candidate.Current.Desired.ID,
		)
		if releaseErr != nil || !found || address != removed.address {
			return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component address reservation changed")
		}
		registries[removed.zoneID] = replacement
		changedRegistries[removed.zoneID] = struct{}{}
	}

	if terminalStatus == taskjournal.TaskStatusCompleted {
		for _, candidate := range intent.Candidates {
			promoted, promoteErr := componentrecord.SetRuntime(
				candidate.Candidate,
				candidate.Candidate.Runtime.GeneratedServices,
				candidate.Candidate.Runtime.PinnedIPv4,
				candidate.Candidate.Desired.Enabled,
			)
			if promoteErr != nil {
				clearComponentTaskChange(change)
				return componentTaskChange{}, promoteErr
			}
			value, encodeErr := componentrecord.EncodeRecord(promoted)
			if encodeErr != nil {
				clearComponentTaskChange(change)
				return componentTaskChange{}, encodeErr
			}
			change.values = append(change.values, value)
			change.mutations = append(change.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(candidate.Candidate.Desired.ID), Value: value,
			})
		}
		change.mutations = append(change.mutations, componentrecord.WriteFenceMutation(task.ID))
	}
	if terminalStatus == taskjournal.TaskStatusCompleted || task.Params[TaskReleasePublicationParam] == "" {
		routeObservationChange, routeErr := repository.prepareComponentTaskRouteObservationAcknowledgement(
			ctx, intent, terminalStatus, revision,
		)
		if routeErr != nil {
			clearComponentTaskChange(change)
			return componentTaskChange{}, routeErr
		}
		change.conditions = append(change.conditions, routeObservationChange.conditions...)
		change.mutations = append(change.mutations, routeObservationChange.mutations...)
		change.values = append(change.values, routeObservationChange.values...)
	}
	secretMutations, err := componentTaskTerminalSecretMutations(intent, task.ID, terminalStatus)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.mutations = append(change.mutations, secretMutations...)
	for _, zoneID := range zones {
		if _, changed := changedRegistries[zoneID]; !changed {
			continue
		}
		value, encodeErr := encodeComponentAddressRegistry(zoneRecords[zoneID], registries[zoneID])
		if encodeErr != nil {
			clearComponentTaskChange(change)
			return componentTaskChange{}, encodeErr
		}
		change.values = append(change.values, value)
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: componentAddressRegistryKey(zoneID), Value: value,
		})
	}
	terminalIntent, err := terminalComponentTaskIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	intentBytes, err := encodeComponentTaskIntent(terminalIntent)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.values = append(change.values, intentBytes)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: componentTaskIntentKey(task.ID), Value: intentBytes},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
	)
	return change, nil
}

func componentTaskAcknowledgementRequiresBlueprintRootCondition(
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	environmentID string,
) bool {
	return terminalStatus != taskjournal.TaskStatusCompleted ||
		task.Params[TaskMaterializationEnvironmentParam] != environmentID
}
