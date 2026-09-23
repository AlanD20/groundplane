package etcd

import (
	"context"
	"sort"
	"time"

	blueprints "github.com/AlanD20/groundplane/internal/infra/etcd/blueprints"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	deletions "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	networkreservations "github.com/AlanD20/groundplane/internal/infra/etcd/networkreservations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) prepareComponentTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
	revision int64,
) (componentTaskChange, error) {
	intentResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentchanges.ComponentTaskIntentKey(task.ID)}, Revision: revision,
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
	intent, err := environmentchanges.DecodeComponentTaskIntent(intentValue.Value)
	if err != nil {
		return componentTaskChange{}, err
	}
	if err := componentplanning.ValidateComponentTaskOwner(componentplanning.TaskIdentity{ID: task.ID, Target: task.Target, Executor: task.Executor, Type: task.Type, CreatedAt: task.CreatedAt}, intent); err != nil {
		return componentTaskChange{}, err
	}
	if intent.Status != taskjournal.TaskStatusPending {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component candidate is not pending")
	}

	zoneSet := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		for _, record := range []componentrecord.Record{candidate.Current, candidate.Candidate} {
			binding, present, bindingErr := environmentchanges.ComponentTaskAddress(record)
			if bindingErr != nil {
				return componentTaskChange{}, bindingErr
			}
			if present {
				zoneSet[binding.ZoneID()] = struct{}{}
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
		environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
		blueprints.EnvironmentBlueprintRootKey(intent.EnvironmentID, desiredRevisionID),
	)
	for _, candidate := range intent.Candidates {
		keys = append(keys, componentrecord.RecordKey(candidate.Current.Desired.ID))
	}
	for _, zoneID := range zones {
		keys = append(keys, networkreservations.ComponentAddressRegistryKey(zoneID))
	}
	for _, zoneID := range zones {
		keys = append(keys, deletions.TombstoneKey("zone", zoneID))
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
			{Key: environmentchanges.ComponentTaskIntentKey(task.ID), ModRevision: intentValue.ModRevision},
			{
				Key:         environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
				ModRevision: state.Values[0].ModRevision,
			},
			{
				Key:         blueprints.EnvironmentBlueprintHeadKey(intent.EnvironmentID),
				ModRevision: state.Values[1].ModRevision,
			},
		},
	}
	if componentTaskAcknowledgementRequiresBlueprintRootCondition(task, terminalStatus, intent.EnvironmentID) {
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: blueprints.EnvironmentBlueprintRootKey(
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
		if !componentrecord.EqualRecord(active, candidate.Current) {
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
	registries := make(map[string]networkreservations.ComponentAddressRegistry, len(zones))
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
		registry := networkreservations.ComponentAddressRegistry{Reservations: map[string]string{}}
		if registryValue != nil {
			registry, decodeErr = recordcodec.Decode[networkreservations.ComponentAddressRegistry](
				registryValue.Value,
				"component_address_registry",
			)
			if decodeErr != nil || networkreservations.ValidateComponentAddressRegistry(zone, registry) != nil {
				return componentTaskChange{}, networkreservations.CorruptComponentAddressRegistry()
			}
		}
		zoneRecords[zoneID] = zone
		registries[zoneID] = registry
		registryValues[zoneID] = registryValue
		change.conditions = append(change.conditions, etcdstore.Condition{Key: deletions.TombstoneKey("zone", zoneID)})
		registryCondition := etcdstore.Condition{Key: networkreservations.ComponentAddressRegistryKey(zoneID)}
		if registryValue != nil {
			registryCondition.ModRevision = registryValue.ModRevision
		}
		change.conditions = append(change.conditions, registryCondition)
	}

	if err := componentplanning.ValidateComponentTaskReservations(intent, registries); err != nil {
		return componentTaskChange{}, err
	}
	changedRegistries := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		current, currentPresent, _ := environmentchanges.ComponentTaskAddress(candidate.Current)
		next, nextPresent, _ := environmentchanges.ComponentTaskAddress(candidate.Candidate)
		if environmentchanges.ComponentTaskBindingsEqual(current, currentPresent, next, nextPresent) {
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
		registry := registries[removed.ZoneID()]
		replacement, address, found, releaseErr := registry.Release(
			zoneRecords[removed.ZoneID()],
			candidate.Current.Desired.ID,
		)
		if releaseErr != nil || !found || address != removed.Address() {
			return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component address reservation changed")
		}
		registries[removed.ZoneID()] = replacement
		changedRegistries[removed.ZoneID()] = struct{}{}
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
	if terminalStatus == taskjournal.TaskStatusCompleted ||
		task.Params[releaserender.TaskReleasePublicationParam] == "" {
		routeObservationChange, routeErr := componentplanning.NewPlanner(repository.store).
			PrepareRouteObservationAcknowledgement(
				ctx, intent, terminalStatus, revision,
			)
		if routeErr != nil {
			clearComponentTaskChange(change)
			return componentTaskChange{}, routeErr
		}
		change.conditions = append(change.conditions, routeObservationChange.Conditions()...)
		change.mutations = append(change.mutations, routeObservationChange.Mutations()...)
		change.values = append(change.values, routeObservationChange.Values()...)
	}
	secretMutations, err := componentplanning.ComponentTaskTerminalSecretMutations(intent, task.ID, terminalStatus)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.mutations = append(change.mutations, secretMutations...)
	for _, zoneID := range zones {
		if _, changed := changedRegistries[zoneID]; !changed {
			continue
		}
		value, encodeErr := networkreservations.EncodeComponentAddressRegistry(zoneRecords[zoneID], registries[zoneID])
		if encodeErr != nil {
			clearComponentTaskChange(change)
			return componentTaskChange{}, encodeErr
		}
		change.values = append(change.values, value)
		change.mutations = append(change.mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: networkreservations.ComponentAddressRegistryKey(zoneID), Value: value,
		})
	}
	terminalIntent, err := environmentchanges.TerminalComponentTaskIntent(intent, terminalStatus, terminalAt)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	intentBytes, err := environmentchanges.EncodeComponentTaskIntent(terminalIntent)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.values = append(change.values, intentBytes)
	change.mutations = append(
		change.mutations,
		etcdstore.Mutation{
			Type:  etcdstore.MutationPut,
			Key:   environmentchanges.ComponentTaskIntentKey(task.ID),
			Value: intentBytes,
		},
		etcdstore.Mutation{
			Type: etcdstore.MutationDelete,
			Key:  environmentchanges.ComponentTaskActiveEnvironmentKey(intent.EnvironmentID),
		},
	)
	return change, nil
}

func componentTaskAcknowledgementRequiresBlueprintRootCondition(
	task TaskRecord,
	terminalStatus taskjournal.TaskStatus,
	environmentID string,
) bool {
	return terminalStatus != taskjournal.TaskStatusCompleted ||
		task.Params[taskjournal.TaskMaterializationEnvironmentParam] != environmentID
}
