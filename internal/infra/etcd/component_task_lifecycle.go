package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"reflect"
	"sort"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type componentTaskChange struct {
	applies    bool
	conditions []etcdstore.Condition
	mutations  []etcdstore.Mutation
	values     [][]byte
}

const (
	componentTaskBlueprintProcedureParam             = "blueprint_compose_procedure"
	componentTaskBlueprintProcedureNone              = "none"
	componentTaskBlueprintProcedureFullReconcile     = "full-reconcile"
	componentTaskBlueprintProcedureCandidateReleases = "candidate-releases"
)

func (repository *TaskRepository) prepareComponentTaskRetry(
	ctx context.Context,
	source TaskRecord,
	retry TaskRecord,
	revision int64,
) (componentTaskChange, error) {
	intentResult, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{componentTaskIntentKey(source.ID)}, Revision: revision,
	})
	if err != nil {
		return componentTaskChange{}, err
	}
	if intentResult == nil || len(intentResult.Values) != 1 {
		return componentTaskChange{}, errs.New(errs.KindInternal, "Component retry read is incomplete")
	}
	intentValue := intentResult.Values[0]
	if intentValue == nil {
		return componentTaskChange{}, nil
	}
	intent, err := decodeComponentTaskIntent(intentValue.Value)
	if err != nil {
		return componentTaskChange{}, err
	}
	if err := validateComponentTaskOwner(source, intent); err != nil {
		return componentTaskChange{}, err
	}
	if source.FinishedAt == nil || intent.TerminalAt == nil || intent.Status != source.Status ||
		!intent.TerminalAt.Equal(*source.FinishedAt) || retry.Executor != source.Executor ||
		retry.Type != source.Type || retry.Target != source.Target || retry.RetryOf != source.ID ||
		retry.RenderGeneration != source.RenderGeneration {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry changed its pinned Task")
	}
	retryIntent, err := NewComponentTaskIntent(
		retry.ID,
		intent.EnvironmentID,
		intent.Candidates,
		retry.CreatedAt,
	)
	if err != nil {
		return componentTaskChange{}, err
	}
	retryIntent.RouteProjection = cloneComponentTaskRouteProjection(intent.RouteProjection)

	zoneSet := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		for _, record := range []ComponentRecord{candidate.Current, candidate.Candidate} {
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
		ctx, repository.store, source, intent, zones,
	)
	if err != nil {
		return componentTaskChange{}, err
	}
	blueprintRetry, err := componentTaskRetryIsBlueprint(source)
	if err != nil {
		return componentTaskChange{}, err
	}
	var blueprintProjection EnvironmentComposeProjection
	if blueprintRetry {
		hierarchy := &HierarchyRepository{store: repository.store}
		desiredProjection, found, projectionErr := hierarchy.GetEnvironmentComposeProjectionRevision(
			ctx, intent.EnvironmentID, desiredRevisionID,
		)
		if projectionErr != nil {
			return componentTaskChange{}, projectionErr
		}
		if !found {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Blueprint retry desired projection is unavailable",
			)
		}
		blueprintProjection = desiredProjection.Record
	}

	keys := make([]string, 0, 4+len(intent.Candidates)+(2*len(zones)))
	keys = append(
		keys,
		componentTaskActiveEnvironmentKey(intent.EnvironmentID),
		environmentComposeProjectionKey(intent.EnvironmentID),
		environmentBlueprintHeadKey(intent.EnvironmentID),
		environmentBlueprintRootKey(intent.EnvironmentID, desiredRevisionID),
	)
	for _, candidate := range intent.Candidates {
		keys = append(keys, componentKey(candidate.Current.Desired.ID))
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
	if state == nil || len(state.Values) != len(keys) ||
		state.Values[2] == nil || state.Values[3] == nil ||
		(!blueprintRetry && state.Values[1] == nil) {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry state is incomplete")
	}
	if state.Values[0] != nil && ids.Validate(ids.KindTask, string(state.Values[0].Value)) != nil {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry ownership is corrupt")
	}
	headRevisionID, err := decodeTaskReference(state.Values[2].Value)
	if err != nil || headRevisionID != desiredRevisionID {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry Environment head changed")
	}
	if blueprintRetry {
		if blueprintProjection.EnvironmentID != intent.EnvironmentID ||
			blueprintProjection.RevisionID != desiredRevisionID ||
			blueprintProjection.RenderGeneration != uint64(source.RenderGeneration) ||
			!componentRetryProjectionMatches(intent, blueprintProjection) {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Component retry no longer matches the immutable Blueprint projection",
			)
		}
		if state.Values[1] != nil {
			applied, decodeErr := decodeEnvironmentComposeProjection(state.Values[1].Value)
			if decodeErr != nil || applied.EnvironmentID != intent.EnvironmentID ||
				applied.RevisionID == desiredRevisionID ||
				applied.RenderGeneration >= uint64(source.RenderGeneration) {
				return componentTaskChange{}, errs.New(
					errs.KindStateConflict,
					"Blueprint retry predecessor projection changed",
				)
			}
		}
	} else {
		projection, decodeErr := decodeEnvironmentComposeProjection(state.Values[1].Value)
		if decodeErr != nil {
			return componentTaskChange{}, decodeErr
		}
		if projection.EnvironmentID != intent.EnvironmentID ||
			projection.RenderGeneration != uint64(source.RenderGeneration) ||
			!componentRetryProjectionMatches(intent, projection) {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"Component retry no longer matches the pinned Environment projection",
			)
		}
	}

	change := componentTaskChange{
		applies: true,
		conditions: []etcdstore.Condition{
			{Key: componentTaskIntentKey(source.ID), ModRevision: intentValue.ModRevision},
			{Key: componentTaskIntentKey(retry.ID)},
			{Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID)},
			{
				Key:         environmentComposeProjectionKey(intent.EnvironmentID),
				ModRevision: keyValueRevision(state.Values[1]),
			},
			{Key: environmentBlueprintHeadKey(intent.EnvironmentID), ModRevision: state.Values[2].ModRevision},
			{
				Key:         environmentBlueprintRootKey(intent.EnvironmentID, desiredRevisionID),
				ModRevision: state.Values[3].ModRevision,
			},
		},
	}
	for index, candidate := range intent.Candidates {
		value := state.Values[index+4]
		if value == nil || value.ModRevision != candidate.CurrentRevision {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"active Component changed before retry",
			)
		}
		active, decodeErr := decodeComponentRecord(value.Value)
		if decodeErr != nil {
			return componentTaskChange{}, decodeErr
		}
		if !reflect.DeepEqual(active, candidate.Current) {
			return componentTaskChange{}, errs.New(
				errs.KindStateConflict,
				"active Component no longer matches retry base",
			)
		}
		change.conditions = append(change.conditions, etcdstore.Condition{
			Key: componentKey(active.Desired.ID), ModRevision: value.ModRevision,
		})
	}

	zoneRecords := make(map[string]zonerecord.Record, len(zones))
	registries := make(map[string]componentAddressRegistry, len(zones))
	registryOffset := 4 + len(intent.Candidates)
	tombstoneOffset := registryOffset + len(zones)
	for index, zoneID := range zones {
		if state.Values[tombstoneOffset+index] != nil {
			return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry Zone was removed")
		}
		zone := desiredZones[zoneID]
		registryValue := state.Values[registryOffset+index]
		registry := componentAddressRegistry{Reservations: map[string]string{}}
		var decodeErr error
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
		change.conditions = append(change.conditions, etcdstore.Condition{Key: deletionTombstoneKey("zone", zoneID)})
		registryCondition := etcdstore.Condition{Key: componentAddressRegistryKey(zoneID)}
		if registryValue != nil {
			registryCondition.ModRevision = registryValue.ModRevision
		}
		change.conditions = append(change.conditions, registryCondition)
	}

	changedRegistries := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		current, currentPresent, _ := componentTaskAddress(candidate.Current)
		if currentPresent && registries[current.zoneID].Reservations[candidate.Current.Desired.ID] != current.address {
			return componentTaskChange{}, errs.New(errs.KindStateConflict, "active Component address changed")
		}
		next, nextPresent, _ := componentTaskAddress(candidate.Candidate)
		if !nextPresent || componentTaskBindingsEqual(current, currentPresent, next, nextPresent) {
			continue
		}
		registry := registries[next.zoneID]
		replacement, reserveErr := registry.reserveExact(
			zoneRecords[next.zoneID],
			candidate.Candidate.Desired.ID,
			next.address,
		)
		if reserveErr != nil {
			return componentTaskChange{}, reserveErr
		}
		if !reflect.DeepEqual(registry, replacement) {
			registries[next.zoneID] = replacement
			changedRegistries[next.zoneID] = struct{}{}
		}
	}
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
	intentBytes, err := encodeComponentTaskIntent(retryIntent)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.values = append(change.values, intentBytes)
	change.mutations = append(change.mutations,
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: componentTaskIntentKey(retry.ID), Value: intentBytes},
		etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: componentTaskActiveEnvironmentKey(intent.EnvironmentID), Value: []byte(retry.ID),
		},
	)
	environmentState, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{environmentKey(intent.EnvironmentID)}, Revision: revision,
	})
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	if environmentState == nil || len(environmentState.Values) != 1 || environmentState.Values[0] == nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, errs.New(
			errs.KindEnvironmentNotFound,
			"Component retry Environment was not found",
		)
	}
	environment, err := decodeEnvironment(environmentState.Values[0].Value)
	if err != nil || environment.ID != intent.EnvironmentID {
		clearComponentTaskChange(change)
		return componentTaskChange{}, corruptRecord()
	}
	_, secretConditions, secretMutations, err := prepareComponentTaskSecretReferences(
		ctx,
		repository.store,
		environment.ProjectID,
		retry.ID,
		intent.Candidates,
		revision,
	)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.conditions = append(
		change.conditions,
		etcdstore.Condition{Key: environmentKey(intent.EnvironmentID), ModRevision: environmentState.Values[0].ModRevision},
	)
	change.conditions = append(change.conditions, secretConditions...)
	change.mutations = append(change.mutations, secretMutations...)
	routeRetryChange, err := repository.prepareComponentTaskRouteObservationRetry(
		ctx, retryIntent, revision,
	)
	if err != nil {
		clearComponentTaskChange(change)
		return componentTaskChange{}, err
	}
	change.conditions = append(change.conditions, routeRetryChange.conditions...)
	change.mutations = append(change.mutations, routeRetryChange.mutations...)
	change.values = append(change.values, routeRetryChange.values...)
	return change, nil
}

func componentTaskRetryIsBlueprint(source TaskRecord) (bool, error) {
	procedure, present := source.Params[componentTaskBlueprintProcedureParam]
	publicationPresent := source.Params[TaskReleasePublicationParam] != ""
	if !present {
		if publicationPresent {
			return false, errs.New(errs.KindStateConflict, "Blueprint Component retry procedure is unavailable")
		}
		return false, nil
	}
	switch procedure {
	case componentTaskBlueprintProcedureNone:
		if publicationPresent {
			return false, errs.New(errs.KindStateConflict, "Blueprint Component retry procedure is inconsistent")
		}
		return true, nil
	case componentTaskBlueprintProcedureCandidateReleases:
		if !publicationPresent {
			return false, errs.New(errs.KindStateConflict, "Blueprint Component retry publication is unavailable")
		}
		return true, nil
	case componentTaskBlueprintProcedureFullReconcile:
		if publicationPresent {
			return false, errs.New(errs.KindStateConflict, "Component retry procedure is inconsistent")
		}
		return false, nil
	default:
		return false, errs.New(errs.KindStateConflict, "Component retry procedure is invalid")
	}
}

func componentRetryProjectionMatches(
	intent ComponentTaskIntent,
	projection EnvironmentComposeProjection,
) bool {
	for _, candidate := range intent.Candidates {
		matched := false
		for _, component := range projection.Components {
			if component.Desired.ID != candidate.Candidate.Desired.ID {
				continue
			}
			if !reflect.DeepEqual(component, candidate.Candidate) {
				return false
			}
			matched = true
			break
		}
		if !matched {
			return false
		}
	}
	return true
}

func (repository *TaskRepository) prepareComponentTaskAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
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
	if intent.Status != TaskStatusPending {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component candidate is not pending")
	}

	zoneSet := make(map[string]struct{})
	for _, candidate := range intent.Candidates {
		for _, record := range []ComponentRecord{candidate.Current, candidate.Candidate} {
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
		keys = append(keys, componentKey(candidate.Current.Desired.ID))
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
	headRevisionID, err := decodeTaskReference(state.Values[1].Value)
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
		active, decodeErr := decodeComponentRecord(value.Value)
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
			Key: componentKey(active.Desired.ID), ModRevision: value.ModRevision,
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
		if terminalStatus == TaskStatusCompleted {
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

	if terminalStatus == TaskStatusCompleted {
		for _, candidate := range intent.Candidates {
			promoted, promoteErr := SetComponentRuntime(
				candidate.Candidate,
				candidate.Candidate.Runtime.GeneratedServices,
				candidate.Candidate.Runtime.PinnedIPv4,
				candidate.Candidate.Desired.Enabled,
			)
			if promoteErr != nil {
				clearComponentTaskChange(change)
				return componentTaskChange{}, promoteErr
			}
			value, encodeErr := encodeComponentRecord(promoted)
			if encodeErr != nil {
				clearComponentTaskChange(change)
				return componentTaskChange{}, encodeErr
			}
			change.values = append(change.values, value)
			change.mutations = append(change.mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: componentKey(candidate.Candidate.Desired.ID), Value: value,
			})
		}
		change.mutations = append(change.mutations, componentWriteFenceMutation(task.ID))
	}
	if terminalStatus == TaskStatusCompleted || task.Params[TaskReleasePublicationParam] == "" {
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
	terminalStatus TaskStatus,
	environmentID string,
) bool {
	return terminalStatus != TaskStatusCompleted ||
		task.Params[TaskMaterializationEnvironmentParam] != environmentID
}

func (repository *TaskRepository) validateComponentTaskAcknowledgementReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	revision int64,
) error {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			componentTaskIntentKey(task.ID),
			componentTaskActiveEnvironmentKey(task.Target),
		},
		Revision: revision,
	})
	if err != nil {
		return err
	}
	if result == nil || len(result.Values) != 2 {
		return errs.New(errs.KindInternal, "Component candidate replay read is incomplete")
	}
	if result.Values[0] == nil {
		return nil
	}
	intent, err := decodeComponentTaskIntent(result.Values[0].Value)
	if err != nil {
		return err
	}
	if err := validateComponentTaskOwner(task, intent); err != nil {
		return err
	}
	if intent.Status != terminalStatus || intent.TerminalAt == nil || task.FinishedAt == nil ||
		!intent.TerminalAt.Equal(*task.FinishedAt) {
		return errs.New(errs.KindStateConflict, "Component candidate does not match terminal Task")
	}
	if result.Values[1] != nil {
		activeTaskID := string(result.Values[1].Value)
		if activeTaskID == task.ID || ids.Validate(ids.KindTask, activeTaskID) != nil {
			return errs.New(errs.KindStateConflict, "terminal Component candidate remains active")
		}
	}
	return nil
}

func validateComponentTaskOwner(task TaskRecord, intent ComponentTaskIntent) error {
	if task.Executor != TaskExecutorAgent || task.Type != TaskUpdate || task.ID != intent.TaskID ||
		task.Target != intent.EnvironmentID || !task.CreatedAt.Equal(intent.CreatedAt) {
		return errs.New(errs.KindStateConflict, "Component candidate does not belong to its Task")
	}
	return nil
}

func componentTaskDesiredProjectionZones(
	ctx context.Context,
	store taskRepositoryStore,
	task TaskRecord,
	intent ComponentTaskIntent,
	zones []string,
) (string, map[string]zonerecord.Record, error) {
	desiredRevisionID := task.Params[EnvironmentDesiredRevisionParam]
	if ids.Validate(ids.KindTask, desiredRevisionID) != nil {
		return "", nil, errs.New(errs.KindStateConflict, "Component Task desired revision is invalid")
	}
	hierarchy := &HierarchyRepository{store: store}
	projection, found, err := hierarchy.GetEnvironmentComposeProjectionRevision(
		ctx, intent.EnvironmentID, desiredRevisionID,
	)
	if err != nil {
		return "", nil, err
	}
	if !found || projection.Record.EnvironmentID != intent.EnvironmentID ||
		projection.Record.RevisionID != desiredRevisionID ||
		projection.Record.RenderGeneration != uint64(task.RenderGeneration) {
		return "", nil, errs.New(
			errs.KindStateConflict,
			"Component Task desired projection changed",
		)
	}
	projected := make(map[string]zonerecord.Record, len(projection.Record.DesiredZones))
	for _, desired := range projection.Record.DesiredZones {
		zone, joinErr := joinEnvironmentZone(projection, desired)
		if joinErr != nil {
			return "", nil, joinErr
		}
		projected[zone.Record.Desired.ID] = zone.Record
	}
	result := make(map[string]zonerecord.Record, len(zones))
	for _, zoneID := range zones {
		zone, ok := projected[zoneID]
		if !ok {
			return "", nil, errs.New(errs.KindStateConflict, "Component Task Zone is not in its desired projection")
		}
		result[zoneID] = zone
	}
	return desiredRevisionID, result, nil
}

func validateComponentTaskReservations(
	intent ComponentTaskIntent,
	registries map[string]componentAddressRegistry,
) error {
	for _, candidate := range intent.Candidates {
		for _, record := range []ComponentRecord{candidate.Current, candidate.Candidate} {
			binding, present, err := componentTaskAddress(record)
			if err != nil {
				return err
			}
			if !present {
				continue
			}
			registry, exists := registries[binding.zoneID]
			if !exists || registry.Reservations[record.Desired.ID] != binding.address {
				return errs.New(errs.KindStateConflict, "Component address reservation changed")
			}
		}
	}
	return nil
}

func clearComponentTaskChange(change componentTaskChange) {
	for _, value := range change.values {
		clear(value)
	}
}

func componentTaskActiveValueMatches(value *etcdstore.KeyValue, taskID string) bool {
	return value != nil && bytes.Equal(value.Value, []byte(taskID))
}
