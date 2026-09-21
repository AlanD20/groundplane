package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"github.com/AlanD20/groundplane/pkg/errs"
	"reflect"
	"sort"
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
	if state == nil || len(state.Values) != len(keys) ||
		state.Values[2] == nil || state.Values[3] == nil ||
		(!blueprintRetry && state.Values[1] == nil) {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry state is incomplete")
	}
	if state.Values[0] != nil && ids.Validate(ids.KindTask, string(state.Values[0].Value)) != nil {
		return componentTaskChange{}, errs.New(errs.KindStateConflict, "Component retry ownership is corrupt")
	}
	headRevisionID, err := idempotencyrecord.DecodeTaskReference(state.Values[2].Value)
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
		active, decodeErr := componentrecord.DecodeRecord(value.Value)
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
			Key: componentrecord.RecordKey(active.Desired.ID), ModRevision: value.ModRevision,
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
		Keys: []string{hierarchyrecord.EnvironmentKey(intent.EnvironmentID)}, Revision: revision,
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
	environment, err := hierarchyrecord.DecodeEnvironment(environmentState.Values[0].Value)
	if err != nil || environment.ID != intent.EnvironmentID {
		clearComponentTaskChange(change)
		return componentTaskChange{}, recordcodec.CorruptRecord()
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
		etcdstore.Condition{Key: hierarchyrecord.EnvironmentKey(intent.EnvironmentID), ModRevision: environmentState.Values[0].ModRevision},
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
