package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyDeletionControllerEffects struct {
	fixedInputDigest string
	conditions       []Condition
	mutations        []Mutation
	values           [][]byte
}

func (repository *HierarchyDeletionRepository) CompleteControllerAction(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	if err := validateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if validateTimestamp("hierarchy deletion Controller completion", completedAt) != nil ||
		action.ProcedureKind != HierarchyDeletionProcedureController || action.ControllerProcedure == nil {
		return HierarchyDeletionOperation{}, errs.New(errs.KindValidationFailed, "hierarchy deletion Controller action is invalid")
	}
	current, err := repository.OperationByTask(ctx, operation.Tombstone.CurrentTaskID)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if err := validateHierarchyDeletionActiveAction(current, action); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if action.TargetID == current.Tombstone.TargetID &&
		hierarchyDeletionRootFinalizerMatches(current.Tombstone.TargetKind, action.ActionKind) {
		return HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "root hierarchy deletion finalizer requires Task acknowledgement")
	}
	effects, err := repository.prepareHierarchyDeletionControllerEffects(ctx, current, action)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clearByteSlices(effects.values)
	expected, err := bindHierarchyDeletionControllerProcedure(
		HierarchyDeletionPlannedAction{
			NodeID: action.NodeID, Ordinal: action.Ordinal, ParentOperationID: action.ParentOperationID,
			ActionKind: action.ActionKind, TargetKind: action.TargetKind, TargetID: action.TargetID,
			TargetRevision: action.TargetRevision,
		},
		HierarchyDeletionControllerFinalizerInput{
			Finalizer: action.ControllerProcedure.Finalizer, TargetKind: action.TargetKind,
			TargetID: action.TargetID, FixedInputRevision: action.TargetRevision,
			FixedInputDigest: effects.fixedInputDigest, BatchOrdinal: 0, BatchCount: 1,
		},
	)
	if err != nil || expected != *action.ControllerProcedure {
		return HierarchyDeletionOperation{}, errs.New(errs.KindStateConflict, "hierarchy deletion finalizer template changed")
	}
	actionValue, err := encodeHierarchyDeletionAction(action)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(actionValue)
	completion := HierarchyDeletionActionCompletion{
		Schema: 1, ParentOperationID: current.Tombstone.OperationID,
		DeletionEpoch: current.Tombstone.DeletionEpoch, Ordinal: action.Ordinal,
		ActionDigest: hierarchyDeletionBytesDigest(actionValue), TargetKind: action.TargetKind,
		TargetID: action.TargetID, TargetRevision: action.TargetRevision,
		Executor: HierarchyDeletionProcedureController,
		ControllerProof: &HierarchyDeletionControllerCompletionProof{
			Finalizer:                   action.ControllerProcedure.Finalizer,
			FixedInputRevision:          action.ControllerProcedure.FixedInputRevision,
			CompareTemplateDigest:       action.ControllerProcedure.CompareTemplateDigest,
			MutationTemplateDigest:      action.ControllerProcedure.MutationTemplateDigest,
			PostconditionTemplateDigest: action.ControllerProcedure.PostconditionTemplateDigest,
		},
		CompletedAt: completedAt,
	}
	completionValue, err := encodeHierarchyDeletionRecord(completion, hierarchyDeletionCompletionRecordBytes)
	if err != nil {
		return HierarchyDeletionOperation{}, err
	}
	defer clear(completionValue)
	nextTombstone, nextFence := advanceHierarchyDeletionCheckpoint(
		current, action, completion.ActionDigest, action.ControllerProcedure.MutationTemplateDigest, completedAt,
	)
	return repository.commitHierarchyDeletionCompletion(
		ctx, current, nextTombstone, nextFence, action.Ordinal, completionValue,
		effects.conditions, effects.mutations,
	)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionControllerEffects(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	switch action.ActionKind {
	case HierarchyDeletionReleaseGroupRemove:
		return repository.prepareHierarchyDeletionReleaseGroupFinalizer(ctx, action)
	case HierarchyDeletionServiceRemove:
		return repository.prepareHierarchyDeletionServiceFinalizer(ctx, action)
	case HierarchyDeletionEntryRemove:
		return repository.prepareHierarchyDeletionEntryFinalizer(ctx, action)
	case HierarchyDeletionRouteRemove:
		return repository.prepareHierarchyDeletionRouteFinalizer(ctx, action)
	case HierarchyDeletionComponentRemove:
		return repository.prepareHierarchyDeletionComponentFinalizer(ctx, action)
	case HierarchyDeletionScriptRemove:
		return repository.prepareHierarchyDeletionScriptFinalizer(ctx, action)
	case HierarchyDeletionZoneRemove:
		return repository.prepareHierarchyDeletionZoneFinalizer(ctx, action)
	case HierarchyDeletionConnectorFinalize:
		return repository.prepareHierarchyDeletionConnectorFinalizer(ctx, action)
	case HierarchyDeletionProjectSecretRemove:
		return repository.prepareHierarchyDeletionSecretFinalizer(ctx, action)
	case HierarchyDeletionReservationRelease:
		return repository.prepareHierarchyDeletionReservationFinalizer(ctx, action)
	case HierarchyDeletionRunnerLocalRemove:
		return repository.prepareHierarchyDeletionRunnerFinalizer(ctx, action)
	case HierarchyDeletionEnvironmentFinalize:
		return repository.prepareHierarchyDeletionEnvironmentFinalizer(ctx, operation, action)
	case HierarchyDeletionProjectFinalize:
		return repository.prepareHierarchyDeletionProjectFinalizer(ctx, action)
	case HierarchyDeletionTenantFinalize:
		return repository.prepareHierarchyDeletionTenantFinalizer(ctx, action)
	case HierarchyDeletionBackingServiceFinalize:
		return repository.prepareHierarchyDeletionProjectFinalizer(ctx, action)
	default:
		return hierarchyDeletionControllerEffects{}, errs.Newf(
			errs.KindValidationFailed, "hierarchy deletion Controller finalizer %q is unsupported before plan execution",
			action.ActionKind,
		)
	}
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionServiceFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, serviceKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeServiceRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	activeKey := serviceLifecycleActiveKey(record.Desired.ID)
	active, err := repository.store.Get(ctx, activeKey)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if active == nil {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	if active.Entry != nil {
		clear(active.Entry.Value)
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"Service lifecycle is still active during hierarchy metadata finalization",
		)
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		serviceNameKey(record.EnvironmentID, record.Desired.Name),
		serviceOwnerKey(record.EnvironmentID, record.Desired.ID),
	})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.conditions = append(effects.conditions, Condition{Key: activeKey})
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionEntryFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, entryRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeEntryRecord(primary.Value)
	if err != nil || record.Entry.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		entryOwnerKey(record.EnvironmentID, record.Entry.ID),
	})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.mutations = append(effects.mutations,
		Mutation{Type: MutationDelete, Key: entryPlainValueGenerationPrefix + record.Entry.ID + "/", Prefix: true},
		Mutation{Type: MutationDelete, Key: entrySecretValueGenerationPrefix + record.Entry.ID + "/", Prefix: true},
	)
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRouteFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, routeKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeRouteRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	return repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, []string{
		routeOwnerKey(record.EnvironmentID, record.Desired.ID),
		routeMatchKey(record.EnvironmentID, record.Desired.Host, record.Desired.Path),
	})
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionComponentFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, componentKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeComponentRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	keys := []string{
		componentEnvironmentOwnerKey(record.Desired.OwnerID, record.Desired.ID),
		componentEnvironmentKindKey(record.Desired.OwnerID, record.Desired.Kind),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.mutations = append(effects.mutations, componentWriteFenceMutation(action.TargetID))
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionZoneFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, zoneKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeZoneRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, []string{
		serviceOwnerPrefix(record.EnvironmentID),
	}); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	nameKey := zoneNameKey(record.EnvironmentID, record.Desired.Name)
	ownerKey := zoneOwnerKey(record.EnvironmentID, record.Desired.ID)
	poolKey := zonePoolRegistryKey(record.EnvironmentID)
	addressesKey := componentAddressRegistryKey(record.Desired.ID)
	values, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		nameKey, ownerKey, poolKey, addressesKey,
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if values == nil || len(values.Values) != 4 || values.Values[0] == nil || values.Values[1] == nil ||
		values.Values[2] == nil || string(values.Values[0].Value) != record.Desired.ID ||
		string(values.Values[1].Value) != record.Desired.ID {
		if values != nil {
			clearKeyValues(values.Values)
		}
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	defer clearKeyValues(values.Values)
	pool, err := decodeEnvelope[zonePoolRegistry](values.Values[2].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(pool) != nil {
		return hierarchyDeletionControllerEffects{}, corruptZonePoolRegistry()
	}
	nextPool, err := pool.release(record)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects := hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value),
		conditions: []Condition{
			{Key: primary.Key, ModRevision: primary.ModRevision},
			{Key: nameKey, ModRevision: values.Values[0].ModRevision},
			{Key: ownerKey, ModRevision: values.Values[1].ModRevision},
			{Key: poolKey, ModRevision: values.Values[2].ModRevision},
			{Key: addressesKey, ModRevision: keyValueRevision(values.Values[3])},
			{Key: serviceOwnerPrefix(record.EnvironmentID), Prefix: true},
		},
		mutations: []Mutation{
			{Type: MutationDelete, Key: nameKey},
			{Type: MutationDelete, Key: ownerKey},
			{Type: MutationDelete, Key: primary.Key},
			{Type: MutationDelete, Key: addressesKey},
		},
	}
	if len(nextPool.Reservations) == 0 {
		effects.mutations = append(effects.mutations, Mutation{Type: MutationDelete, Key: poolKey})
		return effects, nil
	}
	poolValue, err := encodeEnvelope("zone_pool_registry", nextPool)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.values = append(effects.values, poolValue)
	effects.mutations = append(effects.mutations, Mutation{Type: MutationPut, Key: poolKey, Value: poolValue})
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionTenantFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, tenantKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeTenant(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	prefixes := []string{
		projectTenantOwnerPrefix(record.ID), runnerOwnerPrefix(RunnerOwnerTenant, record.ID),
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	slugKey := tenantSlugKey(record.Slug)
	slug, err := repository.store.Get(ctx, slugKey)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if slug.Entry == nil || string(slug.Entry.Value) != record.ID {
		if slug.Entry != nil {
			clear(slug.Entry.Value)
		}
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	defer clear(slug.Entry.Value)
	conditions := []Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: slugKey, ModRevision: slug.Entry.ModRevision},
	}
	for _, prefix := range prefixes {
		conditions = append(conditions, Condition{Key: prefix, Prefix: true})
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions,
		mutations: []Mutation{
			{Type: MutationDelete, Key: slugKey},
			{Type: MutationDelete, Key: tenantKey(record.ID)},
		},
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionScriptFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	if action.ControllerProcedure == nil {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	storage, err := readActiveScriptStorage(ctx, repository.store, action.TargetID, action.ControllerProcedure.FixedInputRevision)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	record := storage.Script.Record
	if storage.Script.Revision != action.TargetRevision || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	if record.ActiveReferences != 0 {
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindResourceInUse, "active Script executions fence hierarchy deletion",
		)
	}
	value, err := encodeScriptRecord(record)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(value)
	primary := KeyValue{
		Key:   scriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
		Value: value, ModRevision: storage.Script.Revision,
	}
	keys := []string{
		scriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
		scriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, &primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	activeValue, err := encodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.values = append(effects.values, activeValue)
	effects.conditions = append(effects.conditions,
		Condition{Key: scriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
		Condition{Key: scriptLocatorKey(action.TargetID), ModRevision: storage.Locator.Revision},
		Condition{Key: scriptEnvironmentLocatorKey(record.EnvironmentID, action.TargetID), ModRevision: storage.EnvironmentLocator.Revision},
	)
	effects.mutations = append(effects.mutations,
		Mutation{Type: MutationDelete, Key: scriptSetBodyGenerationPrefix(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID), Prefix: true},
		Mutation{Type: MutationDelete, Key: scriptLocatorKey(action.TargetID)},
		Mutation{Type: MutationDelete, Key: scriptEnvironmentLocatorKey(record.EnvironmentID, action.TargetID)},
		Mutation{Type: MutationPut, Key: scriptSetActiveKey(record.EnvironmentID), Value: activeValue},
	)
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionConnectorFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, connectorRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeConnectorRecord(primary.Value)
	if err != nil || record.Connector.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	keys := []string{
		connectorEnvironmentKey(record.Connector.EnvironmentID, action.TargetID),
		connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
		connectorCredentialValueKey(action.TargetID),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	prefixes := hierarchyDeletionConnectorReferencePrefixes(action.TargetID)
	_, err = repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes)
	if err != nil {
		clearByteSlices(effects.values)
		return hierarchyDeletionControllerEffects{}, err
	}
	for _, prefix := range prefixes {
		effects.conditions = append(effects.conditions, Condition{Key: prefix, Prefix: true})
	}
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionSecretFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, secretRecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeSecretRecord(primary.Value)
	if err != nil || record.Secret.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	keys := []string{secretOwnerKey(record.Secret), secretScopedKey(record.Secret), secretValueKey(action.TargetID)}
	return repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionReservationFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	result, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		environmentKey(action.TargetID), environmentPoolRegistryKey,
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		if result != nil {
			clearKeyValues(result.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(errs.KindStateConflict, "hierarchy deletion reservation authority changed")
	}
	defer clearKeyValues(result.Values)
	environment, err := decodeEnvironment(result.Values[0].Value)
	if err != nil || environment.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	fixedInputDigest := hierarchyDeletionBytesDigest(result.Values[0].Value)
	if result.Values[0].ModRevision != action.TargetRevision {
		if environment.DeletionTaskID == "" {
			return hierarchyDeletionControllerEffects{}, errs.New(
				errs.KindStateConflict,
				"hierarchy deletion reservation authority changed",
			)
		}
		frozen := environment
		frozen.DeletionTaskID = ""
		frozenValue, encodeErr := encodeEnvironment(frozen)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		fixedInputDigest = hierarchyDeletionBytesDigest(frozenValue)
		clear(frozenValue)
	}
	registry, err := decodeEnvelope[EnvironmentPoolRegistry](result.Values[1].Value, "environment_pool_registry")
	if err != nil || validateEnvironmentPoolRegistry(registry) != nil {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	next, err := registry.Release(environment.ID, environment.NetworkPool)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	mutation := Mutation{Type: MutationDelete, Key: environmentPoolRegistryKey}
	values := [][]byte(nil)
	if len(next.Reservations) != 0 {
		value, encodeErr := encodeEnvelope("environment_pool_registry", next)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		values = append(values, value)
		mutation = Mutation{Type: MutationPut, Key: environmentPoolRegistryKey, Value: value}
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: fixedInputDigest,
		conditions: []Condition{
			{Key: environmentKey(environment.ID), ModRevision: result.Values[0].ModRevision},
			{Key: environmentPoolRegistryKey, ModRevision: result.Values[1].ModRevision},
		},
		mutations: []Mutation{mutation}, values: values,
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRunnerFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	base, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		runnerKey(action.TargetID), runnerLifecycleKey(action.TargetID),
		runnerObservationKey(action.TargetID), runnerRuntimeOwnershipKey(action.TargetID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if base == nil || len(base.Values) != 4 || base.Values[0] == nil || base.Values[1] == nil ||
		base.Values[0].ModRevision != action.TargetRevision || base.Values[3] != nil {
		if base != nil {
			clearKeyValues(base.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(errs.KindStateConflict, "hierarchy deletion Runner cleanup authority changed")
	}
	defer clearKeyValues(base.Values)
	record, err := decodeRunnerAggregate(base.Values[0], base.Values[1])
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	allocation, err := (&RunnerRepository{store: repository.store}).readRunnerAllocationEvidence(ctx, record, 0)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clearRunnerAllocationEvidence(allocation)
	quota, err := decodeRunnerTenantQuota(allocation.quota.Value)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	quota, err = quota.Release(action.TargetID)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	system, err := decodeSystemPoolRegistry(allocation.system.Value)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	system, err = system.ReleaseRunner(action.TargetID, record.Allocation.NetworkCIDR)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	quotaValue, err := encodeEnvelope("runner_tenant_quota", quota)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	systemValue, err := encodeEnvelope("system_pool_registry", system)
	if err != nil {
		clear(quotaValue)
		return hierarchyDeletionControllerEffects{}, err
	}
	conditions := []Condition{
		{Key: runnerKey(action.TargetID), ModRevision: base.Values[0].ModRevision},
		{Key: runnerLifecycleKey(action.TargetID), ModRevision: base.Values[1].ModRevision},
		{Key: runnerObservationKey(action.TargetID), ModRevision: keyValueRevision(base.Values[2])},
		{Key: runnerRuntimeOwnershipKey(action.TargetID)},
		{Key: runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID), ModRevision: allocation.owner.ModRevision},
		{Key: runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug), ModRevision: allocation.slug.ModRevision},
		{Key: runnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: systemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
	}
	mutations := []Mutation{
		{Type: MutationDelete, Key: runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID)},
		{Type: MutationDelete, Key: runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		{Type: MutationPut, Key: runnerTenantQuotaKey(record.Desired.TenantID), Value: quotaValue},
		{Type: MutationDelete, Key: runnerHostSlotKey(record.Allocation.Slot)},
		{Type: MutationPut, Key: systemPoolRegistryKey, Value: systemValue},
		{Type: MutationDelete, Key: runnerObservationKey(action.TargetID)},
		{Type: MutationDelete, Key: runnerLifecycleKey(action.TargetID)},
		{Type: MutationDelete, Key: runnerKey(action.TargetID)},
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(base.Values[0].Value), conditions: conditions,
		mutations: mutations, values: [][]byte{quotaValue, systemValue},
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionProjectFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, projectKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeProject(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	prefixes := []string{
		environmentOwnerPrefix(record.ID), runnerOwnerPrefix(RunnerOwnerProject, record.ID),
		secretOwnerCollectionPrefix(core.SecretScopeProject, record.ID),
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{projectSlugKey(record), projectOwnerKey(record)}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.ID || string(indexes.Values[1].Value) != record.ID {
		if indexes != nil {
			clearKeyValues(indexes.Values)
		}
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	defer clearKeyValues(indexes.Values)
	conditions := []Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: indexes.Values[0].Key, ModRevision: indexes.Values[0].ModRevision},
		{Key: indexes.Values[1].Key, ModRevision: indexes.Values[1].ModRevision},
	}
	for _, prefix := range prefixes {
		conditions = append(conditions, Condition{Key: prefix, Prefix: true})
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions,
		mutations: []Mutation{
			{Type: MutationDelete, Key: projectSlugKey(record)},
			{Type: MutationDelete, Key: projectOwnerKey(record)},
			{Type: MutationDelete, Key: projectKey(record.ID)},
		},
	}, nil
}
