package etcd

import (
	"context"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	connectorrecord "github.com/AlanD20/groundplane/internal/infra/etcd/connectors"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	routerecord "github.com/AlanD20/groundplane/internal/infra/etcd/routes"
	runnerrecord "github.com/AlanD20/groundplane/internal/infra/etcd/runners"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
	"time"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type hierarchyDeletionControllerEffects struct {
	fixedInputDigest string
	conditions       []etcdstore.Condition
	mutations        []etcdstore.Mutation
	values           [][]byte
}

func (repository *HierarchyDeletionRepository) CompleteControllerAction(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
	completedAt time.Time,
) (HierarchyDeletionOperation, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return HierarchyDeletionOperation{}, err
	}
	if recordcodec.ValidateTimestamp("hierarchy deletion Controller completion", completedAt) != nil ||
		action.ProcedureKind != HierarchyDeletionProcedureController || action.ControllerProcedure == nil {
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindValidationFailed,
			"hierarchy deletion Controller action is invalid",
		)
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
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"root hierarchy deletion finalizer requires Task acknowledgement",
		)
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
		return HierarchyDeletionOperation{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion finalizer template changed",
		)
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
		return repository.prepareHierarchyDeletionZoneFinalizer(ctx, operation, action)
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
			errs.KindValidationFailed,
			"hierarchy deletion Controller finalizer %q is unsupported before plan execution",
			action.ActionKind,
		)
	}
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRouteFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	result, err := repository.store.Get(ctx, routerecord.ObservationKey(action.TargetID))
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if result == nil || result.ReadRevision <= 0 {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	conditions := []etcdstore.Condition{{Key: routerecord.ObservationKey(action.TargetID)}}
	mutations := []etcdstore.Mutation{}
	digest := hierarchyDeletionBytesDigest([]byte(action.TargetID))
	if result.Entry != nil {
		if result.Entry.Key != routerecord.ObservationKey(action.TargetID) {
			clear(result.Entry.Value)
			return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
		}
		if _, decodeErr := routerecord.DecodeObservation(result.Entry.Value); decodeErr != nil {
			clear(result.Entry.Value)
			return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
		}
		conditions[0].ModRevision = result.Entry.ModRevision
		digest = hierarchyDeletionBytesDigest(result.Entry.Value)
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
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, componentrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := componentrecord.DecodeRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
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
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	evidence, zoneValue, err := hierarchyDeletionZoneEvidenceAtRevision(
		ctx, repository.store, action.TargetID, operation.Tombstone.SnapshotRevision, action.TargetRevision,
	)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(zoneValue)
	zone := zonerecord.Record{EnvironmentID: evidence.EnvironmentID, Desired: evidence.Desired}
	poolKey, addressesKey := zonePoolRegistryKey(evidence.EnvironmentID), componentAddressRegistryKey(action.TargetID)
	values, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		poolKey, addressesKey,
		deletionTombstoneKey(string(DeletionTargetZone), evidence.ZoneID),
		componentTaskActiveEnvironmentKey(evidence.EnvironmentID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if values == nil || len(values.Values) != 4 || values.Values[0] == nil ||
		values.Values[2] != nil || values.Values[3] != nil {
		if values != nil {
			clearKeyValues(values.Values)
		}
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	defer clearKeyValues(values.Values)
	pool, err := recordcodec.Decode[zonePoolRegistry](values.Values[0].Value, "zone_pool_registry")
	if err != nil || validateZonePoolRegistry(pool) != nil ||
		pool.Reservations[action.TargetID] != evidence.Desired.Subnet {
		return hierarchyDeletionControllerEffects{}, corruptZonePoolRegistry()
	}
	if values.Values[1] != nil {
		addresses, decodeErr := recordcodec.Decode[componentAddressRegistry](
			values.Values[1].Value,
			"component_address_registry",
		)
		if decodeErr != nil || validateComponentAddressRegistry(zone, addresses) != nil {
			return hierarchyDeletionControllerEffects{}, corruptComponentAddressRegistry()
		}
		if len(addresses.Reservations) != 0 {
			return hierarchyDeletionControllerEffects{}, errs.New(errs.KindResourceInUse,
				"Zone gained a Component address reservation")
		}
	}
	nextPool, err := pool.release(zone)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects := hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(zoneValue),
		conditions: []etcdstore.Condition{
			{Key: poolKey, ModRevision: values.Values[0].ModRevision},
			{Key: addressesKey, ModRevision: keyValueRevision(values.Values[1])},
			{Key: deletionTombstoneKey(string(DeletionTargetZone), evidence.ZoneID)},
			{Key: componentTaskActiveEnvironmentKey(evidence.EnvironmentID)},
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
func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionTenantFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, hierarchyrecord.TenantKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := hierarchyrecord.DecodeTenant(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	prefixes := []string{
		hierarchyrecord.ProjectTenantOwnerPrefix(record.ID), runnerOwnerPrefix(runnerrecord.RunnerOwnerTenant, record.ID),
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	slugKey := hierarchyrecord.TenantSlugKey(record.Slug)
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
	conditions := []etcdstore.Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: slugKey, ModRevision: slug.Entry.ModRevision},
	}
	for _, prefix := range prefixes {
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: slugKey},
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.TenantKey(record.ID)},
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
	storage, err := readActiveScriptStorage(
		ctx,
		repository.store,
		action.TargetID,
		action.ControllerProcedure.FixedInputRevision,
	)
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
	value, err := scriptrecord.EncodeRecord(record)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(value)
	primary := etcdstore.KeyValue{
		Key:   scriptrecord.ScriptSetScriptKey(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
		Value: value, ModRevision: storage.Script.Revision,
	}
	keys := []string{
		scriptrecord.ScriptSetOwnerKey(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
		scriptrecord.ScriptSetSlugKey(record.EnvironmentID, record.ScriptSetGeneration, record.Desired.Slug),
	}
	effects, err := repository.prepareHierarchyDeletionIndexedDelete(ctx, action, &primary, keys)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	activeValue, err := scriptrecord.EncodeScriptSetGeneration(storage.Active.Record)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	effects.values = append(effects.values, activeValue)
	effects.conditions = append(
		effects.conditions,
		etcdstore.Condition{Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), ModRevision: storage.Active.Revision},
		etcdstore.Condition{Key: scriptrecord.ScriptLocatorKey(action.TargetID), ModRevision: storage.Locator.Revision},
		etcdstore.Condition{
			Key:         scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, action.TargetID),
			ModRevision: storage.EnvironmentLocator.Revision,
		},
	)
	effects.mutations = append(
		effects.mutations,
		etcdstore.Mutation{
			Type:   etcdstore.MutationDelete,
			Key:    scriptrecord.ScriptSetBodyGenerationPrefix(record.EnvironmentID, record.ScriptSetGeneration, action.TargetID),
			Prefix: true,
		},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptLocatorKey(action.TargetID)},
		etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: scriptrecord.ScriptEnvironmentLocatorKey(record.EnvironmentID, action.TargetID)},
		etcdstore.Mutation{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(record.EnvironmentID), Value: activeValue},
	)
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionConnectorFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, connectorrecord.RecordKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := connectorrecord.DecodeRecord(primary.Value)
	if err != nil || record.Connector.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	keys := []string{
		connectorEnvironmentKey(record.Connector.EnvironmentID, action.TargetID),
		connectorNameKey(record.Connector.EnvironmentID, record.Connector.Name),
		connectorrecord.CredentialValueKey(action.TargetID),
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
		effects.conditions = append(effects.conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return effects, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionReservationFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	result, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		hierarchyrecord.EnvironmentKey(action.TargetID), environmentPoolRegistryKey,
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil {
		if result != nil {
			clearKeyValues(result.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion reservation authority changed",
		)
	}
	defer clearKeyValues(result.Values)
	environment, err := hierarchyrecord.DecodeEnvironment(result.Values[0].Value)
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
		frozenValue, encodeErr := hierarchyrecord.EncodeEnvironment(frozen)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		fixedInputDigest = hierarchyDeletionBytesDigest(frozenValue)
		clear(frozenValue)
	}
	registry, err := recordcodec.Decode[EnvironmentPoolRegistry](result.Values[1].Value, "environment_pool_registry")
	if err != nil || validateEnvironmentPoolRegistry(registry) != nil {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	next, err := registry.Release(environment.ID, environment.NetworkPool)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	mutation := etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: environmentPoolRegistryKey}
	values := [][]byte(nil)
	if len(next.Reservations) != 0 {
		value, encodeErr := recordcodec.Encode("environment_pool_registry", next)
		if encodeErr != nil {
			return hierarchyDeletionControllerEffects{}, encodeErr
		}
		values = append(values, value)
		mutation = etcdstore.Mutation{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: value}
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: fixedInputDigest,
		conditions: []etcdstore.Condition{
			{Key: hierarchyrecord.EnvironmentKey(environment.ID), ModRevision: result.Values[0].ModRevision},
			{Key: environmentPoolRegistryKey, ModRevision: result.Values[1].ModRevision},
		},
		mutations: []etcdstore.Mutation{mutation}, values: values,
	}, nil
}

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionRunnerFinalizer(
	ctx context.Context,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	base, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
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
		return hierarchyDeletionControllerEffects{}, errs.New(
			errs.KindStateConflict,
			"hierarchy deletion Runner cleanup authority changed",
		)
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
	quotaValue, err := recordcodec.Encode("runner_tenant_quota", quota)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	systemValue, err := recordcodec.Encode("system_pool_registry", system)
	if err != nil {
		clear(quotaValue)
		return hierarchyDeletionControllerEffects{}, err
	}
	conditions := []etcdstore.Condition{
		{Key: runnerKey(action.TargetID), ModRevision: base.Values[0].ModRevision},
		{Key: runnerLifecycleKey(action.TargetID), ModRevision: base.Values[1].ModRevision},
		{Key: runnerObservationKey(action.TargetID), ModRevision: keyValueRevision(base.Values[2])},
		{Key: runnerRuntimeOwnershipKey(action.TargetID)},
		{
			Key:         runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID),
			ModRevision: allocation.owner.ModRevision,
		},
		{
			Key:         runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug),
			ModRevision: allocation.slug.ModRevision,
		},
		{Key: runnerTenantQuotaKey(record.Desired.TenantID), ModRevision: allocation.quota.ModRevision},
		{Key: runnerHostSlotKey(record.Allocation.Slot), ModRevision: allocation.host.ModRevision},
		{Key: systemPoolRegistryKey, ModRevision: allocation.system.ModRevision},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: runnerOwnerKey(record.Desired.OwnerKind, record.Desired.OwnerID, action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerTenantSlugKey(record.Desired.TenantID, record.Desired.Slug)},
		{Type: etcdstore.MutationPut, Key: runnerTenantQuotaKey(record.Desired.TenantID), Value: quotaValue},
		{Type: etcdstore.MutationDelete, Key: runnerHostSlotKey(record.Allocation.Slot)},
		{Type: etcdstore.MutationPut, Key: systemPoolRegistryKey, Value: systemValue},
		{Type: etcdstore.MutationDelete, Key: runnerObservationKey(action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerLifecycleKey(action.TargetID)},
		{Type: etcdstore.MutationDelete, Key: runnerKey(action.TargetID)},
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
	primary, err := repository.readHierarchyDeletionPrimary(ctx, hierarchyrecord.ProjectKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := hierarchyrecord.DecodeProject(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	prefixes := []string{
		hierarchyrecord.EnvironmentOwnerPrefix(record.ID), runnerOwnerPrefix(runnerrecord.RunnerOwnerProject, record.ID),
		secretOwnerCollectionPrefix(core.SecretScopeProject, record.ID),
	}
	if _, err := repository.requireHierarchyDeletionPrefixesEmpty(ctx, prefixes); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	indexes, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{hierarchyrecord.ProjectSlugKey(record), hierarchyrecord.ProjectOwnerKey(record)}},
	)
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
	conditions := []etcdstore.Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: indexes.Values[0].Key, ModRevision: indexes.Values[0].ModRevision},
		{Key: indexes.Values[1].Key, ModRevision: indexes.Values[1].ModRevision},
	}
	for _, prefix := range prefixes {
		conditions = append(conditions, etcdstore.Condition{Key: prefix, Prefix: true})
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions,
		mutations: []etcdstore.Mutation{
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectSlugKey(record)},
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectOwnerKey(record)},
			{Type: etcdstore.MutationDelete, Key: hierarchyrecord.ProjectKey(record.ID)},
		},
	}, nil
}
