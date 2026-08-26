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
	case HierarchyDeletionScriptRemove:
		return repository.prepareHierarchyDeletionScriptFinalizer(ctx, action)
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
	primary, err := repository.readHierarchyDeletionPrimary(ctx, scriptKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeScriptRecord(primary.Value)
	if err != nil || record.Desired.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	keys := []string{scriptOwnerKey(record.EnvironmentID, action.TargetID), scriptNameKey(record.EnvironmentID, record.Desired.Name)}
	return repository.prepareHierarchyDeletionIndexedDelete(ctx, action, primary, keys)
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
	if result == nil || len(result.Values) != 2 || result.Values[0] == nil || result.Values[1] == nil ||
		result.Values[0].ModRevision != action.TargetRevision {
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
		fixedInputDigest: hierarchyDeletionBytesDigest(result.Values[0].Value),
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

func (repository *HierarchyDeletionRepository) prepareHierarchyDeletionEnvironmentFinalizer(
	ctx context.Context,
	operation HierarchyDeletionOperation,
	action HierarchyDeletionAction,
) (hierarchyDeletionControllerEffects, error) {
	primary, err := repository.readHierarchyDeletionPrimary(ctx, environmentKey(action.TargetID), action)
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	defer clear(primary.Value)
	record, err := decodeEnvironment(primary.Value)
	if err != nil || record.ID != action.TargetID {
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		environmentNameKey(record.ProjectID, record.Name), environmentOwnerKey(record.ProjectID, record.ID),
		environmentBlueprintHeadKey(record.ID), environmentComposeProjectionKey(record.ID),
	}})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if indexes == nil || len(indexes.Values) != 4 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != record.ID || string(indexes.Values[1].Value) != record.ID ||
		(indexes.Values[2] == nil) != (indexes.Values[3] == nil) {
		if indexes != nil {
			clearKeyValues(indexes.Values)
		}
		return hierarchyDeletionControllerEffects{}, corruptHierarchyDeletion()
	}
	defer clearKeyValues(indexes.Values)
	if err := requireEnvironmentDeletionLiveAuthorityEmpty(
		ctx, repository.store, record.ID, operation.Tombstone.OperationID, indexes.ReadRevision,
	); err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	revisions, err := repository.store.Range(ctx, RangeRequest{
		Prefix: environmentBlueprintRevisionsPrefix(record.ID), Limit: 1, Revision: indexes.ReadRevision,
	})
	if err != nil {
		return hierarchyDeletionControllerEffects{}, err
	}
	if revisions == nil || revisions.ReadRevision != indexes.ReadRevision || len(revisions.Values) != 0 {
		if revisions != nil {
			clearRangeValues(revisions.Values)
		}
		return hierarchyDeletionControllerEffects{}, errs.New(errs.KindStateConflict, "hierarchy deletion Environment retained Blueprint revisions")
	}
	conditions := []Condition{
		{Key: primary.Key, ModRevision: primary.ModRevision},
		{Key: indexes.Values[0].Key, ModRevision: indexes.Values[0].ModRevision},
		{Key: indexes.Values[1].Key, ModRevision: indexes.Values[1].ModRevision},
		{Key: environmentBlueprintHeadKey(record.ID), ModRevision: keyValueRevision(indexes.Values[2])},
		{Key: environmentComposeProjectionKey(record.ID), ModRevision: keyValueRevision(indexes.Values[3])},
	}
	conditions = append(conditions, environmentDeletionLiveAuthorityConditions(record.ID, operation.Tombstone.OperationID)...)
	conditions = append(conditions, Condition{Key: environmentBlueprintRevisionsPrefix(record.ID), Prefix: true})
	mutations := []Mutation{
		{Type: MutationDelete, Key: environmentBlueprintHeadKey(record.ID)},
		{Type: MutationDelete, Key: environmentComposeProjectionKey(record.ID)},
		{Type: MutationDelete, Key: environmentNameKey(record.ProjectID, record.Name)},
		{Type: MutationDelete, Key: environmentOwnerKey(record.ProjectID, record.ID)},
		{Type: MutationDelete, Key: environmentKey(record.ID)},
	}
	return hierarchyDeletionControllerEffects{
		fixedInputDigest: hierarchyDeletionBytesDigest(primary.Value), conditions: conditions, mutations: mutations,
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
