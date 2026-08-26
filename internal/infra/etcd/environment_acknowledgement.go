package etcd

import (
	"context"
	"strings"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const environmentBlueprintDeletionBatchSize int64 = 32

const environmentReleaseGroupOwnerPrefix = "/v1/indexes/release-groups/by-owner/environment/"

func (repository *TaskRepository) prepareEnvironmentCreationAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) ([]Condition, []Mutation, []byte, error) {
	state, err := repository.readTaskEnvironmentMutationState(ctx, task.Target, readRevision, true)
	if err != nil {
		return nil, nil, nil, err
	}
	record := state.Environment.Record
	if record.ID != task.Target || record.CreateTaskID != task.ID {
		return nil, nil, nil, errs.New(
			errs.KindStateConflict,
			"environment provisioning belongs to another Task",
		)
	}
	replacement, err := CompleteEnvironmentProvisioning(
		record,
		task.ID,
		terminalStatus == TaskStatusCompleted,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	encoded, err := encodeEnvironment(replacement)
	if err != nil {
		return nil, nil, nil, err
	}
	conditions := []Condition{
		{Key: environmentKey(record.ID), ModRevision: state.Environment.Revision},
		{Key: environmentMutationEpochKey(record.ID), ModRevision: state.EpochRevision},
		{Key: environmentOperationLockKey(record.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: environmentKey(record.ID), Value: encoded},
		{Type: MutationPut, Key: environmentMutationEpochKey(record.ID), Value: state.EpochValue},
	}
	return conditions, mutations, encoded, nil
}

func (repository *TaskRepository) validateEnvironmentCreationReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) error {
	state, err := repository.readTaskEnvironmentMutationState(ctx, task.Target, readRevision, false)
	if err != nil {
		return err
	}
	record := state.Environment.Record
	want := EnvironmentProvisioningFailed
	if terminalStatus == TaskStatusCompleted {
		want = EnvironmentProvisioningReady
	}
	if record.ID != task.Target || record.CreateTaskID != task.ID ||
		record.ProvisioningState != want {
		return errs.New(
			errs.KindStateConflict,
			"environment provisioning terminal state does not match its Task",
		)
	}
	return nil
}

func (repository *TaskRepository) finalizeEnvironmentBlueprintRevisionBatch(
	ctx context.Context,
	task TaskRecord,
	updatedAt time.Time,
) (bool, error) {
	prefix := environmentBlueprintRevisionsPrefix(task.Target)
	page, err := repository.store.Range(ctx, RangeRequest{
		Prefix: prefix, Limit: environmentBlueprintDeletionBatchSize,
	})
	if err != nil {
		return false, err
	}
	if page == nil || page.ReadRevision <= 0 ||
		len(page.Values) > int(environmentBlueprintDeletionBatchSize) {
		return false, errs.New(errs.KindInternal, "environment Blueprint deletion page is invalid")
	}
	if len(page.Values) == 0 {
		return false, nil
	}
	tombstoneResult, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
			environmentMutationEpochKey(task.Target),
			environmentOperationLockKey(task.Target),
		},
		Revision: page.ReadRevision,
	})
	if err != nil {
		return false, err
	}
	if tombstoneResult == nil || tombstoneResult.ReadRevision != page.ReadRevision ||
		len(tombstoneResult.Values) != 3 {
		return false, errs.New(
			errs.KindInternal,
			"environment deletion fencing evidence is invalid",
		)
	}
	if tombstoneResult.Values[0] == nil {
		return false, errs.New(errs.KindStateConflict, "environment deletion tombstone is missing")
	}
	if tombstoneResult.Values[1] == nil {
		return false, errs.New(errs.KindInternal, "environment mutation epoch is missing")
	}
	if tombstoneResult.Values[2] == nil {
		return false, errs.New(
			errs.KindStateConflict,
			"environment deletion operation lock is missing",
		)
	}
	tombstoneValue := tombstoneResult.Values[0]
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return false, err
	}
	if tombstone.TargetKind != DeletionTargetEnvironment || tombstone.TargetID != task.Target ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return false, errs.New(
			errs.KindStateConflict,
			"environment deletion tombstone does not match its Task",
		)
	}
	ownedFence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		task.Target,
		page.ReadRevision,
		environmentMutationFenceOwner{
			Kind: BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
		},
	)
	if err != nil {
		return false, err
	}
	revisionID, err := environmentBlueprintRevisionIDFromKey(
		prefix,
		page.Values[len(page.Values)-1].Key,
	)
	if err != nil {
		return false, err
	}
	tombstone.Phase = DeletionPhaseFinalizing
	tombstone.Checkpoint = DeletionCheckpoint{
		ResourceKind: "blueprint_revision",
		StableID:     revisionID,
	}
	tombstone.UpdatedAt = updatedAt
	encodedTombstone, err := encodeDeletionTombstone(tombstone)
	if err != nil {
		return false, err
	}
	defer clear(encodedTombstone)

	conditions := make([]Condition, 0, len(page.Values)+3)
	mutations := make([]Mutation, 0, len(page.Values)+2)
	for _, value := range page.Values {
		conditions = append(conditions, Condition{Key: value.Key, ModRevision: value.ModRevision})
		mutations = append(mutations, Mutation{Type: MutationDelete, Key: value.Key})
	}
	conditions = append(conditions, Condition{
		Key:         deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
		ModRevision: tombstoneValue.ModRevision,
	})
	mutations = append(mutations, Mutation{
		Type:  MutationPut,
		Key:   deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
		Value: encodedTombstone,
	})
	conditions, err = appendEnvironmentMutationFenceConditions(conditions, ownedFence)
	if err != nil {
		return false, err
	}
	epochMutation, err := ownedFence.epochRewriteMutation()
	if err != nil {
		return false, err
	}
	defer clear(epochMutation.Value)
	mutations = append(mutations, epochMutation)
	if err := validateEnvironmentMutationTransactionBudget(conditions, mutations); err != nil {
		return false, err
	}
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return false, errs.New(
			errs.KindStateConflict,
			"environment Blueprint finalization state changed",
		)
	}
	return true, nil
}

func environmentBlueprintRevisionIDFromKey(prefix string, key string) (string, error) {
	remainder := strings.TrimPrefix(key, prefix)
	separator := strings.IndexByte(remainder, '/')
	if remainder == key || separator <= 0 ||
		validateStableID(ids.KindTask, remainder[:separator]) != nil {
		return "", errs.New(errs.KindInternal, "environment Blueprint revision key is corrupt")
	}
	return remainder[:separator], nil
}

func (repository *TaskRepository) prepareEnvironmentRemovalAcknowledgement(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) ([]Condition, []Mutation, error) {
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
			environmentBlueprintHeadKey(task.Target),
			environmentComposeProjectionKey(task.Target),
			environmentPoolRegistryKey,
			environmentMutationEpochKey(task.Target),
			environmentOperationLockKey(task.Target),
		},
		Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if stored == nil || stored.ReadRevision != readRevision || len(stored.Values) != 7 ||
		stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[4] == nil ||
		stored.Values[5] == nil || stored.Values[6] == nil {
		return nil, nil, errs.New(errs.KindInternal, "environment deletion state is inconsistent")
	}
	if (stored.Values[2] == nil) != (stored.Values[3] == nil) {
		return nil, nil, errs.New(errs.KindInternal, "environment Blueprint state is inconsistent")
	}
	environmentValue := stored.Values[0]
	environment, err := decodeEnvironment(environmentValue.Value)
	if err != nil {
		return nil, nil, err
	}
	tombstoneValue := stored.Values[1]
	tombstone, err := decodeDeletionTombstone(tombstoneValue.Value)
	if err != nil {
		return nil, nil, err
	}
	if environment.ID != task.Target || tombstone.TargetKind != DeletionTargetEnvironment ||
		tombstone.TargetID != environment.ID || tombstone.TargetRevision != environmentValue.ModRevision ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return nil, nil, errs.New(
			errs.KindStateConflict,
			"environment deletion tombstone does not match its Task",
		)
	}
	intentConditions, intentMutations, err := repository.prepareEnvironmentDeletionIntentTerminal(
		ctx, task, tombstone, terminalStatus, readRevision,
	)
	if err != nil {
		return nil, nil, err
	}
	epoch, err := decodeEnvironmentDeletionEpoch(stored.Values[5], environment.ID)
	if err != nil {
		return nil, nil, err
	}
	if _, err := decodeOwnedEnvironmentDeletionLock(stored.Values[6], task); err != nil {
		return nil, nil, err
	}
	ownedFence, err := loadOwnedEnvironmentMutationFence(
		ctx,
		repository.store,
		environment.ID,
		readRevision,
		environmentMutationFenceOwner{
			Kind: BackupOperationDeletion, OperationID: task.OperationID, TaskID: task.ID,
		},
	)
	if err != nil {
		return nil, nil, err
	}
	poolRegistry, err := decodeEnvelope[EnvironmentPoolRegistry](
		stored.Values[4].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil ||
		poolRegistry.Reservations[environment.ID] != environment.NetworkPool {
		return nil, nil, errs.New(errs.KindInternal, "environment pool reservation is inconsistent")
	}
	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentNameKey(environment.ProjectID, environment.Name),
			environmentOwnerKey(environment.ProjectID, environment.ID),
		},
		Revision: readRevision,
	})
	if err != nil {
		return nil, nil, err
	}
	if len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(
			indexes.Values[0].Value,
		) != environment.ID || string(indexes.Values[1].Value) != environment.ID {
		return nil, nil, errs.New(errs.KindInternal, "environment deletion indexes are corrupt")
	}
	conditions := []Condition{
		{Key: environmentKey(environment.ID), ModRevision: environmentValue.ModRevision},
		{
			Key:         environmentNameKey(environment.ProjectID, environment.Name),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key:         environmentOwnerKey(environment.ProjectID, environment.ID),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{
			Key:         deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID),
			ModRevision: tombstoneValue.ModRevision,
		},
		{
			Key:         environmentBlueprintHeadKey(environment.ID),
			ModRevision: keyValueRevision(stored.Values[2]),
		},
		{
			Key:         environmentComposeProjectionKey(environment.ID),
			ModRevision: keyValueRevision(stored.Values[3]),
		},
		{
			Key:         environmentMutationEpochKey(environment.ID),
			ModRevision: stored.Values[5].ModRevision,
		},
		{
			Key:         environmentOperationLockKey(environment.ID),
			ModRevision: stored.Values[6].ModRevision,
		},
	}
	conditions, err = appendEnvironmentMutationFenceConditions(conditions, ownedFence)
	if err != nil {
		return nil, nil, err
	}
	conditions = append(conditions, intentConditions...)
	if terminalStatus == TaskStatusCompleted {
		if err := requireEnvironmentDeletionLiveAuthorityEmpty(
			ctx, repository.store, environment.ID, task.OperationID, readRevision,
		); err != nil {
			return nil, nil, err
		}
		conditions = append(
			conditions,
			environmentDeletionLiveAuthorityConditions(environment.ID, task.OperationID)...,
		)
	}
	mutations := make([]Mutation, 0, len(intentMutations)+12)
	if terminalStatus == TaskStatusCompleted {
		mutations = append(mutations,
			Mutation{
				Type: MutationDelete,
				Key:  deletionTombstoneKey(string(DeletionTargetEnvironment), environment.ID),
			},
			Mutation{Type: MutationDelete, Key: environmentOperationLockKey(environment.ID)},
		)
		mutations = append(mutations, intentMutations...)
		mutations = append(
			mutations,
			Mutation{Type: MutationDelete, Key: environmentMutationEpochKey(environment.ID)},
		)
		nextPoolRegistry, err := poolRegistry.Release(environment.ID, environment.NetworkPool)
		if err != nil {
			return nil, nil, err
		}
		conditions = append(conditions, Condition{
			Key: environmentPoolRegistryKey, ModRevision: stored.Values[4].ModRevision,
		})
		if len(nextPoolRegistry.Reservations) == 0 {
			mutations = append(
				mutations,
				Mutation{Type: MutationDelete, Key: environmentPoolRegistryKey},
			)
		} else {
			poolRegistryValue, err := encodeEnvelope("environment_pool_registry", nextPoolRegistry)
			if err != nil {
				return nil, nil, err
			}
			mutations = append(mutations, Mutation{
				Type: MutationPut, Key: environmentPoolRegistryKey, Value: poolRegistryValue,
			})
		}
		remaining, err := repository.store.Range(ctx, RangeRequest{
			Prefix: environmentBlueprintRevisionsPrefix(
				environment.ID,
			),
			Limit:    1,
			Revision: readRevision,
		})
		if err != nil {
			return nil, nil, err
		}
		if remaining == nil || remaining.ReadRevision != readRevision {
			return nil, nil, errs.New(
				errs.KindInternal,
				"environment Blueprint finalization revision changed",
			)
		}
		if len(remaining.Values) != 0 {
			return nil, nil, errs.New(
				errs.KindStateConflict,
				"environment Blueprint revisions remain during finalization",
			)
		}
		if stored.Values[2] != nil {
			mutations = append(
				mutations,
				Mutation{Type: MutationDelete, Key: environmentBlueprintHeadKey(environment.ID)},
				Mutation{
					Type: MutationDelete,
					Key:  environmentComposeProjectionKey(environment.ID),
				},
			)
		}
		mutations = append(
			mutations,
			Mutation{
				Type: MutationDelete,
				Key:  environmentNameKey(environment.ProjectID, environment.Name),
			},
			Mutation{
				Type: MutationDelete,
				Key:  environmentOwnerKey(environment.ProjectID, environment.ID),
			},
			Mutation{Type: MutationDelete, Key: environmentKey(environment.ID)},
		)
	} else {
		epochValue, err := encodeEnvironmentMutationEpochRecord(epoch)
		if err != nil {
			return nil, nil, err
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: environmentMutationEpochKey(environment.ID), Value: epochValue,
		})
	}
	if err := validateEnvironmentMutationTransactionBudget(conditions, mutations); err != nil {
		clearMutationValues(mutations)
		return nil, nil, err
	}
	return conditions, mutations, nil
}

func requireEnvironmentDeletionLiveAuthorityEmpty(
	ctx context.Context,
	store hierarchyStore,
	environmentID string,
	operationID string,
	revision int64,
) error {
	direct, err := store.GetMany(ctx, GetManyRequest{
		Keys: environmentDeletionLiveAuthorityKeys(environmentID), Revision: revision,
	})
	if err != nil {
		return err
	}
	if direct == nil || direct.ReadRevision != revision ||
		len(direct.Values) != len(environmentDeletionLiveAuthorityKeys(environmentID)) {
		if direct != nil {
			clearKeyValues(direct.Values)
		}
		return errs.New(errs.KindInternal, "environment child authority evidence is incomplete")
	}
	for _, value := range direct.Values {
		if value != nil {
			clearKeyValues(direct.Values)
			return errs.New(errs.KindStateConflict, "environment retained live child authority")
		}
	}
	clearKeyValues(direct.Values)
	for _, prefix := range environmentDeletionLiveAuthorityPrefixes(environmentID, operationID) {
		page, err := store.Range(ctx, RangeRequest{Prefix: prefix, Limit: 1, Revision: revision})
		if err != nil {
			return err
		}
		if page == nil || page.ReadRevision != revision || len(page.Values) > 1 {
			if page != nil {
				clearRangeValues(page.Values)
			}
			return errs.New(errs.KindInternal, "environment child authority evidence is incomplete")
		}
		present := len(page.Values) != 0
		clearRangeValues(page.Values)
		if present {
			return errs.New(errs.KindStateConflict, "environment retained durable children")
		}
	}
	return nil
}

func environmentDeletionLiveAuthorityConditions(environmentID string, operationID string) []Condition {
	keys := environmentDeletionLiveAuthorityKeys(environmentID)
	conditions := make([]Condition, 0, len(keys)+19)
	for _, key := range keys {
		conditions = append(conditions, Condition{Key: key})
	}
	for _, prefix := range environmentDeletionLiveAuthorityPrefixes(environmentID, operationID) {
		conditions = append(conditions, Condition{Key: prefix, Prefix: true})
	}
	return conditions
}

func environmentDeletionLiveAuthorityKeys(environmentID string) []string {
	return []string{
		zonePoolRegistryKey(environmentID),
		environmentCoordinationKey(environmentID),
		componentTaskActiveEnvironmentKey(environmentID),
		backupPolicyKey(environmentID),
		backupKeyKey(environmentID),
		backupKeyValueKey(environmentID),
	}
}

func environmentDeletionLiveAuthorityPrefixes(environmentID string, operationID string) []string {
	return []string{
		zoneOwnerPrefix(environmentID),
		serviceOwnerPrefix(environmentID),
		routeOwnerPrefix(environmentID),
		entryOwnerCollectionPrefix(environmentID),
		scriptOwnerPrefix(environmentID),
		attachOwnerPrefix(environmentID),
		componentEnvironmentOwnerPrefix(environmentID),
		connectorEnvironmentPrefix(environmentID),
		environmentReleaseGroupOwnerPrefix + environmentID + "/",
		backupSourceEnvironmentPrefix(environmentID),
		backupScheduleCursorPrefix + environmentID + "/",
		backupDueOutcomePrefix + environmentID + "/",
		backupRecoveryPointEnvironmentPrefix + environmentID + "/",
		backupRunEnvironmentPrefix + environmentID + "/",
		backupOrphanEnvironmentPrefix + environmentID + "/",
		backupRestoreEnvironmentPrefix + environmentID + "/",
		backupKeyRotationEnvironmentPrefix + environmentID + "/",
		environmentDeletionWorkOperationPrefix(operationID),
	}
}

func appendEnvironmentMutationFenceConditions(
	conditions []Condition,
	fence environmentMutationFenceEvidence,
) ([]Condition, error) {
	indexes := make(map[string]int, len(conditions))
	for index, condition := range conditions {
		if condition.Key == "" || condition.Prefix {
			return nil, errs.New(errs.KindInternal, "environment mutation compare is invalid")
		}
		if _, duplicate := indexes[condition.Key]; duplicate {
			return nil, errs.New(errs.KindInternal, "environment mutation compare is duplicated")
		}
		indexes[condition.Key] = index
	}
	for _, required := range fence.transactionConditions() {
		if index, found := indexes[required.Key]; found {
			if conditions[index].ModRevision != required.ModRevision {
				return nil, errs.New(
					errs.KindInternal,
					"environment mutation compare conflicts with its fence",
				)
			}
			continue
		}
		indexes[required.Key] = len(conditions)
		conditions = append(conditions, required)
	}
	return conditions, nil
}

func validateEnvironmentMutationTransactionBudget(
	conditions []Condition,
	mutations []Mutation,
) error {
	if len(conditions)+len(mutations) > maximumTransactionOperations {
		return errs.New(
			errs.KindValidationFailed,
			"environment mutation exceeds the atomic transaction limit",
		)
	}
	return nil
}

func (repository *TaskRepository) validateEnvironmentRemovalReplay(
	ctx context.Context,
	task TaskRecord,
	terminalStatus TaskStatus,
	readRevision int64,
) error {
	stored, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			environmentKey(task.Target),
			deletionTombstoneKey(string(DeletionTargetEnvironment), task.Target),
			environmentBlueprintHeadKey(task.Target),
			environmentComposeProjectionKey(task.Target),
			environmentPoolRegistryKey,
			environmentMutationEpochKey(task.Target),
			environmentOperationLockKey(task.Target),
		},
		Revision: readRevision,
	})
	if err != nil {
		return err
	}
	if stored == nil || stored.ReadRevision != readRevision || len(stored.Values) != 7 ||
		(stored.Values[2] == nil) != (stored.Values[3] == nil) {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion terminal state does not match its Task",
		)
	}
	if terminalStatus == TaskStatusCompleted {
		if stored.Values[0] != nil || stored.Values[1] != nil || stored.Values[2] != nil ||
			stored.Values[3] != nil ||
			stored.Values[5] != nil ||
			stored.Values[6] != nil {
			return errs.New(
				errs.KindStateConflict,
				"completed environment deletion retained its target",
			)
		}
		if err := repository.validateEnvironmentDeletionIntentReplay(
			ctx, task, nil, terminalStatus, readRevision,
		); err != nil {
			return err
		}
		if stored.Values[4] != nil {
			poolRegistry, err := decodeEnvelope[EnvironmentPoolRegistry](
				stored.Values[4].Value,
				"environment_pool_registry",
			)
			if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil {
				return errs.New(errs.KindInternal, "environment pool registry is inconsistent")
			}
			if _, retained := poolRegistry.Reservations[task.Target]; retained {
				return errs.New(
					errs.KindStateConflict,
					"completed environment deletion retained its pool",
				)
			}
		}
		return nil
	}
	if stored.Values[0] == nil || stored.Values[1] == nil || stored.Values[5] == nil ||
		stored.Values[6] == nil {
		return errs.New(errs.KindStateConflict, "environment deletion retry state is missing")
	}
	environment, err := decodeEnvironment(stored.Values[0].Value)
	if err != nil {
		return err
	}
	if environment.ID != task.Target {
		return errs.New(errs.KindStateConflict, "environment deletion retained another target")
	}
	tombstone, err := decodeDeletionTombstone(stored.Values[1].Value)
	if err != nil {
		return err
	}
	if tombstone.TargetKind != DeletionTargetEnvironment || tombstone.TargetID != task.Target ||
		tombstone.TargetRevision != stored.Values[0].ModRevision || tombstone.TaskID != task.ID ||
		(tombstone.Phase != DeletionPhaseHostEffects && tombstone.Phase != DeletionPhaseFinalizing) {
		return errs.New(
			errs.KindStateConflict,
			"environment deletion retry tombstone ownership changed",
		)
	}
	if _, err := decodeEnvironmentDeletionEpoch(stored.Values[5], environment.ID); err != nil {
		return err
	}
	if _, err := decodeOwnedEnvironmentDeletionLock(stored.Values[6], task); err != nil {
		return err
	}
	if err := repository.validateEnvironmentDeletionIntentReplay(
		ctx, task, &tombstone, terminalStatus, readRevision,
	); err != nil {
		return err
	}
	if stored.Values[4] == nil {
		return errs.New(errs.KindStateConflict, "environment deletion lost its pool")
	}
	poolRegistry, err := decodeEnvelope[EnvironmentPoolRegistry](
		stored.Values[4].Value,
		"environment_pool_registry",
	)
	if err != nil || validateEnvironmentPoolRegistry(poolRegistry) != nil ||
		poolRegistry.Reservations[environment.ID] != environment.NetworkPool {
		return errs.New(errs.KindStateConflict, "environment deletion changed its pool")
	}
	return nil
}
