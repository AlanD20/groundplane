package etcd

import (
	"context"
	backupruntime "github.com/AlanD20/groundplane/internal/infra/etcd/backupruntime"
	componentrecord "github.com/AlanD20/groundplane/internal/infra/etcd/components"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	hierarchydeletion "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchydeletion"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"
)

const maximumBackingServiceTransactionRequestOperations = 128

// BackingServiceCreation is the complete durable input for one backing
// facade. The repository publishes every public member and its first desired
// revision in one transaction so readers can never observe a partial facade.
type BackingServiceCreation struct {
	VolumeRoot   string
	Stage        etcdstore.Versioned[BackingServiceCreationStage]
	PoolRegistry etcdstore.Versioned[EnvironmentPoolRegistry]
	Project      hierarchyrecord.ProjectRecord
	Environment  hierarchyrecord.EnvironmentRecord
	Components   []componentrecord.Record
	Zone         zonerecord.Record
	Service      servicerecord.ServiceRecord
	Secrets      []secretrecord.Record
	SecretValues []secretrecord.EncryptedValue
	Entries      []entryrecord.Record
	EntryValues  []EntryValueGeneration
	Claim        EnvironmentBlueprintStageClaim
	Revision     EnvironmentDesiredRevisionIdentity
	Projection   projectionrecord.EnvironmentComposeProjection
	Task         TaskRecord
	HookInputs   *BackingHookEncryptedInputs
	Marker       idempotencyrecord.IdempotencyMarker
}

// PublishBackingServiceWithTask atomically creates one backing facade and
// queues the Agent reconciliation pinned to its immutable desired revision.
// Blueprint audit and projection chunks must already be sealed by the normal
// staging protocol.
func (repository *HierarchyRepository) PublishBackingServiceWithTask(
	ctx context.Context,
	creation BackingServiceCreation,
) (_ IdempotencyTransactionResult, returnErr error) {
	creation.Task = cloneTaskRecord(creation.Task)
	if creation.Task.IdempotencyKey == "" {
		creation.Task.IdempotencyKey = creation.Marker.Locator.Key
	}
	creation.Task.idempotencyMarker = cloneIdempotencyLocator(&creation.Marker.Locator)
	hookPublication, err := prepareBackingHookTaskPublication(
		ctx, repository.store, creation.Task, creation.HookInputs,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer hookPublication.clear()
	defer func() { returnErr = hookPublication.finish(ctx, repository.store, returnErr) }()
	creation.Task = hookPublication.task
	if err := validateBackingServiceCreation(ctx, creation); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, creation.Marker); err != nil ||
		found {
		return existing, err
	}
	publication, err := repository.prepareEnvironmentBlueprintPublication(
		ctx,
		creation.Claim,
		creation.Revision,
		creation.Projection,
		creation.Task,
		creation.Marker,
		0,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)

	projectValue, err := hierarchyrecord.EncodeProject(creation.Project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(projectValue)
	environmentValue, err := hierarchyrecord.EncodeEnvironment(creation.Environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	projectCoordinationValue, err := encodeInitialHierarchyCoordination(
		hierarchydeletion.HierarchyDeletionTargetProject,
		creation.Project.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(projectCoordinationValue)
	environmentCoordinationValue, err := encodeInitialHierarchyCoordination(
		hierarchydeletion.HierarchyDeletionTargetEnvironment,
		creation.Environment.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentCoordinationValue)
	scriptSetValue, err := scriptrecord.EncodeScriptSetGeneration(scriptrecord.SetGenerationRecord{
		EnvironmentID: creation.Environment.ID, GenerationID: creation.Environment.ID,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(scriptSetValue)
	poolRegistryValue, err := recordcodec.Encode("environment_pool_registry", creation.PoolRegistry.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(poolRegistryValue)
	zoneRegistryValue, err := recordcodec.Encode("zone_pool_registry", zonePoolRegistry{
		Reservations: map[string]string{creation.Zone.Desired.ID: creation.Zone.Desired.Subnet},
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(zoneRegistryValue)
	serviceValue, err := servicerecord.EncodeServiceRuntimeRecord(servicerecord.NewServiceRuntimeRecord(creation.Service))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(serviceValue)
	epochValue, err := backupruntime.EncodeEnvironmentMutationEpochRecord(backupruntime.EnvironmentMutationEpochRecord{
		EnvironmentID: creation.Environment.ID,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochValue)
	taskValue, err := encodeTaskRecord(creation.Task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	taskReference, err := idempotencyrecord.EncodeTaskReference(creation.Task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskReference)
	componentValues := make([][]byte, len(creation.Components))
	for index := range creation.Components {
		componentValues[index], err = componentrecord.EncodeRecord(creation.Components[index])
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(componentValues[index])
	}
	entryValues := make([][]byte, len(creation.Entries))
	entryGenerationKeys := make([]string, len(creation.Entries))
	entryGenerationValues := make([][]byte, len(creation.Entries))
	for index := range creation.Entries {
		entryValues[index], err = entryrecord.EncodeRecord(creation.Entries[index])
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(entryValues[index])
		entryGenerationKeys[index], entryGenerationValues[index], err = prepareEntryGeneration(
			creation.Entries[index],
			creation.EntryValues[index],
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(entryGenerationValues[index])
	}
	secretValues := make([][]byte, len(creation.Secrets))
	secretEncryptedValues := make([][]byte, len(creation.Secrets))
	for index := range creation.Secrets {
		secretValues[index], err = secretrecord.EncodeRecord(creation.Secrets[index])
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(secretValues[index])
		secretEncryptedValues[index], err = secretrecord.EncodeEncryptedValue(creation.SecretValues[index])
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(secretEncryptedValues[index])
	}

	creationStageKey, err := backingServiceCreationStageKey(creation.Stage.Record.Locator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	conditions := backingServiceCreationConditions(creation, publication, creationStageKey)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationDelete, Key: creationStageKey},
		{Type: etcdstore.MutationPut, Key: taskKey(creation.Task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskOperationIndexKey(creation.Task.OperationID, creation.Task.ID),
			Value: taskReference,
		},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(creation.Task.OperationID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(creation.Task.Executor, creation.Task.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
		{Type: etcdstore.MutationPut, Key: environmentBlueprintHeadKey(creation.Environment.ID), Value: taskReference},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectKey(creation.Project.ID), Value: projectValue},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectSlugKey(creation.Project), Value: []byte(creation.Project.ID)},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.ProjectOwnerKey(creation.Project), Value: []byte(creation.Project.ID)},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetProject), creation.Project.ID),
			Value: projectCoordinationValue,
		},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(creation.Environment.ID), Value: environmentValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentNameKey(creation.Project.ID, creation.Environment.Name),
			Value: []byte(creation.Environment.ID),
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchyrecord.EnvironmentOwnerKey(creation.Project.ID, creation.Environment.ID),
			Value: []byte(creation.Environment.ID),
		},
		{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentMutationEpochKey(creation.Environment.ID), Value: epochValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   hierarchydeletion.HierarchyCoordinationKey(string(hierarchydeletion.HierarchyDeletionTargetEnvironment), creation.Environment.ID),
			Value: environmentCoordinationValue,
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(creation.Environment.ID), Value: scriptSetValue},
		{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: poolRegistryValue},
		{Type: etcdstore.MutationPut, Key: zonePoolRegistryKey(creation.Environment.ID), Value: zoneRegistryValue},
		{Type: etcdstore.MutationPut, Key: servicerecord.ServiceRuntimeKey(creation.Service.Desired.ID), Value: serviceValue},
	}
	for index, component := range creation.Components {
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: componentrecord.RecordKey(component.Desired.ID), Value: componentValues[index]},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID),
				Value: []byte(component.Desired.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentrecord.EnvironmentKindKey(creation.Environment.ID, component.Desired.Kind),
				Value: []byte(component.Desired.ID),
			},
		)
	}
	if len(creation.Components) > 0 {
		mutations = append(mutations, componentrecord.WriteFenceMutation(creation.Task.ID))
	}
	for index, entry := range creation.Entries {
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: entryrecord.RecordKey(entry.Entry.ID), Value: entryValues[index]},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   entryOwnerKey(creation.Environment.ID, entry.Entry.ID),
				Value: []byte(entry.Entry.ID),
			},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: entryGenerationKeys[index], Value: entryGenerationValues[index]},
		)
	}
	for index, secret := range creation.Secrets {
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: secretrecord.RecordKey(secret.Secret.ID), Value: secretValues[index]},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: secretOwnerKey(secret.Secret), Value: []byte(secret.Secret.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: secretScopedKey(secret.Secret), Value: []byte(secret.Secret.ID)},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: secretrecord.ValueKey(secret.Secret.ID), Value: secretEncryptedValues[index]},
		)
	}
	classifier := classifyBackingServiceCreation(creation, publication, len(conditions))
	conditions, mutations, classifier, err = hookPublication.bind(conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	initiation, err := newTaskInitiation(creation.Task.Owner, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(creation.Task, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := plan.enforceTransactionBounds(validateBackingServicePublicationBudget); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, creation.Marker, plan)
}
