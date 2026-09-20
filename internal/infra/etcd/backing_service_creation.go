package etcd

import (
	"context"
	entryrecord "github.com/AlanD20/groundplane/internal/infra/etcd/entries"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	scriptrecord "github.com/AlanD20/groundplane/internal/infra/etcd/scripts"
	secretrecord "github.com/AlanD20/groundplane/internal/infra/etcd/secrets"
	zonerecord "github.com/AlanD20/groundplane/internal/infra/etcd/zones"

	"github.com/AlanD20/groundplane/pkg/errs"
)

const maximumBackingServiceTransactionRequestOperations = 128

// BackingServiceCreation is the complete durable input for one backing
// facade. The repository publishes every public member and its first desired
// revision in one transaction so readers can never observe a partial facade.
type BackingServiceCreation struct {
	VolumeRoot   string
	Stage        Versioned[BackingServiceCreationStage]
	PoolRegistry Versioned[EnvironmentPoolRegistry]
	Project      hierarchyrecord.ProjectRecord
	Environment  hierarchyrecord.EnvironmentRecord
	Components   []ComponentRecord
	Zone         zonerecord.Record
	Service      ServiceRecord
	Secrets      []secretrecord.Record
	SecretValues []secretrecord.EncryptedValue
	Entries      []entryrecord.Record
	EntryValues  []EntryValueGeneration
	Claim        EnvironmentBlueprintStageClaim
	Revision     EnvironmentDesiredRevisionIdentity
	Projection   EnvironmentComposeProjection
	Task         TaskRecord
	HookInputs   *BackingHookEncryptedInputs
	Marker       IdempotencyMarker
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
		HierarchyDeletionTargetProject,
		creation.Project.ID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(projectCoordinationValue)
	environmentCoordinationValue, err := encodeInitialHierarchyCoordination(
		HierarchyDeletionTargetEnvironment,
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
	serviceValue, err := encodeServiceRuntimeRecord(newServiceRuntimeRecord(creation.Service))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(serviceValue)
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
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
	taskReference, err := encodeTaskReference(creation.Task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskReference)
	componentValues := make([][]byte, len(creation.Components))
	for index := range creation.Components {
		componentValues[index], err = encodeComponentRecord(creation.Components[index])
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
			Key:   HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), creation.Project.ID),
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
			Key:   HierarchyCoordinationKey(string(HierarchyDeletionTargetEnvironment), creation.Environment.ID),
			Value: environmentCoordinationValue,
		},
		{Type: etcdstore.MutationPut, Key: scriptrecord.ScriptSetActiveKey(creation.Environment.ID), Value: scriptSetValue},
		{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: poolRegistryValue},
		{Type: etcdstore.MutationPut, Key: zonePoolRegistryKey(creation.Environment.ID), Value: zoneRegistryValue},
		{Type: etcdstore.MutationPut, Key: serviceRuntimeKey(creation.Service.Desired.ID), Value: serviceValue},
	}
	for index, component := range creation.Components {
		mutations = append(
			mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: componentKey(component.Desired.ID), Value: componentValues[index]},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentEnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID),
				Value: []byte(component.Desired.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentEnvironmentKindKey(creation.Environment.ID, component.Desired.Kind),
				Value: []byte(component.Desired.ID),
			},
		)
	}
	if len(creation.Components) > 0 {
		mutations = append(mutations, componentWriteFenceMutation(creation.Task.ID))
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

func validateBackingServicePublicationBudget(
	conditions []etcdstore.Condition,
	mutations []etcdstore.Mutation,
) error {
	return validateBackingServicePublicationOperationCounts(
		len(conditions),
		len(mutations),
		len(conditions),
	)
}

func validateBackingServicePublicationOperationCounts(
	comparisons int,
	successMutations int,
	failureReads int,
) error {
	selectedOperations := comparisons + successMutations
	requestOperations := selectedOperations + failureReads
	if selectedOperations > etcdstore.MaximumOperations ||
		requestOperations > maximumBackingServiceTransactionRequestOperations {
		return errs.Newf(
			errs.KindValidationFailed,
			"Backing-service publication exceeds transaction bounds (%d/%d/%d; selected %d/%d; request %d/%d)",
			comparisons,
			successMutations,
			failureReads,
			selectedOperations,
			etcdstore.MaximumOperations,
			requestOperations,
			maximumBackingServiceTransactionRequestOperations,
		)
	}
	return nil
}

func validateBackingServiceCreation(ctx context.Context, creation BackingServiceCreation) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := hierarchyrecord.ValidateProject(creation.Project); err != nil {
		return err
	}
	if creation.Project.Kind != hierarchyrecord.ProjectKindBacking || creation.Project.TenantID != "" {
		return errs.New(errs.KindValidationFailed, "Backing-service Project ownership is invalid")
	}
	if err := validateBackingServiceCreationStage(creation.Stage.Record); err != nil {
		return err
	}
	if creation.Stage.Revision <= 0 || creation.Stage.ReadRevision < creation.Stage.Revision ||
		creation.Stage.Record.ProjectID != creation.Project.ID ||
		creation.Stage.Record.EnvironmentID != creation.Environment.ID ||
		creation.Stage.Record.TaskID != creation.Task.ID || creation.Stage.Record.Locator != creation.Marker.Locator {
		return errs.New(errs.KindValidationFailed, "Backing-service creation stage does not match publication")
	}
	if err := hierarchyrecord.ValidateEnvironment(creation.Environment); err != nil {
		return err
	}
	if creation.Environment.ProjectID != creation.Project.ID || creation.Environment.Name != "main" ||
		creation.Environment.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		creation.Environment.CreateTaskID != creation.Task.ID ||
		!creation.Environment.CreatedAt.Equal(creation.Task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service Environment lifecycle is invalid")
	}
	if err := hierarchyrecord.ValidateEnvironmentVolumeDir(creation.VolumeRoot, creation.Project, creation.Environment); err != nil {
		return err
	}
	if err := validateBackingServiceComponents(creation.Components); err != nil {
		return err
	}
	if creation.PoolRegistry.Revision < 0 ||
		creation.PoolRegistry.ReadRevision < creation.PoolRegistry.Revision ||
		creation.PoolRegistry.Record.Reservations[creation.Environment.ID] != creation.Environment.NetworkPool {
		return errs.New(errs.KindValidationFailed, "Backing-service Environment pool reservation is invalid")
	}
	if err := validateEnvironmentPoolRegistry(creation.PoolRegistry.Record); err != nil {
		return err
	}
	if err := zonerecord.ValidateRecord(creation.Zone); err != nil {
		return err
	}
	if creation.Zone.EnvironmentID != creation.Environment.ID ||
		creation.Zone.Desired.OwnerKind != "backing_project" ||
		creation.Zone.Desired.OwnerID != creation.Project.ID {
		return errs.New(errs.KindValidationFailed, "Backing-service Zone ownership is invalid")
	}
	if _, err := (zonePoolRegistry{Reservations: map[string]string{}}).reserve(creation.Environment, creation.Zone); err != nil {
		return err
	}
	if err := validateServiceRecord(creation.Service); err != nil {
		return err
	}
	if creation.Service.EnvironmentID != creation.Environment.ID || creation.Service.Desired.Adapter == "" ||
		creation.Service.BackingNetworkID != creation.Zone.Desired.ID {
		return errs.New(errs.KindValidationFailed, "Backing-service adapter Service is invalid")
	}
	customCreation := creation.Service.Desired.Adapter == "custom"
	if err := validateBackingServiceAdapterCreationShape(creation.Service.Desired, customCreation); err != nil {
		return err
	}
	if len(creation.Entries) != len(creation.EntryValues) || customCreation != (len(creation.Entries) == 0) {
		return errs.New(errs.KindValidationFailed, "Backing-service bootstrap entries are invalid")
	}
	seenEntries := make(map[string]struct{}, len(creation.Entries))
	for index, entry := range creation.Entries {
		if entry.EnvironmentID != creation.Environment.ID {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Entry ownership is invalid")
		}
		if _, exists := seenEntries[entry.Entry.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Entry identity is duplicated")
		}
		seenEntries[entry.Entry.ID] = struct{}{}
		_, value, err := prepareEntryGeneration(entry, creation.EntryValues[index])
		if err != nil {
			return err
		}
		clear(value)
	}
	if len(creation.Secrets) != len(creation.SecretValues) || customCreation != (len(creation.Secrets) == 0) {
		return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Secrets are invalid")
	}
	seenSecrets := make(map[string]struct{}, len(creation.Secrets))
	for index, secret := range creation.Secrets {
		if secret.Secret.ProjectID != creation.Project.ID || secret.Secret.Scope != "project" ||
			secret.Secret.ID != creation.SecretValues[index].SecretID {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Secret ownership is invalid")
		}
		if _, exists := seenSecrets[secret.Secret.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Backing-service bootstrap Secret identity is duplicated")
		}
		seenSecrets[secret.Secret.ID] = struct{}{}
		if err := secretrecord.ValidateRecord(secret); err != nil {
			return err
		}
		if err := secretrecord.ValidateEncryptedValue(creation.SecretValues[index]); err != nil {
			return err
		}
	}
	if err := validateBackingServiceProjection(creation); err != nil {
		return err
	}
	wantOwner, err := EnvironmentTaskOwner(creation.Project, creation.Environment)
	if err != nil {
		return err
	}
	healthServiceID, hasHealthStep := creation.Task.Params[TaskBackingServiceHealthParam]
	desiredHealth := creation.Service.Desired.Healthcheck
	hasDesiredHealth := desiredHealth.HTTP != "" || desiredHealth.TCP != "" || desiredHealth.Pgrep != ""
	afterStartServiceID, hasAfterStartStep := creation.Task.Params[TaskBackingServiceAfterStartParam]
	hasDesiredAfterStart := creation.Service.Desired.Hooks != nil &&
		creation.Service.Desired.Hooks.AfterStart != nil
	if creation.Task.Owner != wantOwner || creation.Task.Actor != TaskActorOperator ||
		creation.Task.Executor != TaskExecutorAgent || creation.Task.Type != TaskUpdate ||
		creation.Task.Target != creation.Environment.ID || creation.Task.Status != TaskStatusPending ||
		creation.Task.RenderGeneration != 1 ||
		creation.Task.Params[EnvironmentDesiredRevisionParam] != creation.Task.ID ||
		creation.Task.Params[TaskMaterializationEnvironmentParam] != creation.Environment.ID ||
		creation.Task.Params[TaskBackingServiceCreationParam] != creation.Service.Desired.ID ||
		hasHealthStep != hasDesiredHealth || hasHealthStep && healthServiceID != creation.Service.Desired.ID ||
		hasAfterStartStep != hasDesiredAfterStart ||
		hasAfterStartStep && afterStartServiceID != creation.Service.Desired.ID ||
		creation.Task.Params[TaskBackingServiceVolumeDirectoryParam] != creation.Environment.VolumeDir {
		return errs.New(errs.KindValidationFailed, "Backing-service creation Task is invalid")
	}
	if creation.Marker.Kind != IdempotencyMarkerTask || creation.Marker.State != IdempotencyMarkerPending ||
		creation.Marker.TaskID != creation.Task.ID ||
		creation.Marker.Locator.ScopeKind != IdempotencyScopePlatform || creation.Marker.Locator.ScopeID != "-" ||
		!creation.Marker.CreatedAt.Equal(creation.Task.CreatedAt) ||
		!creation.Marker.UpdatedAt.Equal(creation.Marker.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service creation marker is invalid")
	}
	if err := validateTaskRecord(creation.Task); err != nil {
		return err
	}
	if err := validateIdempotencyMarker(creation.Marker); err != nil {
		return err
	}
	return nil
}

func backingServiceCreationConditions(
	creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence,
	creationStageKey string,
) []etcdstore.Condition {
	conditions := []etcdstore.Condition{
		{Key: creationStageKey, ModRevision: creation.Stage.Revision},
		{Key: taskKey(creation.Task.ID)},
		{Key: taskOperationIndexKey(creation.Task.OperationID, creation.Task.ID)},
		{Key: taskActiveOperationKey(creation.Task.OperationID)},
		{Key: taskQueueKey(creation.Task.Executor, creation.Task.ID)},
		{
			Key:         environmentBlueprintRootKey(creation.Environment.ID, creation.Task.ID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(creation.Environment.ID)},
		{Key: hierarchyrecord.ProjectKey(creation.Project.ID)},
		{Key: hierarchyrecord.ProjectSlugKey(creation.Project)},
		{Key: hierarchyrecord.ProjectOwnerKey(creation.Project)},
		{Key: HierarchyCoordinationKey(string(HierarchyDeletionTargetProject), creation.Project.ID)},
		{Key: deletionTombstoneKey("project", creation.Project.ID)},
		{Key: hierarchyrecord.EnvironmentKey(creation.Environment.ID)},
		{Key: hierarchyrecord.EnvironmentNameKey(creation.Project.ID, creation.Environment.Name)},
		{Key: hierarchyrecord.EnvironmentOwnerKey(creation.Project.ID, creation.Environment.ID)},
		{Key: deletionTombstoneKey("environment", creation.Environment.ID)},
		{Key: hierarchyrecord.EnvironmentMutationEpochKey(creation.Environment.ID)},
		{Key: HierarchyCoordinationKey(string(HierarchyDeletionTargetEnvironment), creation.Environment.ID)},
		{Key: scriptrecord.ScriptSetActiveKey(creation.Environment.ID)},
		{Key: environmentPoolRegistryKey, ModRevision: creation.PoolRegistry.Revision},
		{Key: deletionTombstoneKey("zone", creation.Zone.Desired.ID)},
		{Key: zonePoolRegistryKey(creation.Environment.ID)},
		{Key: serviceRuntimeKey(creation.Service.Desired.ID)},
		{Key: deletionTombstoneKey("service", creation.Service.Desired.ID)},
	}
	for _, component := range creation.Components {
		conditions = append(conditions,
			etcdstore.Condition{Key: componentKey(component.Desired.ID)},
			etcdstore.Condition{Key: componentEnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID)},
			etcdstore.Condition{Key: componentEnvironmentKindKey(creation.Environment.ID, component.Desired.Kind)},
		)
	}
	for index, entry := range creation.Entries {
		generationKey := secretEntryValueGenerationKey(entry.Entry.ID, entry.CurrentValueGenerationID)
		if creation.EntryValues[index].Plain != nil {
			generationKey = plainEntryValueGenerationKey(entry.Entry.ID, entry.CurrentValueGenerationID)
		}
		conditions = append(conditions,
			etcdstore.Condition{Key: entryrecord.RecordKey(entry.Entry.ID)},
			etcdstore.Condition{Key: entryOwnerKey(creation.Environment.ID, entry.Entry.ID)},
			etcdstore.Condition{Key: generationKey},
		)
	}
	for _, secret := range creation.Secrets {
		conditions = append(conditions,
			etcdstore.Condition{Key: secretrecord.RecordKey(secret.Secret.ID)},
			etcdstore.Condition{Key: secretOwnerKey(secret.Secret)},
			etcdstore.Condition{Key: secretScopedKey(secret.Secret)},
			etcdstore.Condition{Key: secretrecord.ValueKey(secret.Secret.ID)},
			etcdstore.Condition{Key: deletionTombstoneKey("secret", secret.Secret.ID)},
		)
	}
	return conditions
}

func classifyBackingServiceCreation(
	creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence,
	want int,
) idempotencyPlanClassifier {
	const (
		creationStageCondition = iota
		taskCondition
		operationTaskCondition
		activeOperationCondition
		queuedTaskCondition
		blueprintRootCondition
		blueprintDescriptorCondition
		blueprintLocatorCondition
		blueprintHeadCondition
		projectCondition
		projectSlugCondition
		projectOwnerCondition
		projectCoordinationCondition
		projectTombstoneCondition
		environmentCondition
		environmentNameCondition
		environmentOwnerCondition
		environmentTombstoneCondition
		environmentEpochCondition
		environmentCoordinationCondition
		environmentPoolCondition
		zoneTombstoneCondition
		zonePoolCondition
		serviceCondition
		serviceTombstoneCondition
		componentConditionStart
	)
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != want {
			return errs.New(errs.KindInternal, "Backing-service creation compare evidence is incomplete")
		}
		if values[operationTaskCondition] != nil {
			activeTaskID, err := decodeTaskReference(values[operationTaskCondition].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				creation.Task.OperationID,
				activeTaskID,
			)
		}
		for _, index := range []int{creationStageCondition, taskCondition, activeOperationCondition, queuedTaskCondition} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Backing-service creation collided with durable Task state")
			}
		}
		if values[blueprintRootCondition] == nil ||
			values[blueprintRootCondition].ModRevision != publication.rootRevision ||
			values[blueprintDescriptorCondition] == nil ||
			values[blueprintDescriptorCondition].ModRevision != publication.descriptorRevision ||
			values[blueprintLocatorCondition] == nil ||
			values[blueprintLocatorCondition].ModRevision != publication.locatorRevision {
			return errs.New(errs.KindStateConflict, "Backing-service sealed staging evidence changed")
		}
		if values[blueprintHeadCondition] != nil {
			return errs.New(errs.KindStateConflict, "Backing-service desired state already exists")
		}
		if values[projectSlugCondition] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service slug is already in use")
		}
		if values[environmentNameCondition] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service Environment name is already in use")
		}
		poolRegistry := values[environmentPoolCondition]
		if (creation.PoolRegistry.Revision == 0 && poolRegistry != nil) ||
			(creation.PoolRegistry.Revision > 0 &&
				(poolRegistry == nil || poolRegistry.ModRevision != creation.PoolRegistry.Revision)) {
			return stateConflict("environment pool registry", "global")
		}
		for _, index := range []int{
			blueprintHeadCondition, projectCondition, projectOwnerCondition,
			projectCoordinationCondition, environmentCondition, environmentOwnerCondition,
			environmentEpochCondition, environmentCoordinationCondition,
			zonePoolCondition, serviceCondition,
		} {
			if values[index] != nil {
				return errs.New(errs.KindStateConflict, "Backing-service stable identity is already in use")
			}
		}
		for _, index := range []int{projectTombstoneCondition, environmentTombstoneCondition, zoneTombstoneCondition, serviceTombstoneCondition} {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Backing-service deletion is in progress")
			}
		}
		for index := componentConditionStart; index < len(values); index++ {
			if values[index] != nil {
				return errs.New(errs.KindStateConflict, "Backing-service Component identity is already in use")
			}
		}
		return errs.New(errs.KindStateConflict, "Backing-service creation raced")
	}
}
