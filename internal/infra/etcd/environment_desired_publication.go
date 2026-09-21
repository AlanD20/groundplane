package etcd

import (
	"context"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"net/netip"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	removalrecord "github.com/AlanD20/groundplane/internal/infra/volumeremovalrecord"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// PublishEnvironmentDesiredRevisionWithTask atomically advances the sole
// Environment desired-state pointer and enqueues the Task pinned to that
// already sealed revision. Direct desired mutations preserve the existing
// Environment pool.
func (repository *HierarchyRepository) PublishEnvironmentDesiredRevisionWithTask(
	ctx context.Context,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	releaseGroupPreparation ReleaseGroupBlueprintPreparedMutation,
	componentPreparation ComponentTaskPreparation,
	attachPreparation BlueprintAttachTaskPreparation,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if claim.SourceKind != EnvironmentBlueprintSourceMutation {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"direct Environment desired publication requires mutation source authority",
		)
	}
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, netip.Prefix{}, environment.Record.NetworkPool,
		project, environment, expectedHeadRevision, claim, revision, projection,
		zoneChanges, serviceChanges, routeChanges, releaseGroupPreparation,
		componentPreparation, attachPreparation, BlueprintBackupPolicyPreparation{},
		BlueprintScriptPublication{}, BlueprintReleasePublication{},
		BlueprintRequirementGate{}, VolumeRemovalBackupPolicyPreparation{}, nil, task, marker, nil,
	)
}

// PublishEnvironmentBlueprintDesiredRevision publishes the one authored
// Blueprint Task with its prepared Script and candidate Release fragments.
func (repository *EnvironmentBlueprintRepository) PublishEnvironmentBlueprintDesiredRevision(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	releaseGroupPreparation ReleaseGroupBlueprintPreparedMutation,
	componentPreparation ComponentTaskPreparation,
	attachPreparation BlueprintAttachTaskPreparation,
	backupPreparation BlueprintBackupPolicyPreparation,
	scriptPublication BlueprintScriptPublication,
	releasePublication BlueprintReleasePublication,
	requirementGate BlueprintRequirementGate,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if claim.SourceKind != EnvironmentBlueprintSourceApply {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment Blueprint publication requires apply source authority",
		)
	}
	return repository.publishEnvironmentDesiredRevisionWithTask(
		ctx, environmentPool, desiredNetworkPool,
		project, environment, expectedHeadRevision, claim, revision, projection,
		zoneChanges, serviceChanges, routeChanges, releaseGroupPreparation,
		componentPreparation, attachPreparation, backupPreparation, scriptPublication, releasePublication,
		requirementGate, VolumeRemovalBackupPolicyPreparation{}, nil, task, marker, repository.transactions,
	)
}

func (repository *HierarchyRepository) publishEnvironmentDesiredRevisionWithTask(
	ctx context.Context,
	environmentPool netip.Prefix,
	desiredNetworkPool string,
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	expectedHeadRevision int64,
	claim EnvironmentBlueprintStageClaim,
	revision EnvironmentDesiredRevisionIdentity,
	projection projectionrecord.EnvironmentComposeProjection,
	zoneChanges []EnvironmentBlueprintZoneChange,
	serviceChanges []EnvironmentBlueprintServiceChange,
	routeChanges []EnvironmentBlueprintRouteChange,
	releaseGroupPreparation ReleaseGroupBlueprintPreparedMutation,
	componentPreparation ComponentTaskPreparation,
	attachPreparation BlueprintAttachTaskPreparation,
	backupPreparation BlueprintBackupPolicyPreparation,
	scriptPublication BlueprintScriptPublication,
	releasePublication BlueprintReleasePublication,
	requirementGate BlueprintRequirementGate,
	volumePolicyPreparation VolumeRemovalBackupPolicyPreparation,
	volumeInitial *removalrecord.InitialPublication,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
	blueprintTransactions environmentBlueprintTransactionStore,
) (_ IdempotencyTransactionResult, publicationErr error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := hierarchyrecord.ValidateEnvironment(environment.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.Record.Kind != hierarchyrecord.ProjectKindTenant || project.Revision <= 0 ||
		environment.Revision <= 0 || project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision ||
		environment.Record.ProjectID != project.Record.ID ||
		environment.Record.ProvisioningState != hierarchyrecord.EnvironmentProvisioningReady ||
		expectedHeadRevision < 0 {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindStateConflict, "Environment is not ready for desired-state publication",
		)
	}
	taskEnvironment, ownsDesired, taskEnvironmentErr := desiredRevisionTaskEnvironment(task)
	if recordcodec.ValidateID(ids.KindEnvironment, revision.EnvironmentID) != nil ||
		recordcodec.ValidateID(ids.KindTask, revision.RevisionID) != nil ||
		revision.EnvironmentID != environment.Record.ID ||
		claim.EnvironmentID != revision.EnvironmentID || claim.RevisionID != revision.RevisionID ||
		claim.TaskID != task.ID || projection.EnvironmentID != revision.EnvironmentID ||
		projection.RevisionID != revision.RevisionID ||
		task.Params[EnvironmentDesiredRevisionParam] != revision.RevisionID ||
		taskEnvironmentErr != nil || !ownsDesired || taskEnvironment != revision.EnvironmentID ||
		task.Status != taskjournal.TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Environment desired revision publication identity is invalid",
		)
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed, "Environment desired revision marker does not match its Task",
		)
	}
	if err := validateReleaseGroupBlueprintPreparedMutation(releaseGroupPreparation, environment.Record.ID); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := scriptPublication.validate(environment.Record.ID, claim.SourceKind); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := releasePublication.validate(environment.Record.ID, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	// Apply assembles independently captured Release sources. Direct Entry
	// capture uses the Environment epoch below; other direct mutations retain
	// their sealed desired baseline. None can publish Blueprint Release changes.
	if claim.SourceKind == EnvironmentBlueprintSourceApply {
		if err := releasePublication.validateRetainedRuntime(task, projection); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}

	fence, err := repository.loadEnvironmentBlueprintMutationFence(ctx, project, environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if claim.SourceKind == EnvironmentBlueprintSourceMutation {
		if err := validateEntryRuntimePublication(task, fence); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	poolChange, err := repository.prepareEnvironmentBlueprintPoolChangeAtRevision(
		ctx,
		environmentPool,
		environment,
		desiredNetworkPool,
		fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearPreparedEnvironmentBlueprintPoolChange(poolChange)
	effectiveEnvironment := poolChange.environment
	scriptRemoval, err := repository.prepareDesiredScriptRemoval(
		ctx, environment.Record.ID, expectedHeadRevision, fence.readAtRevision(), projection, task,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	entryPublication, err := repository.prepareDesiredEntryRemovalPublication(
		ctx,
		claim,
		projection,
		task,
		scriptRemoval,
		fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearBackupRuntimeMutations(entryPublication.mutations)
	zonePool, err := repository.prepareEnvironmentBlueprintZonePoolAtRevision(
		ctx, effectiveEnvironment.Record, projection.DesiredZones, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(zonePool.value)
	publishDomain := claim.SourceKind == EnvironmentBlueprintSourceApply
	requirementPublication, err := prepareBlueprintRequirementGatePublication(
		requirementGate,
		task,
		projection,
		publishDomain && len(projection.BlueprintRequirements.Resolved) != 0,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer requirementPublication.clear()
	var componentPublication preparedComponentTaskPublication
	var attachPublication preparedBlueprintAttachTaskPublication
	var backupPublication preparedBlueprintBackupPolicyPublication
	if publishDomain {
		componentPublication, err = repository.prepareComponentTaskPublication(
			ctx, effectiveEnvironment, task, zoneChanges, componentPreparation,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearPreparedComponentTaskPublication(componentPublication)
		attachPublication, err = prepareBlueprintAttachTaskPublication(
			effectiveEnvironment, projection, task, attachPreparation,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearPreparedBlueprintAttachTaskPublication(attachPublication)
		backupPublication, err = prepareBlueprintBackupPolicyPublication(
			task, projection, attachPreparation, backupPreparation,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearPreparedBlueprintBackupPolicyPublication(backupPublication)
	} else if len(zoneChanges) != 0 || len(serviceChanges) != 0 || len(routeChanges) != 0 ||
		!releaseGroupPreparation.isZero() ||
		!componentTaskPreparationIsZero(componentPreparation) ||
		!blueprintAttachTaskPreparationIsZero(attachPreparation) ||
		!backupPreparation.IsZero() ||
		!scriptPublication.IsZero() || !releasePublication.IsZero() {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment desired mutation cannot publish Blueprint domain changes",
		)
	}
	publication, err := repository.prepareEnvironmentBlueprintPublication(
		ctx, claim, revision, projection, task, marker, expectedHeadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(publication.publishedDescriptor)

	// Only Apply or an explicit file writer acknowledges configuration. Sharing
	// desired publication does not grant a metadata/Volume Task file authority.
	if publishDomain || len(task.Materializations) != 0 {
		task, err = prepareRuntimeConfigurationTask(ctx, repository.store, task, fence.readAtRevision())
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	} else if task.Configuration != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed,
			"non-materializing desired mutation cannot acknowledge configuration")
	}
	task, preparedPins, err := prepareRecoverySecretPins(ctx, repository.store, task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if !preparedPins.IsZero() {
		defer func() { publicationErr = finishRecoverySecretPreparation(ctx, repository.store, task, publicationErr) }()
	}
	pinChange, err := recoverySecretPinActivation(preparedPins)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearTaskMaterializationProjectionChange(pinChange)
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	epochMutation, err := fence.epochRewriteMutation()
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochMutation.Value)

	var volumeRuntimePublication volumeRemovalInitialPublication
	if volumeInitial != nil {
		if volumePolicyPreparation.state == nil {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Volume removal policy preparation is required",
			)
		}
		volumeRuntimePublication, err = repository.prepareVolumeRemovalDesiredPublication(
			ctx, *volumeInitial, claim, projection, task, marker, scriptRemoval.volumeID, fence.readAtRevision(),
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearBackupRuntimeMutations(volumeRuntimePublication.mutations)
	}

	var volumePolicyPublication volumeRemovalBackupPolicyPublication
	if volumePolicyPreparation.state != nil {
		if err := volumePolicyPreparation.validateDesiredPublication(claim, projection, task, marker, scriptRemoval.volumeID); err != nil {
			return IdempotencyTransactionResult{}, err
		}
		// ADR0049 defines this as Controller UTC now immediately before the
		// shared publication transaction is constructed, not staging time.
		volumePolicyPublication, err = prepareVolumeRemovalBackupPolicyPublication(
			volumePolicyPreparation,
			time.Now().UTC(),
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clearBackupRuntimeMutations(volumePolicyPublication.mutations)
	}

	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{
			Key:         environmentBlueprintRootKey(revision.EnvironmentID, revision.RevisionID),
			ModRevision: publication.rootRevision,
		},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(revision.EnvironmentID), ModRevision: expectedHeadRevision},
		{Key: zonePoolRegistryKey(revision.EnvironmentID), ModRevision: zonePool.currentRevision},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: etcdstore.MutationDelete, Key: publication.locatorKey},
		{Type: etcdstore.MutationPut, Key: environmentBlueprintHeadKey(revision.EnvironmentID), Value: reference},
		{Type: etcdstore.MutationPut, Key: zonePoolRegistryKey(revision.EnvironmentID), Value: zonePool.value},
	}
	zonePoolConditionIndex := len(conditions) - 1
	poolRegistryConditionIndex := -1
	if poolChange.changed() {
		poolRegistryConditionIndex = len(conditions)
		conditions = append(conditions, etcdstore.Condition{
			Key: environmentPoolRegistryKey, ModRevision: poolChange.registryRevision,
		})
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), Value: poolChange.environmentValue},
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: poolChange.registryValue},
		)
	}
	removalLockConditionIndex := -1
	if volumeInitial == nil {
		removalLockConditionIndex = len(conditions)
		conditions = append(conditions, etcdstore.Condition{Key: removalrecord.EnvironmentLockKey(environment.Record.ID)})
	}
	baseCount := len(conditions)
	baseClassifier := func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != baseCount+len(fence.conditions) {
			return errs.New(errs.KindInternal, "Environment desired publication compare evidence is incomplete")
		}
		if removalLockConditionIndex >= 0 && values[removalLockConditionIndex] != nil {
			return errs.New(errs.KindStateConflict, "Environment Volume removal is in progress")
		}
		if values[2] != nil {
			activeTaskID, decodeErr := idempotencyrecord.DecodeTaskReference(values[2].Value)
			if decodeErr != nil {
				return decodeErr
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				task.OperationID,
				activeTaskID,
			)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Environment desired publication collided with durable Task state")
			}
		}
		for _, index := range []int{4, 5, 6} {
			if values[index] == nil {
				return errs.New(errs.KindStateConflict, "Environment sealed staging evidence changed")
			}
		}
		head := values[7]
		if (expectedHeadRevision == 0 && head != nil) ||
			(expectedHeadRevision > 0 && (head == nil || head.ModRevision != expectedHeadRevision)) {
			return errs.New(errs.KindStateConflict, "Environment desired state changed")
		}
		zoneRegistry := values[zonePoolConditionIndex]
		if (zonePool.currentRevision == 0 && zoneRegistry != nil) ||
			(zonePool.currentRevision > 0 &&
				(zoneRegistry == nil || zoneRegistry.ModRevision != zonePool.currentRevision)) {
			return stateConflict("Zone pool registry", environment.Record.ID)
		}
		if poolRegistryConditionIndex >= 0 {
			registry := values[poolRegistryConditionIndex]
			if registry == nil || registry.ModRevision != poolChange.registryRevision {
				return stateConflict("environment pool registry", environment.Record.ID)
			}
		}
		if conflict := fence.classifyCAS(values[baseCount:]); conflict != nil {
			return conflict
		}
		return nil
	}
	conditions = append(conditions, fence.transactionConditions()...)
	mutations = append(mutations, epochMutation)
	classified := scriptRemoval.classifyConflict(len(conditions), baseClassifier)
	conditions = append(conditions, scriptRemoval.conditions...)
	conditions, classified = bindEntryRuntimePublication(task, conditions, classified)
	conditions, classified, err = bindRuntimeConfigurationPublication(task, conditions, classified)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	requirementBaseConditionCount := len(conditions)
	conditions = append(conditions, requirementPublication.conditions...)
	mutations = append(mutations, requirementPublication.mutations...)
	requirementBaseClassifier := classified
	classified = func(revision int64, values []*etcdstore.KeyValue) error {
		if len(values) != requirementBaseConditionCount+len(requirementPublication.conditions) {
			return errs.New(errs.KindInternal, "Blueprint requirement publication compare evidence is incomplete")
		}
		if err := requirementBaseClassifier(revision, values[:requirementBaseConditionCount]); err != nil {
			return err
		}
		return requirementPublication.classify(values[requirementBaseConditionCount:])
	}
	if publishDomain {
		conditions, classified, err = composeEnvironmentBlueprintComponentPublication(
			conditions,
			classified,
			componentPublication,
		)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		mutations = append(mutations, componentPublication.mutations...)
		conditions = append(conditions, attachPublication.conditions...)
		mutations = append(mutations, attachPublication.mutations...)
		classified = classifyEnvironmentBlueprintAttachPublication(classified, attachPublication)
		conditions = append(conditions, backupPublication.conditions...)
		mutations = append(mutations, backupPublication.mutations...)
		classified = classifyEnvironmentBlueprintBackupPolicyPublication(classified, backupPublication)
		releaseGroupBaseConditionCount := len(conditions)
		conditions = append(conditions, releaseGroupPreparation.conditions...)
		mutations = append(mutations, releaseGroupPreparation.mutations...)
		previousClassifier := classified
		classified = func(revision int64, values []*etcdstore.KeyValue) error {
			if len(values) != releaseGroupBaseConditionCount+len(releaseGroupPreparation.conditions) {
				return errs.New(errs.KindInternal, "Blueprint Release Group compare evidence is incomplete")
			}
			return previousClassifier(revision, values[:releaseGroupBaseConditionCount])
		}
		scriptBaseConditionCount := len(conditions)
		conditions = append(conditions, scriptPublication.conditions...)
		mutations = append(mutations, scriptPublication.mutations...)
		scriptBaseClassifier := classified
		classified = func(revision int64, values []*etcdstore.KeyValue) error {
			if len(values) != scriptBaseConditionCount+len(scriptPublication.conditions) {
				return errs.New(errs.KindInternal, "Blueprint Script compare evidence is incomplete")
			}
			if err := scriptBaseClassifier(revision, values[:scriptBaseConditionCount]); err != nil {
				return err
			}
			return scriptPublication.classify(values[scriptBaseConditionCount:])
		}
		releaseForCompare, err := releasePublication.withExistingComparisons(conditions)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		releaseBaseConditionCount := len(conditions)
		conditions = append(conditions, releaseForCompare.conditions...)
		mutations = append(mutations, releasePublication.mutations...)
		if err := releasePublication.sources.ValidateStagedMutations(claim, mutations); err != nil {
			return IdempotencyTransactionResult{}, err
		}
		releaseBaseClassifier := classified
		classified = func(revision int64, values []*etcdstore.KeyValue) error {
			if len(values) != releaseBaseConditionCount+len(releaseForCompare.conditions) {
				return errs.New(errs.KindInternal, "Blueprint Release compare evidence is incomplete")
			}
			if err := releaseBaseClassifier(revision, values[:releaseBaseConditionCount]); err != nil {
				return err
			}
			return releaseForCompare.classify(values[releaseBaseConditionCount:])
		}
	}
	if volumePolicyPreparation.state != nil {
		volumePolicyPublication, err = volumePolicyPublication.withExistingComparisons(conditions)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		classified = volumePolicyPublication.classifyConflict(len(conditions), classified)
		conditions = append(conditions, volumePolicyPublication.conditions...)
		mutations = append(mutations, volumePolicyPublication.mutations...)
	}
	if volumeInitial != nil {
		classified = volumeRuntimePublication.classifyConflict(len(conditions), classified)
		conditions = append(conditions, volumeRuntimePublication.conditions...)
		mutations = append(mutations, volumeRuntimePublication.mutations...)
	}
	conditions, mutations, classified = entryPublication.bind(conditions, mutations, classified)
	conditions, mutations, classified = bindRecoverySecretPinPublication(pinChange, conditions, mutations, classified)
	classifier := func(revision int64, values []*etcdstore.KeyValue) error {
		if conflict := classified(revision, values); conflict != nil {
			return conflict
		}
		return errs.New(errs.KindStateConflict, "Environment desired publication raced")
	}
	taskTenant, err := loadConnectorTaskInitiationTenantAtRevision(
		ctx, repository.store, project, fence.readAtRevision(),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, effectiveEnvironment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(task, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	// ADR0049 inherits ADR0051's final-publication envelope. Keep that
	// transaction choice separate from permission to publish Blueprint domains;
	// a Volume removal still cannot supply arbitrary Blueprint fragments.
	finalPublication := publishDomain || volumePolicyPreparation.state != nil
	if !finalPublication {
		if err := plan.enforceTransactionBounds(validateEnvironmentDesiredPublicationBudget); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if finalPublication {
		return idempotency.applyEnvironmentBlueprint(ctx, marker, plan, blueprintTransactions)
	}
	return idempotency.Apply(ctx, marker, plan)
}
