package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// BackingServiceCreation is the complete durable input for one backing
// facade. The repository publishes every public member and its first desired
// revision in one transaction so readers can never observe a partial facade.
type BackingServiceCreation struct {
	VolumeRoot   string
	PoolRegistry Versioned[EnvironmentPoolRegistry]
	Project      ProjectRecord
	Environment  EnvironmentRecord
	Components   []ComponentRecord
	Zone         ZoneRecord
	Service      ServiceRecord
	Claim        EnvironmentBlueprintStageClaim
	Revision     EnvironmentDesiredRevisionIdentity
	Projection   EnvironmentComposeProjection
	Task         TaskRecord
	Marker       IdempotencyMarker
}

// PublishBackingServiceWithTask atomically creates one backing facade and
// queues the Agent reconciliation pinned to its immutable desired revision.
// Blueprint audit and projection chunks must already be sealed by the normal
// staging protocol.
func (repository *HierarchyRepository) PublishBackingServiceWithTask(
	ctx context.Context,
	creation BackingServiceCreation,
) (IdempotencyTransactionResult, error) {
	creation.Task = cloneTaskRecord(creation.Task)
	if creation.Task.IdempotencyKey == "" {
		creation.Task.IdempotencyKey = creation.Marker.Locator.Key
	}
	creation.Task.idempotencyMarker = cloneIdempotencyLocator(&creation.Marker.Locator)
	if err := validateBackingServiceCreation(ctx, creation); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, creation.Marker); err != nil || found {
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

	projectValue, err := encodeProject(creation.Project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(projectValue)
	environmentValue, err := encodeEnvironment(creation.Environment)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	poolRegistryValue, err := encodeEnvelope("environment_pool_registry", creation.PoolRegistry.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(poolRegistryValue)
	zoneValue, err := encodeZoneRecord(creation.Zone)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(zoneValue)
	zoneRegistryValue, err := encodeEnvelope("zone_pool_registry", zonePoolRegistry{
		Reservations: map[string]string{creation.Zone.Desired.ID: creation.Zone.Desired.Subnet},
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(zoneRegistryValue)
	serviceValue, err := encodeServiceRecord(creation.Service)
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

	conditions := backingServiceCreationConditions(creation, publication)
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(creation.Task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(creation.Task.OperationID, creation.Task.ID), Value: taskReference},
		{Type: MutationPut, Key: taskActiveOperationKey(creation.Task.OperationID), Value: taskReference},
		{Type: MutationPut, Key: taskQueueKey(creation.Task.Executor, creation.Task.ID), Value: taskReference},
		{Type: MutationPut, Key: publication.descriptorKey, Value: publication.publishedDescriptor},
		{Type: MutationDelete, Key: publication.locatorKey},
		{Type: MutationPut, Key: environmentBlueprintHeadKey(creation.Environment.ID), Value: taskReference},
		{Type: MutationPut, Key: projectKey(creation.Project.ID), Value: projectValue},
		{Type: MutationPut, Key: projectSlugKey(creation.Project), Value: []byte(creation.Project.ID)},
		{Type: MutationPut, Key: projectOwnerKey(creation.Project), Value: []byte(creation.Project.ID)},
		{Type: MutationPut, Key: environmentKey(creation.Environment.ID), Value: environmentValue},
		{Type: MutationPut, Key: environmentNameKey(creation.Project.ID, creation.Environment.Name), Value: []byte(creation.Environment.ID)},
		{Type: MutationPut, Key: environmentOwnerKey(creation.Project.ID, creation.Environment.ID), Value: []byte(creation.Environment.ID)},
		{Type: MutationPut, Key: environmentMutationEpochKey(creation.Environment.ID), Value: epochValue},
		{Type: MutationPut, Key: environmentPoolRegistryKey, Value: poolRegistryValue},
		{Type: MutationPut, Key: zoneKey(creation.Zone.Desired.ID), Value: zoneValue},
		{Type: MutationPut, Key: zoneNameKey(creation.Environment.ID, creation.Zone.Desired.Name), Value: []byte(creation.Zone.Desired.ID)},
		{Type: MutationPut, Key: zoneOwnerKey(creation.Environment.ID, creation.Zone.Desired.ID), Value: []byte(creation.Zone.Desired.ID)},
		{Type: MutationPut, Key: zonePoolRegistryKey(creation.Environment.ID), Value: zoneRegistryValue},
		{Type: MutationPut, Key: serviceKey(creation.Service.Desired.ID), Value: serviceValue},
		{Type: MutationPut, Key: serviceNameKey(creation.Environment.ID, creation.Service.Desired.Name), Value: []byte(creation.Service.Desired.ID)},
		{Type: MutationPut, Key: serviceOwnerKey(creation.Environment.ID, creation.Service.Desired.ID), Value: []byte(creation.Service.Desired.ID)},
	}
	for index, component := range creation.Components {
		mutations = append(mutations,
			Mutation{Type: MutationPut, Key: componentKey(component.Desired.ID), Value: componentValues[index]},
			Mutation{Type: MutationPut, Key: componentEnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID), Value: []byte(component.Desired.ID)},
			Mutation{Type: MutationPut, Key: componentEnvironmentKindKey(creation.Environment.ID, component.Desired.Kind), Value: []byte(component.Desired.ID)},
		)
	}
	classifier := classifyBackingServiceCreation(creation, publication, len(conditions))
	initiation, err := newTaskInitiation(creation.Task.Owner, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(creation.Task, initiation, conditions, mutations, classifier)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateEnvironmentDesiredPublicationBudget(plan, creation.Marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, creation.Marker, plan)
}

func validateBackingServiceCreation(ctx context.Context, creation BackingServiceCreation) error {
	if err := validateContext(ctx); err != nil {
		return err
	}
	if err := validateProject(creation.Project); err != nil {
		return err
	}
	if creation.Project.Kind != ProjectKindBacking || creation.Project.TenantID != "" {
		return errs.New(errs.KindValidationFailed, "Backing-service Project ownership is invalid")
	}
	if err := validateEnvironment(creation.Environment); err != nil {
		return err
	}
	if creation.Environment.ProjectID != creation.Project.ID || creation.Environment.Name != "main" ||
		creation.Environment.ProvisioningState != EnvironmentProvisioningReady ||
		creation.Environment.CreateTaskID != creation.Task.ID ||
		!creation.Environment.CreatedAt.Equal(creation.Task.CreatedAt) {
		return errs.New(errs.KindValidationFailed, "Backing-service Environment lifecycle is invalid")
	}
	if err := ValidateEnvironmentVolumeDir(creation.VolumeRoot, creation.Project, creation.Environment); err != nil {
		return err
	}
	if err := validateInitialEnvironmentComponents(creation.Environment.ID, creation.Components); err != nil {
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
	if err := validateZoneRecord(creation.Zone); err != nil {
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
	if err := validateBackingServiceProjection(creation); err != nil {
		return err
	}
	wantOwner, err := EnvironmentTaskOwner(creation.Project, creation.Environment)
	if err != nil {
		return err
	}
	if creation.Task.Owner != wantOwner || creation.Task.Actor != TaskActorOperator ||
		creation.Task.Executor != TaskExecutorAgent || creation.Task.Type != TaskUpdate ||
		creation.Task.Target != creation.Environment.ID || creation.Task.Status != TaskStatusPending ||
		creation.Task.RenderGeneration != 1 ||
		creation.Task.Params[EnvironmentDesiredRevisionParam] != creation.Task.ID ||
		creation.Task.Params[TaskMaterializationEnvironmentParam] != creation.Environment.ID {
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

func validateBackingServiceProjection(creation BackingServiceCreation) error {
	projection := creation.Projection
	if creation.Revision.EnvironmentID != creation.Environment.ID || creation.Revision.RevisionID != creation.Task.ID ||
		creation.Claim.EnvironmentID != creation.Environment.ID || creation.Claim.RevisionID != creation.Task.ID ||
		creation.Claim.TaskID != creation.Task.ID || creation.Claim.BaselineHeadRevision != 0 ||
		creation.Claim.SourceKind != EnvironmentBlueprintSourceApply || creation.Claim.RenderGeneration != 1 ||
		projection.EnvironmentID != creation.Environment.ID || projection.RevisionID != creation.Task.ID ||
		projection.RenderGeneration != 1 || len(projection.ComposeArtifact) == 0 ||
		len(projection.Services) != 1 || len(projection.Networks) != 1 || len(projection.Volumes) != 1 ||
		len(projection.VolumeMounts) != 1 ||
		projection.Services[0] != (EnvironmentComposeIdentity{ID: creation.Service.Desired.ID, Name: creation.Service.Desired.Name}) ||
		projection.Networks[0] != (EnvironmentComposeIdentity{ID: creation.Zone.Desired.ID, Name: creation.Zone.Desired.Name}) ||
		projection.VolumeMounts[0].ServiceID != creation.Service.Desired.ID ||
		projection.VolumeMounts[0].VolumeID != projection.Volumes[0].ID {
		return errs.New(errs.KindValidationFailed, "Backing-service desired projection is invalid")
	}
	return nil
}

func backingServiceCreationConditions(
	creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence,
) []Condition {
	conditions := []Condition{
		{Key: taskKey(creation.Task.ID)},
		{Key: taskOperationIndexKey(creation.Task.OperationID, creation.Task.ID)},
		{Key: taskActiveOperationKey(creation.Task.OperationID)},
		{Key: taskQueueKey(creation.Task.Executor, creation.Task.ID)},
		{Key: environmentBlueprintRootKey(creation.Environment.ID, creation.Task.ID), ModRevision: publication.rootRevision},
		{Key: publication.descriptorKey, ModRevision: publication.descriptorRevision},
		{Key: publication.locatorKey, ModRevision: publication.locatorRevision},
		{Key: environmentBlueprintHeadKey(creation.Environment.ID)},
		{Key: projectKey(creation.Project.ID)},
		{Key: projectSlugKey(creation.Project)},
		{Key: projectOwnerKey(creation.Project)},
		{Key: deletionTombstoneKey("project", creation.Project.ID)},
		{Key: environmentKey(creation.Environment.ID)},
		{Key: environmentNameKey(creation.Project.ID, creation.Environment.Name)},
		{Key: environmentOwnerKey(creation.Project.ID, creation.Environment.ID)},
		{Key: deletionTombstoneKey("environment", creation.Environment.ID)},
		{Key: environmentMutationEpochKey(creation.Environment.ID)},
		{Key: environmentPoolRegistryKey, ModRevision: creation.PoolRegistry.Revision},
		{Key: zoneKey(creation.Zone.Desired.ID)},
		{Key: zoneNameKey(creation.Environment.ID, creation.Zone.Desired.Name)},
		{Key: zoneOwnerKey(creation.Environment.ID, creation.Zone.Desired.ID)},
		{Key: deletionTombstoneKey("zone", creation.Zone.Desired.ID)},
		{Key: zonePoolRegistryKey(creation.Environment.ID)},
		{Key: serviceKey(creation.Service.Desired.ID)},
		{Key: serviceNameKey(creation.Environment.ID, creation.Service.Desired.Name)},
		{Key: serviceOwnerKey(creation.Environment.ID, creation.Service.Desired.ID)},
		{Key: deletionTombstoneKey("service", creation.Service.Desired.ID)},
	}
	for _, component := range creation.Components {
		conditions = append(conditions,
			Condition{Key: componentKey(component.Desired.ID)},
			Condition{Key: componentEnvironmentOwnerKey(creation.Environment.ID, component.Desired.ID)},
			Condition{Key: componentEnvironmentKindKey(creation.Environment.ID, component.Desired.Kind)},
		)
	}
	return conditions
}

func classifyBackingServiceCreation(
	creation BackingServiceCreation,
	publication environmentBlueprintPublicationEvidence,
	want int,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != want {
			return errs.New(errs.KindInternal, "Backing-service creation compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := decodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(errs.KindStateConflict, "operation %s already has active task %s", creation.Task.OperationID, activeTaskID)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Backing-service creation collided with durable Task state")
			}
		}
		if values[4] == nil || values[4].ModRevision != publication.rootRevision ||
			values[5] == nil || values[5].ModRevision != publication.descriptorRevision ||
			values[6] == nil || values[6].ModRevision != publication.locatorRevision {
			return errs.New(errs.KindStateConflict, "Backing-service sealed staging evidence changed")
		}
		if values[7] != nil {
			return errs.New(errs.KindStateConflict, "Backing-service desired state already exists")
		}
		if values[9] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service slug is already in use")
		}
		if values[13] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service Environment name is already in use")
		}
		poolRegistry := values[17]
		if (creation.PoolRegistry.Revision == 0 && poolRegistry != nil) ||
			(creation.PoolRegistry.Revision > 0 &&
				(poolRegistry == nil || poolRegistry.ModRevision != creation.PoolRegistry.Revision)) {
			return stateConflict("environment pool registry", "global")
		}
		if values[19] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service Zone name is already in use")
		}
		if values[24] != nil {
			return errs.New(errs.KindNameConflict, "Backing-service Service name is already in use")
		}
		for _, index := range []int{8, 10, 12, 14, 16, 18, 20, 22, 23, 25} {
			if values[index] != nil {
				return errs.New(errs.KindStateConflict, "Backing-service stable identity is already in use")
			}
		}
		for _, index := range []int{11, 15, 21, 26} {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Backing-service deletion is in progress")
			}
		}
		for index := 27; index < len(values); index++ {
			if values[index] != nil {
				return errs.New(errs.KindStateConflict, "Backing-service Component identity is already in use")
			}
		}
		return errs.New(errs.KindStateConflict, "Backing-service creation raced")
	}
}
