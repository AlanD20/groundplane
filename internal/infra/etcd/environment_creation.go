package etcd

import (
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateEnvironmentWithTask atomically publishes the provisioning
// Environment, its indexes, initial Components, the owning Task,
// active-operation record, queue membership, and Task idempotency marker under
// Project/Tenant deletion fences. The same transaction creates the canonical
// Environment mutation epoch consumed by every later persistence mutation.
func (repository *HierarchyRepository) CreateEnvironmentWithTask(
	ctx context.Context,
	volumeRoot string,
	project Versioned[ProjectRecord],
	poolRegistry Versioned[EnvironmentPoolRegistry],
	record EnvironmentRecord,
	components []ComponentRecord,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateProject(project.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if project.Record.Kind != ProjectKindTenant || project.Revision <= 0 ||
		project.ReadRevision < project.Revision {
		return IdempotencyTransactionResult{}, errs.New(errs.KindProjectNotFound, "project was not found")
	}
	if err := validateEnvironment(record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := ValidateEnvironmentVolumeDir(volumeRoot, project.Record, record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateInitialEnvironmentComponents(record.ID, components); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if poolRegistry.Revision < 0 || poolRegistry.ReadRevision < poolRegistry.Revision ||
		poolRegistry.Record.Reservations[record.ID] != record.NetworkPool {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment pool reservation does not match its provisioning record",
		)
	}
	if err := validateEnvironmentPoolRegistry(poolRegistry.Record); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if record.ProvisioningState != EnvironmentProvisioningProvisioning ||
		record.CreateTaskID != task.ID || !record.CreatedAt.Equal(task.CreatedAt) ||
		task.Type != TaskCreate || task.Target != record.ID || task.Status != TaskStatusPending {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment creation Task does not own its provisioning record",
		)
	}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeProject ||
		marker.Locator.ScopeID != project.Record.ID || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Environment creation marker does not match its Project-scoped Task",
		)
	}
	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}

	environmentValue, err := encodeEnvironment(record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(environmentValue)
	poolRegistryValue, err := encodeEnvelope("environment_pool_registry", poolRegistry.Record)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(poolRegistryValue)
	componentValues := make([][]byte, len(components))
	for index := range components {
		componentValues[index], err = encodeComponentRecord(components[index])
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
		defer clear(componentValues[index])
	}
	taskValue, err := encodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := encodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	epochValue, err := encodeEnvironmentMutationEpochRecord(EnvironmentMutationEpochRecord{
		EnvironmentID: record.ID,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(epochValue)
	coordinationValue, err := encodeInitialHierarchyCoordination(HierarchyDeletionTargetEnvironment, record.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(coordinationValue)
	scriptSetValue, err := encodeScriptSetGeneration(ScriptSetGenerationRecord{
		EnvironmentID: record.ID, GenerationID: record.ID,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(scriptSetValue)
	coordinationKey := HierarchyCoordinationKey(string(HierarchyDeletionTargetEnvironment), record.ID)
	scriptSetKey := scriptSetActiveKey(record.ID)

	conditions := []etcdstore.Condition{
		{Key: taskKey(task.ID)},
		{Key: taskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskActiveOperationKey(task.OperationID)},
		{Key: taskQueueKey(task.Executor, task.ID)},
		{Key: environmentKey(record.ID)},
		{Key: environmentNameKey(record.ProjectID, record.Name)},
		{Key: environmentOwnerKey(record.ProjectID, record.ID)},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey("environment", record.ID)},
		{Key: deletionTombstoneKey("project", project.Record.ID)},
		{Key: deletionTombstoneKey("tenant", project.Record.TenantID)},
		{Key: environmentPoolRegistryKey, ModRevision: poolRegistry.Revision},
		{Key: environmentMutationEpochKey(record.ID)},
		{Key: coordinationKey},
		{Key: scriptSetKey},
	}
	for _, component := range components {
		conditions = append(conditions,
			etcdstore.Condition{Key: componentKey(component.Desired.ID)},
			etcdstore.Condition{Key: componentEnvironmentOwnerKey(record.ID, component.Desired.ID)},
			etcdstore.Condition{Key: componentEnvironmentKindKey(record.ID, component.Desired.Kind)},
		)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: etcdstore.MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: etcdstore.MutationPut, Key: environmentKey(record.ID), Value: environmentValue},
		{Type: etcdstore.MutationPut, Key: environmentNameKey(record.ProjectID, record.Name), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: environmentOwnerKey(record.ProjectID, record.ID), Value: []byte(record.ID)},
		{Type: etcdstore.MutationPut, Key: environmentPoolRegistryKey, Value: poolRegistryValue},
		{Type: etcdstore.MutationPut, Key: environmentMutationEpochKey(record.ID), Value: epochValue},
		{Type: etcdstore.MutationPut, Key: coordinationKey, Value: coordinationValue},
		{Type: etcdstore.MutationPut, Key: scriptSetKey, Value: scriptSetValue},
	}
	for index, component := range components {
		mutations = append(mutations,
			etcdstore.Mutation{Type: etcdstore.MutationPut, Key: componentKey(component.Desired.ID), Value: componentValues[index]},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentEnvironmentOwnerKey(record.ID, component.Desired.ID),
				Value: []byte(component.Desired.ID),
			},
			etcdstore.Mutation{
				Type:  etcdstore.MutationPut,
				Key:   componentEnvironmentKindKey(record.ID, component.Desired.Kind),
				Value: []byte(component.Desired.ID),
			},
		)
	}
	if len(components) > 0 {
		mutations = append(mutations, componentWriteFenceMutation(task.ID))
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentCreationTaskInitiation(taskTenant, project, record, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyEnvironmentCreateConflict(project, poolRegistry, components, task.OperationID),
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	idempotency, err := newIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateInitialEnvironmentComponents(environmentID string, components []ComponentRecord) error {
	if len(components) != 2 {
		return errs.New(errs.KindValidationFailed, "Environment creation requires exactly two initial Components")
	}
	wantKinds := map[core.ComponentKind]bool{
		core.ComponentKindIngressCaddy:   false,
		core.ComponentKindEdgeCloudflare: false,
	}
	seenIDs := make(map[string]struct{}, len(components))
	for _, component := range components {
		if err := validateComponentRecord(component); err != nil {
			return err
		}
		if component.Desired.Owner != core.ComponentOwnerEnvironment || component.Desired.OwnerID != environmentID {
			return errs.New(errs.KindValidationFailed, "Initial Component owner must be the new Environment")
		}
		if component.Desired.Enabled || !component.Desired.Config.Empty() {
			return errs.New(errs.KindValidationFailed, "Initial Components must have empty disabled desired state")
		}
		if len(component.Runtime.GeneratedServices) != 0 || component.Runtime.PinnedIPv4 != "" ||
			component.Runtime.Healthy {
			return errs.New(errs.KindValidationFailed, "Initial Components must have empty runtime state")
		}
		if _, exists := seenIDs[component.Desired.ID]; exists {
			return errs.New(errs.KindValidationFailed, "Initial Component IDs must be unique")
		}
		seenIDs[component.Desired.ID] = struct{}{}
		if seen, exists := wantKinds[component.Desired.Kind]; !exists || seen {
			return errs.New(
				errs.KindValidationFailed,
				"Initial Component kinds must be unique Caddy and Cloudflare singletons",
			)
		}
		wantKinds[component.Desired.Kind] = true
	}
	return nil
}

func classifyEnvironmentCreateConflict(
	project Versioned[ProjectRecord],
	poolRegistry Versioned[EnvironmentPoolRegistry],
	components []ComponentRecord,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		if len(values) != 14+(3*len(components)) {
			return errs.New(errs.KindInternal, "Environment creation compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := decodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindStateConflict,
				"operation %s already has active task %s",
				operationID,
				activeTaskID,
			)
		}
		for _, index := range []int{0, 1, 3} {
			if values[index] != nil {
				return errs.New(errs.KindInternal, "Environment creation collided with durable Task state")
			}
		}
		if values[4] != nil || values[6] != nil {
			return errs.New(errs.KindInternal, "Environment creation collided with durable identity state")
		}
		if values[5] != nil {
			return errs.New(errs.KindNameConflict, "Environment name is already in use")
		}
		if values[7] == nil {
			return errs.New(errs.KindProjectNotFound, "project was not found")
		}
		if values[7].ModRevision != project.Revision {
			return stateConflict("project", project.Record.ID)
		}
		if values[8] != nil {
			return errs.New(errs.KindResourceInUse, "Environment deletion is in progress")
		}
		if values[9] != nil {
			return errs.New(errs.KindResourceInUse, "Project deletion is in progress")
		}
		if values[10] != nil {
			return errs.New(errs.KindResourceInUse, "Tenant deletion is in progress")
		}
		if (poolRegistry.Revision == 0 && values[11] != nil) ||
			(poolRegistry.Revision > 0 && (values[11] == nil || values[11].ModRevision != poolRegistry.Revision)) {
			return stateConflict("environment pool registry", "global")
		}
		if values[12] != nil {
			return errs.New(errs.KindInternal, "environment creation collided with mutation epoch state")
		}
		if values[13] != nil {
			return errs.New(errs.KindInternal, "environment creation collided with hierarchy coordination state")
		}
		for index := range components {
			offset := 14 + (index * 3)
			if values[offset] != nil || values[offset+1] != nil {
				return errs.New(errs.KindStateConflict, "Component stable identity is already in use")
			}
			if values[offset+2] != nil {
				return errs.New(errs.KindInternal, "Environment creation collided with Component singleton state")
			}
		}
		return stateConflict("environment", "creation")
	}
}
