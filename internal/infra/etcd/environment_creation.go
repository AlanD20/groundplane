package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// CreateEnvironmentWithTask atomically publishes the provisioning
// Environment, its indexes, the owning Task, active-operation record, queue
// membership, and Task idempotency marker under Project/Tenant deletion fences.
func (repository *HierarchyRepository) CreateEnvironmentWithTask(
	ctx context.Context,
	volumeRoot string,
	project Versioned[ProjectRecord],
	record EnvironmentRecord,
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

	conditions := []Condition{
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
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: environmentKey(record.ID), Value: environmentValue},
		{Type: MutationPut, Key: environmentNameKey(record.ProjectID, record.Name), Value: []byte(record.ID)},
		{Type: MutationPut, Key: environmentOwnerKey(record.ProjectID, record.ID), Value: []byte(record.ID)},
	}
	plan, err := newTaskIdempotencyMutationPlan(
		conditions,
		mutations,
		classifyEnvironmentCreateConflict(project, task.OperationID),
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

func classifyEnvironmentCreateConflict(
	project Versioned[ProjectRecord],
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		if len(values) != 11 {
			return errs.New(errs.KindInternal, "Environment creation compare evidence is incomplete")
		}
		if values[2] != nil {
			activeTaskID, err := decodeTaskReference(values[2].Value)
			if err != nil {
				return err
			}
			return errs.Newf(errs.KindStateConflict, "operation %s already has active task %s", operationID, activeTaskID)
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
		return stateConflict("environment", "creation")
	}
}
