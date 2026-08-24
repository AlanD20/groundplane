package etcd

import (
	"context"
	"reflect"

	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

const serviceLifecycleActivePrefix = "/v1/indexes/service-lifecycle/active/"

func serviceLifecycleActiveKey(serviceID string) string {
	return serviceLifecycleActivePrefix + serviceID
}

// BeginServiceLifecycleWithTask atomically changes Controller-owned runtime
// intent and publishes the exact Task that will converge it. Applied Services
// also persist the immutable render snapshot used after restart and on retry.
func (repository *ServiceRepository) BeginServiceLifecycleWithTask(
	ctx context.Context,
	tenant Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
	projection *Versioned[EnvironmentComposeProjection],
	renderInput *ServiceLifecycleRenderInput,
	task TaskRecord,
	marker IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := validateServiceLifecycleHierarchy(tenant, project, environment, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceLifecycleReplacement(current.Record, replacement, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceLifecycleProjection(projection, renderInput, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	wantReplayTarget := IdempotencyReplayTarget{Kind: IdempotencyReplayTargetService, ID: current.Record.Desired.ID}
	if marker.Kind != IdempotencyMarkerTask || marker.State != IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget || !marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle marker does not match its Task",
		)
	}

	indexes, err := repository.store.GetMany(ctx, GetManyRequest{
		Keys: []string{
			serviceNameKey(environment.Record.ID, current.Record.Desired.Name),
			serviceOwnerKey(environment.Record.ID, current.Record.Desired.ID),
		},
		Revision: current.ReadRevision,
	})
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if indexes == nil || len(indexes.Values) != 2 || indexes.Values[0] == nil || indexes.Values[1] == nil ||
		string(indexes.Values[0].Value) != current.Record.Desired.ID ||
		string(indexes.Values[1].Value) != current.Record.Desired.ID {
		return IdempotencyTransactionResult{}, errs.New(errs.KindInternal, "Service lifecycle indexes are corrupt")
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
	serviceValue, err := encodeServiceRecord(replacement)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(serviceValue)
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
		{Key: serviceKey(current.Record.Desired.ID), ModRevision: current.Revision},
		{
			Key:         serviceNameKey(environment.Record.ID, current.Record.Desired.Name),
			ModRevision: indexes.Values[0].ModRevision,
		},
		{
			Key:         serviceOwnerKey(environment.Record.ID, current.Record.Desired.ID),
			ModRevision: indexes.Values[1].ModRevision,
		},
		{Key: serviceLifecycleActiveKey(current.Record.Desired.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: tenantKey(tenant.Record.ID), ModRevision: tenant.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetTenant), tenant.Record.ID)},
		{Key: deletionTombstoneKey("service", current.Record.Desired.ID)},
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: serviceKey(current.Record.Desired.ID), Value: serviceValue},
		{Type: MutationPut, Key: serviceLifecycleActiveKey(current.Record.Desired.ID), Value: reference},
	}
	if projection == nil {
		conditions = append(conditions, Condition{Key: environmentComposeProjectionKey(environment.Record.ID)})
	} else {
		conditions = append(conditions, Condition{
			Key: environmentComposeProjectionKey(environment.Record.ID), ModRevision: projection.Revision,
		})
		if renderInput != nil {
			inputValue, encodeErr := encodeServiceLifecycleRenderInput(*renderInput)
			if encodeErr != nil {
				return IdempotencyTransactionResult{}, encodeErr
			}
			defer clear(inputValue)
			conditions = append(conditions, Condition{Key: serviceLifecycleRenderInputKey(task.ID)})
			mutations = append(mutations, Mutation{
				Type: MutationPut, Key: serviceLifecycleRenderInputKey(task.ID), Value: inputValue,
			})
		}
	}
	taskTenant, err := loadTaskInitiationTenant(ctx, repository.store, project)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newEnvironmentTaskInitiation(taskTenant, project, environment, TaskActorOperator)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classifyServiceLifecycleStartConflict(
			tenant, project, environment, current, projection, renderInput != nil, task.OperationID,
		),
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

func validateServiceLifecycleHierarchy(
	tenant Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	service Versioned[ServiceRecord],
) error {
	if tenant.Revision <= 0 || project.Revision <= 0 || environment.Revision <= 0 || service.Revision <= 0 ||
		tenant.ReadRevision < tenant.Revision || project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision || service.ReadRevision < service.Revision ||
		validateServiceRecord(service.Record) != nil ||
		project.Record.Kind != ProjectKindTenant || project.Record.TenantID != tenant.Record.ID ||
		environment.Record.ProjectID != project.Record.ID || service.Record.EnvironmentID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle hierarchy is invalid")
	}
	return nil
}

func validateServiceLifecycleReplacement(current ServiceRecord, replacement ServiceRecord, task TaskRecord) error {
	if validateServiceRecord(current) != nil || validateServiceRecord(replacement) != nil ||
		replacement.EnvironmentID != current.EnvironmentID ||
		replacement.BackingNetworkID != current.BackingNetworkID ||
		!reflect.DeepEqual(replacement.Desired, current.Desired) ||
		replacement.Runtime.ServiceID != current.Runtime.ServiceID || task.Target != current.Desired.ID ||
		task.Status != TaskStatusPending || task.NextEventSequence != 1 || len(task.Steps) != 1 {
		return errs.New(errs.KindValidationFailed, "Service lifecycle replacement is invalid")
	}
	want := core.ServiceRuntimeIntent("")
	switch task.Type {
	case TaskStart:
		want = core.ServiceRuntimeIntentRunning
	case TaskStop:
		want = core.ServiceRuntimeIntentStopped
	case TaskDestroy:
		want = core.ServiceRuntimeIntentAbsent
	default:
		return errs.New(errs.KindValidationFailed, "Service lifecycle Task type is invalid")
	}
	if replacement.Runtime.RuntimeIntent != want {
		return errs.New(errs.KindValidationFailed, "Service lifecycle runtime intent does not match Task")
	}
	return nil
}

func validateServiceLifecycleProjection(
	projection *Versioned[EnvironmentComposeProjection],
	input *ServiceLifecycleRenderInput,
	task TaskRecord,
) error {
	if input == nil {
		if task.Executor != TaskExecutorController || task.RenderGeneration != 1 ||
			len(task.Params) != 2 || task.Params[TaskResourceKindParam] != TaskResourceService ||
			task.Params[TaskServiceEnvironmentParam] == "" {
			return errs.New(errs.KindValidationFailed, "never-applied Service lifecycle Task is invalid")
		}
		return nil
	}
	if projection == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		task.Executor != TaskExecutorAgent || uint64(task.RenderGeneration) != projection.Record.RenderGeneration ||
		input.PlanID != task.PlanID || input.ServiceID != task.Target ||
		input.EnvironmentID != projection.Record.EnvironmentID ||
		!sameRouteRemovalProjection(input.Projection, projection.Record) ||
		len(task.Params) != 2 || task.Params[TaskServiceEnvironmentParam] != input.EnvironmentID ||
		task.Params[TaskComposeArtifactParam] != input.ArtifactID {
		return errs.New(errs.KindValidationFailed, "applied Service lifecycle Task is invalid")
	}
	return validateServiceLifecycleRenderInput(*input)
}

func classifyServiceLifecycleStartConflict(
	tenant Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	service Versioned[ServiceRecord],
	projection *Versioned[EnvironmentComposeProjection],
	applied bool,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		expected := 16
		if applied {
			expected++
		} else {
			expected++
		}
		if len(values) != expected {
			return errs.New(errs.KindInternal, "Service lifecycle compare evidence is incomplete")
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
				return errs.New(errs.KindInternal, "Service lifecycle collided with durable Task state")
			}
		}
		if values[4] == nil {
			return errs.New(errs.KindServiceNotFound, "Service was not found")
		}
		if values[4].ModRevision != service.Revision {
			return stateConflict("service", service.Record.Desired.ID)
		}
		for _, index := range []int{5, 6} {
			if values[index] == nil || string(values[index].Value) != service.Record.Desired.ID {
				return errs.New(errs.KindInternal, "Service lifecycle index changed or is corrupt")
			}
		}
		if values[7] != nil {
			return errs.New(errs.KindResourceInUse, "Service already has an active lifecycle Task")
		}
		for index, revision := range []int64{environment.Revision, project.Revision, tenant.Revision} {
			value := values[8+index]
			if value == nil || value.ModRevision != revision {
				return errs.New(errs.KindStateConflict, "Service lifecycle hierarchy changed")
			}
		}
		for index := 11; index < 15; index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Service hierarchy deletion is in progress")
			}
		}
		position := 15
		if projection == nil {
			if values[position] != nil {
				return stateConflict("Environment projection", environment.Record.ID)
			}
		} else {
			if values[position] == nil || values[position].ModRevision != projection.Revision {
				return stateConflict("Environment projection", environment.Record.ID)
			}
			if applied && values[position+1] != nil {
				return errs.New(errs.KindInternal, "Service lifecycle render input already exists")
			}
		}
		return errs.New(errs.KindStateConflict, "Service lifecycle state changed")
	}
}
