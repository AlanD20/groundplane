package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
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
	return repository.BeginServiceLifecycleWithTaskHookInputs(
		ctx, &tenant, project, environment, current, replacement, projection, renderInput, nil, task, marker,
	)
}

func (repository *ServiceRepository) BeginServiceLifecycleWithTaskHookInputs(
	ctx context.Context,
	tenant *Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	current Versioned[ServiceRecord],
	replacement ServiceRecord,
	projection *Versioned[EnvironmentComposeProjection],
	renderInput *ServiceLifecycleRenderInput,
	hookInputs *BackingHookEncryptedInputs,
	task TaskRecord,
	marker IdempotencyMarker,
) (_ IdempotencyTransactionResult, returnErr error) {
	if err := validateServiceLifecycleHierarchy(tenant, project, environment, current); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceLifecycleReplacement(current.Record, replacement, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := validateServiceLifecycleProjection(projection, renderInput, task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if renderInput != nil {
		expectedHooks := serviceLifecycleHooks(current.Record.Desired.Hooks, task.Type)
		if !backinghook.EqualConfiguration(renderInput.HookConfiguration, expectedHooks) ||
			renderInput.HookConfiguration != nil && renderInput.AdapterKey != current.Record.Desired.Adapter {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"Service lifecycle hook configuration changed before publication",
			)
		}
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
	if err := validateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := loadOrdinaryEnvironmentMutationContext(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		environmentKey(environment.Record.ID),
		project.Record.ID,
		project.Record.TenantID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	versionedTenant, versionedProject, versionedEnvironment, err := mutationContext.versionHierarchy(
		tenant,
		project,
		environment,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}

	task = cloneTaskRecord(task)
	if task.IdempotencyKey == "" {
		task.IdempotencyKey = marker.Locator.Key
	}
	task.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	publication, err := prepareBackingHookTaskPublication(ctx, repository.store, task, hookInputs)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer publication.clear()
	defer func() { returnErr = publication.finish(ctx, repository.store, returnErr) }()
	task = publication.task
	if err := validateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	serviceValue, err := encodeServiceRuntimeRecord(newServiceRuntimeRecord(replacement))
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
		serviceDesiredCondition(current),
		serviceRuntimeCondition(current),
		{Key: serviceLifecycleActiveKey(current.Record.Desired.ID)},
		{Key: environmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: projectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionTombstoneKey(string(DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionTombstoneKey(string(DeletionTargetProject), project.Record.ID)},
		{Key: deletionTombstoneKey("service", current.Record.Desired.ID)},
	}
	if tenant != nil {
		conditions = append(conditions[:9], append([]Condition{
			{Key: tenantKey(tenant.Record.ID), ModRevision: tenant.Revision},
		}, conditions[9:]...)...)
		conditions = append(conditions[:12], append([]Condition{
			{Key: deletionTombstoneKey(string(DeletionTargetTenant), tenant.Record.ID)},
		}, conditions[12:]...)...)
	}
	mutations := []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue},
		{Type: MutationPut, Key: taskOperationIndexKey(task.OperationID, task.ID), Value: reference},
		{Type: MutationPut, Key: taskActiveOperationKey(task.OperationID), Value: reference},
		{Type: MutationPut, Key: taskQueueKey(task.Executor, task.ID), Value: reference},
		{Type: MutationPut, Key: serviceRuntimeKey(current.Record.Desired.ID), Value: serviceValue},
		{Type: MutationPut, Key: serviceLifecycleActiveKey(current.Record.Desired.ID), Value: reference},
	}
	if renderInput != nil {
		inputValue, encodeErr := encodeServiceLifecycleRenderInput(*renderInput)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		defer clear(inputValue)
		conditions = append(
			conditions,
			Condition{Key: serviceLifecycleRenderInputKey(task.ID)},
			Condition{
				Key:         serviceLifecycleProjectionFenceKey(renderInput.EnvironmentID),
				ModRevision: renderInput.AppliedProjectionRevision,
			},
			Condition{
				Key:         releaseProjectionKey(renderInput.ServiceID),
				ModRevision: renderInput.Release.ProjectionRevision,
			},
			Condition{
				Key:         releaseIntentStagingKey("", renderInput.Release.ServingReleaseID),
				ModRevision: renderInput.Release.IntentRevision,
			},
			Condition{
				Key:         releaseRenderInputStagingKey("", renderInput.Release.ServingReleaseID),
				ModRevision: renderInput.Release.RenderRevision,
			},
		)
		if renderInput.Release.RetainedPrior != nil {
			conditions = append(conditions, Condition{
				Key:         releaseRenderInputStagingKey("", renderInput.Release.PriorServingReleaseID),
				ModRevision: renderInput.Release.RetainedPriorRenderRevision,
			})
		}
		mutations = append(mutations, Mutation{
			Type: MutationPut, Key: serviceLifecycleRenderInputKey(task.ID), Value: inputValue,
		})
	}
	initiation, err := newEnvironmentTaskInitiation(
		versionedTenant,
		versionedProject,
		versionedEnvironment,
		TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	originalClassify := classifyServiceLifecycleStartConflict(
		tenant, project, environment, current, renderInput, task.OperationID,
	)
	binding, err := mutationContext.bind(ctx, repository.store, conditions, mutations, true)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.clear()
	defer clearMutationValues(binding.mutations)
	if err := validateBoundServiceConditions(binding, current, 4, 5); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := binding.preparedConflict(originalClassify); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := func(revision int64, values []*KeyValue) error {
		return binding.classify(revision, values, originalClassify)
	}
	conditions, mutations, classify, err = publication.bind(binding.conditions, binding.mutations, classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clearMutationValues(mutations)
	if err := validateEnvironmentMutationTransactionBudget(conditions, mutations); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	plan, err := newTaskIdempotencyMutationPlan(
		task,
		initiation,
		conditions,
		mutations,
		classify,
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

func validateBoundServiceConditions(
	binding *ordinaryEnvironmentMutationBinding,
	service Versioned[ServiceRecord],
	desiredIndex int,
	runtimeIndex int,
) error {
	if binding == nil || desiredIndex < 0 || runtimeIndex < 0 ||
		desiredIndex >= len(binding.conditions) || runtimeIndex >= len(binding.conditions) {
		return errs.New(errs.KindInternal, "Service compare binding is incomplete")
	}
	if binding.conditions[desiredIndex] != serviceDesiredCondition(service) {
		return stateConflict("service", service.Record.Desired.ID)
	}
	if binding.conditions[runtimeIndex] != serviceRuntimeCondition(service) {
		return stateConflict("service runtime", service.Record.Desired.ID)
	}
	return nil
}

func serviceLifecycleProjectionFenceKey(environmentID string) string {
	return environmentComposeProjectionKey(environmentID)
}

func validateServiceLifecycleHierarchy(
	tenant *Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	service Versioned[ServiceRecord],
) error {
	if project.Revision <= 0 || environment.Revision <= 0 || service.Revision <= 0 ||
		project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision || service.ReadRevision < service.Revision ||
		validateServiceRecord(service.Record) != nil ||
		environment.Record.ProjectID != project.Record.ID || service.Record.EnvironmentID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle hierarchy is invalid")
	}
	switch project.Record.Kind {
	case ProjectKindTenant:
		if tenant == nil || tenant.Revision <= 0 || tenant.ReadRevision < tenant.Revision ||
			project.Record.TenantID != tenant.Record.ID {
			return errs.New(errs.KindValidationFailed, "Service lifecycle Tenant hierarchy is invalid")
		}
	case ProjectKindBacking:
		if tenant != nil || project.Record.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "Service lifecycle backing hierarchy is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Service lifecycle Project kind is invalid")
	}
	return nil
}

func validateServiceLifecycleReplacement(current ServiceRecord, replacement ServiceRecord, task TaskRecord) error {
	if validateServiceRecord(current) != nil || validateServiceRecord(replacement) != nil ||
		replacement.EnvironmentID != current.EnvironmentID ||
		replacement.BackingNetworkID != current.BackingNetworkID ||
		!sameServiceRemovalDesired(replacement.Desired, current.Desired) ||
		replacement.Runtime.ServiceID != current.Runtime.ServiceID || task.Target != current.Desired.ID ||
		task.Status != TaskStatusPending || task.NextEventSequence != 1 || len(task.Steps) < 1 || len(task.Steps) > 3 {
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
	if projection == nil && input == nil {
		if task.Executor != TaskExecutorController || task.RenderGeneration != 1 ||
			len(task.Params) != 2 || task.Params[TaskResourceKindParam] != TaskResourceService ||
			task.Params[TaskServiceEnvironmentParam] == "" {
			return errs.New(errs.KindValidationFailed, "unapplied Service lifecycle Task is invalid")
		}
		return nil
	}
	if projection == nil || input == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		task.Executor != TaskExecutorAgent || uint64(task.RenderGeneration) != projection.Record.RenderGeneration ||
		input.PlanID != task.PlanID || input.ServiceID != task.Target ||
		input.EnvironmentID != projection.Record.EnvironmentID ||
		!sameRouteRemovalProjection(input.Projection, projection.Record) ||
		len(task.Params) != 2 || task.Params[TaskServiceEnvironmentParam] != input.EnvironmentID ||
		task.Params[TaskComposeArtifactParam] != input.ArtifactID {
		return errs.New(errs.KindValidationFailed, "applied Service lifecycle Task is invalid")
	}
	wantSteps := 1
	if input.Release.RetainedPrior != nil {
		wantSteps = 2
	}
	if serviceLifecycleHookConfigured(*input, task.Type) {
		wantSteps++
	} else if input.HookConfiguration != nil {
		return errs.New(errs.KindValidationFailed, "Service lifecycle hook does not match Task type")
	}
	if len(task.Steps) != wantSteps {
		return errs.New(errs.KindValidationFailed, "applied Service lifecycle Task steps are invalid")
	}
	return validateServiceLifecycleRenderInput(*input)
}

func serviceLifecycleHooks(configuration *backinghook.Configuration, taskType TaskType) *backinghook.Configuration {
	if configuration == nil {
		return nil
	}
	configured := taskType == TaskStart && configuration.AfterStart != nil ||
		(taskType == TaskStop || taskType == TaskDestroy) && configuration.BeforeStop != nil
	if !configured {
		return nil
	}
	return configuration
}

func serviceLifecycleHookConfigured(input ServiceLifecycleRenderInput, taskType TaskType) bool {
	return serviceLifecycleHooks(input.HookConfiguration, taskType) != nil
}

func classifyServiceLifecycleStartConflict(
	tenant *Versioned[TenantRecord],
	project Versioned[ProjectRecord],
	environment Versioned[EnvironmentRecord],
	service Versioned[ServiceRecord],
	input *ServiceLifecycleRenderInput,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*KeyValue) error {
		hierarchyEnd := 9
		deletionStart := hierarchyEnd
		if tenant != nil {
			hierarchyEnd++
			deletionStart++
		}
		deletionEnd := deletionStart + 3
		if tenant != nil {
			deletionEnd++
		}
		expected := deletionEnd
		applied := input != nil
		if applied {
			expected += 5
			if input.Release.RetainedPrior != nil {
				expected++
			}
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
		if !conditionMatchesRead(serviceRuntimeCondition(service), values[5]) {
			return stateConflict("service runtime", service.Record.Desired.ID)
		}
		if values[6] != nil {
			return errs.New(errs.KindResourceInUse, "Service already has an active lifecycle Task")
		}
		revisions := []int64{environment.Revision, project.Revision}
		if tenant != nil {
			revisions = append(revisions, tenant.Revision)
		}
		for index, revision := range revisions {
			value := values[7+index]
			if value == nil || value.ModRevision != revision {
				return errs.New(errs.KindStateConflict, "Service lifecycle hierarchy changed")
			}
		}
		for index := deletionStart; index < deletionEnd; index++ {
			if values[index] != nil {
				return errs.New(errs.KindResourceInUse, "Service hierarchy deletion is in progress")
			}
		}
		if applied {
			if values[deletionEnd] != nil {
				return errs.New(errs.KindInternal, "Service lifecycle render input already exists")
			}
			for index := deletionEnd + 1; index < len(values); index++ {
				if values[index] == nil {
					return errs.New(errs.KindStateConflict, "Service lifecycle immutable runtime authority changed")
				}
			}
		}
		return errs.New(errs.KindStateConflict, "Service lifecycle state changed")
	}
}
