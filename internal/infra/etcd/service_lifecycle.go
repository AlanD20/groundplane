package etcd

import (
	"context"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	environmentchanges "github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	environmentfence "github.com/AlanD20/groundplane/internal/infra/etcd/environmentfence"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	hierarchyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/hierarchy"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	recordcodec "github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	releases "github.com/AlanD20/groundplane/internal/infra/etcd/releases"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	taskconfiguration "github.com/AlanD20/groundplane/internal/infra/etcd/taskconfiguration"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/backinghook"
	"github.com/AlanD20/groundplane/internal/core"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// BeginServiceLifecycleWithTask atomically changes Controller-owned runtime
// intent and publishes the exact Task that will converge it. Applied Services
// also persist the immutable render snapshot used after restart and on retry.
func (repository *ServiceRepository) BeginServiceLifecycleWithTask(
	ctx context.Context,
	tenant etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	replacement servicerecord.ServiceRecord,
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	renderInput *releaserender.ServiceLifecycleRenderInput,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.BeginServiceLifecycleWithTaskHookInputs(
		ctx, &tenant, project, environment, current, replacement, projection, renderInput, nil, task, marker,
	)
}

func (repository *ServiceRepository) BeginServiceLifecycleWithTaskHookInputs(
	ctx context.Context,
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	current etcdstore.Versioned[servicerecord.ServiceRecord],
	replacement servicerecord.ServiceRecord,
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	renderInput *releaserender.ServiceLifecycleRenderInput,
	hookInputs *taskconfiguration.BackingHookEncryptedInputs,
	task TaskRecord,
	marker idempotencyrecord.IdempotencyMarker,
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
	wantReplayTarget := idempotencyrecord.IdempotencyReplayTarget{
		Kind: idempotencyrecord.IdempotencyReplayTargetService,
		ID:   current.Record.Desired.ID,
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != task.ID || marker.Locator.ScopeKind != idempotencyrecord.IdempotencyScopeEnvironment ||
		marker.Locator.ScopeID != environment.Record.ID || marker.ReplayTarget == nil ||
		*marker.ReplayTarget != wantReplayTarget ||
		!marker.CreatedAt.Equal(task.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"Service lifecycle marker does not match its Task",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(ctx, repository.store, marker); err != nil || found {
		return existing, err
	}
	mutationContext, err := environmentfence.LoadMutationContext(
		ctx,
		repository.store,
		current.Record.EnvironmentID,
		hierarchyrecord.EnvironmentKey(environment.Record.ID),
		project.Record.ID,
		project.Record.TenantID,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	versionedTenant, versionedProject, versionedEnvironment, err := mutationContext.VersionHierarchy(
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
	if err := ValidateTaskRecord(task); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	serviceValue, err := servicerecord.EncodeServiceRuntimeRecord(servicerecord.NewServiceRuntimeRecord(replacement))
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(serviceValue)
	taskValue, err := EncodeTaskRecord(task)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(task.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)

	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID)},
		{Key: taskjournal.TaskOperationIndexKey(task.OperationID, task.ID)},
		{Key: taskjournal.TaskActiveOperationKey(task.OperationID)},
		{Key: taskjournal.TaskQueueKey(task.Executor, task.ID)},
		servicerecord.ServiceDesiredCondition(current),
		servicerecord.ServiceRuntimeCondition(current),
		{Key: servicerecord.ServiceLifecycleActiveKey(current.Record.Desired.ID)},
		{Key: hierarchyrecord.EnvironmentKey(environment.Record.ID), ModRevision: environment.Revision},
		{Key: hierarchyrecord.ProjectKey(project.Record.ID), ModRevision: project.Revision},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetEnvironment), environment.Record.ID)},
		{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetProject), project.Record.ID)},
		{Key: deletionrecord.TombstoneKey("service", current.Record.Desired.ID)},
	}
	if tenant != nil {
		conditions = append(conditions[:9], append([]etcdstore.Condition{
			{Key: hierarchyrecord.TenantKey(tenant.Record.ID), ModRevision: tenant.Revision},
		}, conditions[9:]...)...)
		conditions = append(conditions[:12], append([]etcdstore.Condition{
			{Key: deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetTenant), tenant.Record.ID)},
		}, conditions[12:]...)...)
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(task.OperationID, task.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(task.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(task.Executor, task.ID), Value: reference},
		{
			Type:  etcdstore.MutationPut,
			Key:   servicerecord.ServiceRuntimeKey(current.Record.Desired.ID),
			Value: serviceValue,
		},
		{
			Type:  etcdstore.MutationPut,
			Key:   servicerecord.ServiceLifecycleActiveKey(current.Record.Desired.ID),
			Value: reference,
		},
	}
	if renderInput != nil {
		inputValue, encodeErr := releaserender.EncodeServiceLifecycleRenderInput(*renderInput)
		if encodeErr != nil {
			return IdempotencyTransactionResult{}, encodeErr
		}
		defer clear(inputValue)
		conditions = append(
			conditions,
			etcdstore.Condition{Key: releaserender.ServiceLifecycleRenderInputKey(task.ID)},
			etcdstore.Condition{
				Key:         serviceLifecycleProjectionFenceKey(renderInput.EnvironmentID),
				ModRevision: renderInput.AppliedProjectionRevision,
			},
			etcdstore.Condition{
				Key:         releases.ReleaseProjectionKey(renderInput.ServiceID),
				ModRevision: renderInput.Release.ProjectionRevision,
			},
			etcdstore.Condition{
				Key:         releases.ReleaseIntentStagingKey("", renderInput.Release.ServingReleaseID),
				ModRevision: renderInput.Release.IntentRevision,
			},
			etcdstore.Condition{
				Key:         releases.ReleaseRenderInputStagingKey("", renderInput.Release.ServingReleaseID),
				ModRevision: renderInput.Release.RenderRevision,
			},
		)
		if renderInput.Release.RetainedPrior != nil {
			conditions = append(conditions, etcdstore.Condition{
				Key:         releases.ReleaseRenderInputStagingKey("", renderInput.Release.PriorServingReleaseID),
				ModRevision: renderInput.Release.RetainedPriorRenderRevision,
			})
		}
		mutations = append(mutations, etcdstore.Mutation{
			Type: etcdstore.MutationPut, Key: releaserender.ServiceLifecycleRenderInputKey(task.ID), Value: inputValue,
		})
	}
	initiation, err := newEnvironmentTaskInitiation(
		versionedTenant,
		versionedProject,
		versionedEnvironment,
		taskjournal.TaskActorOperator,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	originalClassify := classifyServiceLifecycleStartConflict(
		tenant, project, environment, current, renderInput, task.OperationID,
	)
	binding, err := mutationContext.Bind(ctx, repository.store, conditions, mutations, true)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer binding.Clear()
	defer etcdstore.ClearMutationValues(binding.Mutations())
	if err := validateBoundServiceConditions(binding, current, 4, 5); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if err := binding.PreparedConflict(originalClassify); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	classify := func(revision int64, values []*etcdstore.KeyValue) error {
		return binding.ClassifyConflict(revision, values, originalClassify)
	}
	conditions, mutations, classify, err = publication.bind(binding.Conditions(), binding.Mutations(), classify)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer etcdstore.ClearMutationValues(mutations)
	if err := environmentfence.ValidateTransactionBudget(conditions, mutations); err != nil {
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
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func validateBoundServiceConditions(
	binding *environmentfence.MutationBinding,
	service etcdstore.Versioned[servicerecord.ServiceRecord],
	desiredIndex int,
	runtimeIndex int,
) error {
	if binding == nil || desiredIndex < 0 || runtimeIndex < 0 ||
		desiredIndex >= len(binding.Conditions()) || runtimeIndex >= len(binding.Conditions()) {
		return errs.New(errs.KindInternal, "Service compare binding is incomplete")
	}
	if binding.Conditions()[desiredIndex] != servicerecord.ServiceDesiredCondition(service) {
		return recordcodec.StateConflict("service", service.Record.Desired.ID)
	}
	if binding.Conditions()[runtimeIndex] != servicerecord.ServiceRuntimeCondition(service) {
		return recordcodec.StateConflict("service runtime", service.Record.Desired.ID)
	}
	return nil
}

func serviceLifecycleProjectionFenceKey(environmentID string) string {
	return projectionrecord.EnvironmentComposeProjectionStorageKey(environmentID)
}

func validateServiceLifecycleHierarchy(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	service etcdstore.Versioned[servicerecord.ServiceRecord],
) error {
	if project.Revision <= 0 || environment.Revision <= 0 || service.Revision <= 0 ||
		project.ReadRevision < project.Revision ||
		environment.ReadRevision < environment.Revision || service.ReadRevision < service.Revision ||
		servicerecord.ValidateServiceRecord(service.Record) != nil ||
		environment.Record.ProjectID != project.Record.ID || service.Record.EnvironmentID != environment.Record.ID {
		return errs.New(errs.KindValidationFailed, "Service lifecycle hierarchy is invalid")
	}
	switch project.Record.Kind {
	case hierarchyrecord.ProjectKindTenant:
		if tenant == nil || tenant.Revision <= 0 || tenant.ReadRevision < tenant.Revision ||
			project.Record.TenantID != tenant.Record.ID {
			return errs.New(errs.KindValidationFailed, "Service lifecycle Tenant hierarchy is invalid")
		}
	case hierarchyrecord.ProjectKindBacking:
		if tenant != nil || project.Record.TenantID != "" {
			return errs.New(errs.KindValidationFailed, "Service lifecycle backing hierarchy is invalid")
		}
	default:
		return errs.New(errs.KindValidationFailed, "Service lifecycle Project kind is invalid")
	}
	return nil
}

func validateServiceLifecycleReplacement(
	current servicerecord.ServiceRecord,
	replacement servicerecord.ServiceRecord,
	task TaskRecord,
) error {
	if servicerecord.ValidateServiceRecord(current) != nil || servicerecord.ValidateServiceRecord(replacement) != nil ||
		replacement.EnvironmentID != current.EnvironmentID ||
		replacement.BackingNetworkID != current.BackingNetworkID ||
		!environmentchanges.SameServiceRemovalDesired(replacement.Desired, current.Desired) ||
		replacement.Runtime.ServiceID != current.Runtime.ServiceID || task.Target != current.Desired.ID ||
		task.Status != taskjournal.TaskStatusPending || task.NextEventSequence != 1 || len(task.Steps) < 1 || len(task.Steps) > 3 {
		return errs.New(errs.KindValidationFailed, "Service lifecycle replacement is invalid")
	}
	want := core.ServiceRuntimeIntent("")
	switch task.Type {
	case taskjournal.TaskStart:
		want = core.ServiceRuntimeIntentRunning
	case taskjournal.TaskStop:
		want = core.ServiceRuntimeIntentStopped
	case taskjournal.TaskDestroy:
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
	projection *etcdstore.Versioned[projectionrecord.EnvironmentComposeProjection],
	input *releaserender.ServiceLifecycleRenderInput,
	task TaskRecord,
) error {
	if projection == nil && input == nil {
		if task.Executor != taskjournal.TaskExecutorController || task.RenderGeneration != 1 ||
			len(
				task.Params,
			) != 2 || task.Params[taskjournal.TaskResourceKindParam] != taskjournal.TaskResourceService ||
			task.Params[taskjournal.TaskServiceEnvironmentParam] == "" {
			return errs.New(errs.KindValidationFailed, "unapplied Service lifecycle Task is invalid")
		}
		return nil
	}
	if projection == nil || input == nil || projection.Revision <= 0 || projection.ReadRevision < projection.Revision ||
		task.Executor != taskjournal.TaskExecutorAgent || uint64(task.RenderGeneration) != projection.Record.RenderGeneration ||
		input.PlanID != task.PlanID || input.ServiceID != task.Target ||
		input.EnvironmentID != projection.Record.EnvironmentID ||
		!environmentchanges.SameRouteRemovalProjection(input.Projection, projection.Record) ||
		len(task.Params) != 2 || task.Params[taskjournal.TaskServiceEnvironmentParam] != input.EnvironmentID ||
		task.Params[taskjournal.TaskComposeArtifactParam] != input.ArtifactID {
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
	return releaserender.ValidateServiceLifecycleRenderInput(*input)
}

func serviceLifecycleHooks(
	configuration *backinghook.Configuration,
	taskType taskjournal.TaskType,
) *backinghook.Configuration {
	if configuration == nil {
		return nil
	}
	configured := taskType == taskjournal.TaskStart && configuration.AfterStart != nil ||
		(taskType == taskjournal.TaskStop || taskType == taskjournal.TaskDestroy) && configuration.BeforeStop != nil
	if !configured {
		return nil
	}
	return configuration
}

func serviceLifecycleHookConfigured(
	input releaserender.ServiceLifecycleRenderInput,
	taskType taskjournal.TaskType,
) bool {
	return serviceLifecycleHooks(input.HookConfiguration, taskType) != nil
}

func classifyServiceLifecycleStartConflict(
	tenant *etcdstore.Versioned[hierarchyrecord.TenantRecord],
	project etcdstore.Versioned[hierarchyrecord.ProjectRecord],
	environment etcdstore.Versioned[hierarchyrecord.EnvironmentRecord],
	service etcdstore.Versioned[servicerecord.ServiceRecord],
	input *releaserender.ServiceLifecycleRenderInput,
	operationID string,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
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
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[2].Value)
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
			return recordcodec.StateConflict("service", service.Record.Desired.ID)
		}
		if !etcdstore.ConditionMatchesRead(servicerecord.ServiceRuntimeCondition(service), values[5]) {
			return recordcodec.StateConflict("service runtime", service.Record.Desired.ID)
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
