package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	attachments "github.com/AlanD20/groundplane/internal/infra/etcd/attachments"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// RetryTask atomically clones one retryable terminal attempt and publishes the
// new Task, immutable history index, active-operation index, FIFO queue
// membership, and the retry request's protected idempotency marker. The Task's
// operation idempotency key remains the original operation key; its private
// marker locator identifies this retry request for exact HTTP replay.
func (repository *TaskRepository) RetryTask(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	actor taskjournal.TaskActor,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if actor != taskjournal.TaskActorOperator {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"ordinary task retry actor must be operator",
		)
	}
	return repository.retryTask(ctx, sourceTaskID, retryTaskID, taskjournal.TaskActorOperator, nil, marker)
}

func (repository *TaskRepository) RetryTaskWithInitiation(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	initiation TaskInitiation,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	return repository.retryTask(ctx, sourceTaskID, retryTaskID, initiation.actor, &initiation, marker)
}

func (repository *TaskRepository) retryTask(
	ctx context.Context,
	sourceTaskID string,
	retryTaskID string,
	actor taskjournal.TaskActor,
	provided *TaskInitiation,
	marker idempotencyrecord.IdempotencyMarker,
) (IdempotencyTransactionResult, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if recordcodec.ValidateID(ids.KindTask, sourceTaskID) != nil {
		return IdempotencyTransactionResult{}, errs.New(errs.KindValidationFailed, "source Task id is invalid")
	}
	source, err := repository.GetTask(ctx, sourceTaskID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if source.Record.Type == taskjournal.TaskBackup || source.Record.Type == taskjournal.TaskBackupPrune {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindTaskNotRetryable, "backup retry requires its atomic domain retry protocol",
		)
	}
	if source.Record.Type == taskjournal.TaskRotate {
		return repository.retryBackupKeyRotationTask(ctx, source, retryTaskID, actor, marker)
	}
	if source.Record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceHierarchyDeletion {
		return repository.retryHierarchyDeletionTask(ctx, source, retryTaskID, actor, provided, marker)
	}
	if source.Record.Type == taskjournal.TaskRemove &&
		source.Record.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceVolume {
		return repository.retryVolumeRemovalTask(ctx, source, retryTaskID, actor, marker)
	}
	retry, err := CloneRetryTask(source.Record, retryTaskID, actor, marker.CreatedAt)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	initiation, err := newInheritedTaskInitiation(source, actor)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if provided != nil {
		if err := validateTaskInitiation(
			TaskRecord{},
			*provided,
			false,
		); err != nil ||
			provided.actor != taskjournal.TaskActorSystem {
			return IdempotencyTransactionResult{}, errs.New(
				errs.KindValidationFailed,
				"system task retry initiation is invalid",
			)
		}
		fences := append(append([]etcdstore.Condition(nil), initiation.fences...), provided.fences...)
		initiation, err = newTaskInitiation(source.Record.Owner, taskjournal.TaskActorSystem, fences...)
		if err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	if marker.Kind != idempotencyrecord.IdempotencyMarkerTask || marker.State != idempotencyrecord.IdempotencyMarkerPending ||
		marker.TaskID != retry.ID ||
		!marker.CreatedAt.Equal(retry.CreatedAt) ||
		!marker.UpdatedAt.Equal(marker.CreatedAt) {
		return IdempotencyTransactionResult{}, errs.New(
			errs.KindValidationFailed,
			"task retry marker does not match its Task",
		)
	}
	if err := idempotencyrecord.ValidateIdempotencyMarker(marker); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	retry.idempotencyMarker = cloneIdempotencyLocator(&marker.Locator)
	if err := ValidateTaskRecord(retry); err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if existing, found, err := existingIdempotencyTransaction(
		ctx,
		repository.store,
		marker,
	); err != nil || found {
		return existing, err
	}

	taskValue, err := EncodeTaskRecord(retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(taskValue)
	reference, err := idempotencyrecord.EncodeTaskReference(retry.ID)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	defer clear(reference)
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(sourceTaskID), ModRevision: source.Revision},
		{Key: taskjournal.TaskStorageKey(retry.ID)},
		{Key: taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID)},
		{Key: taskjournal.TaskActiveOperationKey(retry.OperationID)},
		{Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID)},
	}
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(retry.ID), Value: taskValue},
		{
			Type:  etcdstore.MutationPut,
			Key:   taskjournal.TaskOperationIndexKey(retry.OperationID, retry.ID),
			Value: reference,
		},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskActiveOperationKey(retry.OperationID), Value: reference},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskQueueKey(retry.Executor, retry.ID), Value: reference},
	}
	releaseChange, err := repository.prepareReleaseTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if releaseChange.applies {
		conditions = append(conditions, releaseChange.conditions...)
		mutations = append(mutations, releaseChange.mutations...)
	}
	defer releaseChange.clear()
	requirementGateChange, err := repository.prepareBlueprintRequirementGateRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if requirementGateChange.applies {
		conditions = append(conditions, requirementGateChange.conditions...)
		mutations = append(mutations, requirementGateChange.mutations...)
	}
	defer requirementGateChange.clear()
	attachChange, err := repository.prepareAttachTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if attachChange.applies {
		conditions = append(conditions, attachChange.conditions...)
		mutations = append(mutations, attachChange.mutations...)
	}
	defer clearAttachTaskChange(attachChange)
	blueprintAttachChange, err := repository.prepareBlueprintAttachTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if blueprintAttachChange.applies {
		conditions = append(conditions, blueprintAttachChange.conditions...)
		mutations = append(mutations, blueprintAttachChange.mutations...)
	}
	defer clearBlueprintAttachTaskChange(blueprintAttachChange)
	environmentChange, err := repository.prepareEnvironmentTaskRetry(
		ctx,
		source.Record,
		retry,
		source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environmentChange.applies {
		conditions = append(conditions, environmentChange.conditions...)
		mutations = append(mutations, environmentChange.mutations...)
	}
	defer clearEnvironmentTaskChange(environmentChange)
	secretChange, err := repository.prepareSecretTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if secretChange.applies {
		conditions = append(conditions, secretChange.conditions...)
		mutations = append(mutations, secretChange.mutations...)
	}
	defer clearSecretTaskChange(secretChange)
	scriptChange, err := repository.prepareScriptTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if scriptChange.applies {
		conditions = append(conditions, scriptChange.conditions...)
		mutations = append(mutations, scriptChange.mutations...)
	}
	defer clearScriptTaskChange(scriptChange)
	releaseGroupChange, err := repository.prepareReleaseGroupTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if releaseGroupChange.applies {
		conditions = append(conditions, releaseGroupChange.conditions...)
		mutations = append(mutations, releaseGroupChange.mutations...)
	}
	defer clearReleaseGroupTaskChange(releaseGroupChange)
	routeChange, err := repository.prepareRemovalTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if routeChange.applies {
		conditions = append(conditions, routeChange.conditions...)
		mutations = append(mutations, routeChange.mutations...)
	}
	defer clearRouteTaskChange(routeChange)
	serviceChange, err := repository.prepareServiceTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if serviceChange.applies {
		conditions = append(conditions, serviceChange.conditions...)
		mutations = append(mutations, serviceChange.mutations...)
	}
	defer clearServiceTaskChange(serviceChange)
	backingZoneChange, err := repository.prepareBackingZoneTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if backingZoneChange.applies {
		conditions = append(conditions, backingZoneChange.conditions...)
		mutations = append(mutations, backingZoneChange.mutations...)
	}
	defer clearBackingZoneTaskChange(backingZoneChange)
	componentChange, err := repository.prepareComponentTaskRetry(ctx, source.Record, retry, source.ReadRevision)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if componentChange.applies {
		conditions = append(conditions, componentChange.conditions...)
		mutations = append(mutations, componentChange.mutations...)
	}
	defer clearComponentTaskChange(componentChange)
	resolverChange, err := repository.preparePlatformDNSResolverTaskRetry(ctx, source, retry)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if resolverChange.applies {
		conditions = append(conditions, resolverChange.conditions...)
		mutations = append(mutations, resolverChange.mutations...)
	}
	defer clearHostResolutionReconciliationChange(resolverChange)
	connectorChange, err := repository.prepareConnectorTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if connectorChange.applies {
		conditions = append(conditions, connectorChange.conditions...)
		mutations = append(mutations, connectorChange.mutations...)
	}
	defer clearConnectorTaskChange(connectorChange)
	runnerChange, err := repository.prepareRunnerTaskRetry(
		ctx, source.Record, retry, source.ReadRevision,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if runnerChange.applies {
		conditions = append(conditions, runnerChange.conditions...)
		mutations = append(mutations, runnerChange.mutations...)
	}
	defer clearRunnerTaskChange(runnerChange)
	retryClassifier := classifyTaskRetryConflict(
		sourceTaskID,
		retry.OperationID,
		source.Record.Target,
		attachChange.applies && source.Record.Type == taskjournal.TaskDetach,
		len(attachChange.conditions),
		len(environmentChange.conditions),
		len(secretChange.conditions),
		len(scriptChange.conditions),
		len(routeChange.conditions),
		len(serviceChange.conditions),
		len(backingZoneChange.conditions),
		len(componentChange.conditions),
		len(connectorChange.conditions),
		len(runnerChange.conditions),
		len(releaseChange.conditions),
		len(requirementGateChange.conditions),
		len(resolverChange.conditions),
	)
	environmentBinding, err := repository.bindOrdinaryTaskEnvironmentMutation(
		ctx,
		source.Record,
		source.ReadRevision,
		conditions,
		mutations,
		false,
		attachChange.applies,
		routeChange.applies && source.Record.Params[taskjournal.TaskEntryEnvironmentParam] != "",
		serviceChange.applies,
		connectorChange.applies,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if environmentBinding != nil {
		defer environmentBinding.Clear()
		defer clear(environmentBinding.Mutations()[len(environmentBinding.Mutations())-1].Value)
		conditions = environmentBinding.Conditions()
		mutations = environmentBinding.Mutations()
		baseClassifier := retryClassifier
		retryClassifier = func(revision int64, values []*etcdstore.KeyValue) error {
			return environmentBinding.ClassifyConflict(revision, values, baseClassifier)
		}
	}
	plan, err := repository.newRetryTaskIdempotencyMutationPlan(ctx,
		retry,
		initiation,
		conditions,
		mutations,
		retryClassifier,
	)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	if retry.Params[taskjournal.TaskZoneRemovalOperationParam] != "" {
		if err := plan.enforceTransactionBounds(
			zoneRemovalTransactionBudgetValidator(zoneRemovalTransactionRetry),
		); err != nil {
			return IdempotencyTransactionResult{}, err
		}
	}
	idempotency, err := NewIdempotencyRepository(repository.store)
	if err != nil {
		return IdempotencyTransactionResult{}, err
	}
	return idempotency.Apply(ctx, marker, plan)
}

func classifyTaskRetryConflict(
	sourceTaskID string,
	operationID string,
	attachTargetID string,
	attachDetach bool,
	attachConditions int,
	environmentConditions int,
	secretConditions int,
	scriptConditions int,
	routeConditions int,
	serviceConditions int,
	backingZoneConditions int,
	componentConditions int,
	connectorConditions int,
	runnerConditions int,
	releaseConditions int,
	requirementGateConditions int,
	resolverConditions int,
) idempotencyPlanClassifier {
	return func(_ int64, values []*etcdstore.KeyValue) error {
		expectedValues := 5 + attachConditions + environmentConditions + secretConditions +
			scriptConditions + routeConditions + serviceConditions + backingZoneConditions + componentConditions +
			connectorConditions + runnerConditions + releaseConditions + requirementGateConditions + resolverConditions
		if len(values) != expectedValues {
			return errs.New(errs.KindInternal, "task retry compare evidence is incomplete")
		}
		if values[0] == nil {
			return errs.Newf(errs.KindTaskNotFound, "task not found: %s", sourceTaskID)
		}
		if values[3] != nil {
			activeTaskID, err := idempotencyrecord.DecodeTaskReference(values[3].Value)
			if err != nil {
				return err
			}
			return errs.Newf(
				errs.KindTaskRetryInFlight,
				"operation %s already has active retry %s",
				operationID,
				activeTaskID,
			)
		}
		if values[1] != nil || values[2] != nil || values[4] != nil {
			return errs.New(errs.KindInternal, "task retry collided with durable Task state")
		}
		if attachDetach {
			if attachConditions < 2 {
				return errs.New(errs.KindInternal, "attach detach retry exclusion evidence is incomplete")
			}
			if exclusionErr := attachments.ClassifyAttachBackupSourceExclusionEvidence(
				values[6],
				attachTargetID,
			); exclusionErr != nil {
				return exclusionErr
			}
		}
		return errs.New(errs.KindStateConflict, "source Task changed while creating its retry")
	}
}
