package etcd

import (
	"context"
	"encoding/json"
	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// AbortPendingTask wins only while the Task is still queued. If assignment
// wins the CAS first, the caller receives a state conflict and must use the
// Agent abort path for the now-running Task.
func (repository *TaskRepository) AbortPendingTask(
	ctx context.Context,
	taskID string,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if recordcodec.ValidateID(ids.KindTask, taskID) != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "task id is invalid")
	}
	if err := recordcodec.ValidateTimestamp("task terminal_at", terminalAt); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}

	conflicts := 0
	for {
		current, err := repository.GetTask(ctx, taskID)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if current.Record.Type == taskjournal.TaskBackup || current.Record.Type == taskjournal.TaskBackupPrune {
			return repository.abortPendingBackupTask(ctx, taskID, terminalAt)
		}
		environmentCreation := current.Record.Executor == taskjournal.TaskExecutorAgent && current.Record.Type == taskjournal.TaskCreate &&
			recordcodec.ValidateID(ids.KindEnvironment, current.Record.Target) == nil
		environmentRemoval := current.Record.Executor == taskjournal.TaskExecutorAgent && current.Record.Type == taskjournal.TaskRemove &&
			recordcodec.ValidateID(ids.KindEnvironment, current.Record.Target) == nil
		zoneRemoval := current.Record.Executor == taskjournal.TaskExecutorAgent && current.Record.Type == taskjournal.TaskRemove &&
			recordcodec.ValidateID(ids.KindNetwork, current.Record.Target) == nil &&
			current.Record.Params[taskjournal.TaskZoneRemovalOperationParam] != ""
		if current.Record.Status == taskjournal.TaskStatusAborted {
			if err := repository.validatePendingAbortReplay(
				ctx, current, environmentCreation, environmentRemoval, zoneRemoval,
			); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			return repository.finishUnassignedReleaseAbort(ctx, current)
		}
		if current.Record.Status != taskjournal.TaskStatusPending {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"only a pending Task can be aborted before assignment",
			)
		}
		blueprintAbortChange, err := repository.preparePendingScriptAbort(ctx, current, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if blueprintAbortChange.advanced {
			continue
		}
		if blueprintAbortChange.applies {
			terminalAt = blueprintAbortChange.terminalAt
		}
		terminal, err := TransitionTaskStatus(current.Record, taskjournal.TaskStatusPending, taskjournal.TaskStatusAborted, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		terminalAt = *terminal.FinishedAt
		transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
			current.Record,
			taskjournal.TaskStatusAborted,
			terminalAt,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: []string{
				taskjournal.TaskActiveOperationKey(current.Record.OperationID), markerKey,
				taskjournal.TaskQueueKey(current.Record.Executor, taskID), retentionKey,
			},
			Revision: current.ReadRevision,
		})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != 4 || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] == nil || companions.Values[3] != nil {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"pending Task lifecycle records are inconsistent",
			)
		}
		queuedTaskID, queueErr := idempotencyrecord.DecodeTaskReference(companions.Values[2].Value)
		if queueErr != nil || queuedTaskID != taskID {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"pending Task queue record does not match its Task",
			)
		}
		if err := validateTaskLifecycleCompanions(
			current.Record,
			companions.Values[0],
			companions.Values[1],
		); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		transitionedMarker, err = hydrateTerminalTaskMarker(transitionedMarker, companions.Values[1].Value)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		terminalValue, err := EncodeTaskRecord(terminal)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		markerValue, err := idempotencyrecord.EncodeIdempotencyMarker(transitionedMarker)
		clear(transitionedMarker.Intent.Ciphertext)
		clear(transitionedMarker.Response.Body)
		if err != nil {
			clear(terminalValue)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		retentionValue, err := json.Marshal(idempotencyrecord.RetentionReferenceJSON{Schema: 1, MarkerKey: markerKey})
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			return etcdstore.Versioned[TaskRecord]{}, errs.Wrap(errs.KindInternal, err)
		}
		taskRetentionKey, taskRetentionValue, err := prepareTaskRetentionIndex(terminal)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		conditions := []etcdstore.Condition{
			{Key: taskjournal.TaskStorageKey(taskID), ModRevision: current.Revision},
			{Key: taskjournal.TaskActiveOperationKey(current.Record.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskjournal.TaskQueueKey(current.Record.Executor, taskID), ModRevision: companions.Values[2].ModRevision},
			{Key: retentionKey},
			{Key: taskRetentionKey},
		}
		mutations := []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(taskID), Value: terminalValue},
			{Type: etcdstore.MutationDelete, Key: taskjournal.TaskActiveOperationKey(current.Record.OperationID)},
			{Type: etcdstore.MutationDelete, Key: taskjournal.TaskQueueKey(current.Record.Executor, taskID)},
			{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
			{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: etcdstore.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
		}
		var environmentValue []byte
		if environmentCreation {
			environmentConditions, environmentMutations, value, prepareErr :=
				repository.prepareEnvironmentCreationAcknowledgement(
					ctx,
					current.Record,
					taskjournal.TaskStatusAborted,
					current.ReadRevision,
				)
			if prepareErr != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(taskRetentionValue)
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			environmentValue = value
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		if environmentRemoval {
			environmentConditions, environmentMutations, prepareErr :=
				repository.prepareEnvironmentRemovalAcknowledgement(
					ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
				)
			if prepareErr != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(taskRetentionValue)
				clear(environmentValue)
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		var zoneMutations []etcdstore.Mutation
		if zoneRemoval {
			zoneConditions, preparedZoneMutations, prepareErr := repository.prepareZoneRemovalAcknowledgement(
				ctx, current.Record, taskjournal.TaskStatusAborted, terminalAt, current.ReadRevision,
			)
			if prepareErr != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(taskRetentionValue)
				clear(environmentValue)
				return etcdstore.Versioned[TaskRecord]{}, prepareErr
			}
			zoneMutations = preparedZoneMutations
			conditions = append(conditions, zoneConditions...)
			mutations = append(mutations, zoneMutations...)
		}
		attachChange, err := repository.prepareAttachTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if attachChange.applies {
			conditions = append(conditions, attachChange.conditions...)
			mutations = append(mutations, attachChange.mutations...)
		}
		blueprintAttachChange, err := repository.prepareBlueprintAttachTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer clearBlueprintAttachTaskChange(blueprintAttachChange)
		if blueprintAttachChange.applies {
			conditions = append(conditions, blueprintAttachChange.conditions...)
			mutations = append(mutations, blueprintAttachChange.mutations...)
		}
		secretChange, err := repository.prepareSecretTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		scriptChange, err := repository.prepareScriptTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		releaseGroupChange, err := repository.prepareReleaseGroupTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearScriptTaskChange(scriptChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer clearReleaseGroupTaskChange(releaseGroupChange)
		if releaseGroupChange.applies {
			conditions = append(conditions, releaseGroupChange.conditions...)
			mutations = append(mutations, releaseGroupChange.mutations...)
		}
		routeChange, err := repository.prepareRemovalTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		serviceChange, err := repository.prepareServiceTaskAcknowledgement(
			ctx, current.Record, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		backingZoneChange, err := repository.prepareBackingZoneTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		componentChange, err := repository.prepareComponentTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if secretChange.applies {
			conditions = append(conditions, secretChange.conditions...)
			mutations = append(mutations, secretChange.mutations...)
		}
		if scriptChange.applies {
			conditions = append(conditions, scriptChange.conditions...)
			mutations = append(mutations, scriptChange.mutations...)
		}
		if routeChange.applies {
			conditions = append(conditions, routeChange.conditions...)
			mutations = append(mutations, routeChange.mutations...)
		}
		if serviceChange.applies {
			conditions = append(conditions, serviceChange.conditions...)
			mutations = append(mutations, serviceChange.mutations...)
		}
		if backingZoneChange.applies {
			conditions = append(conditions, backingZoneChange.conditions...)
			mutations = append(mutations, backingZoneChange.mutations...)
		}
		if componentChange.applies {
			conditions = append(conditions, componentChange.conditions...)
			mutations = append(mutations, componentChange.mutations...)
		}
		platformComponentChange, err := repository.preparePlatformComponentTaskAcknowledgement(
			ctx, terminal, taskjournal.TaskStatusAborted, nil, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if platformComponentChange.applies {
			conditions = append(conditions, platformComponentChange.conditions...)
			mutations = append(mutations, platformComponentChange.mutations...)
		}
		hostResolutionBaseConditionCount := len(conditions)
		hostResolutionChange, err := repository.prepareHostResolutionReconciliation(
			ctx, terminal, taskjournal.TaskStatusAborted, current.ReadRevision, conditions, platformComponentChange,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer clearHostResolutionReconciliationChange(hostResolutionChange)
		if hostResolutionChange.applies {
			conditions = append(conditions, hostResolutionChange.conditions[hostResolutionBaseConditionCount:]...)
			mutations = append(mutations, hostResolutionChange.mutations...)
		}
		connectorChange, err := repository.prepareConnectorTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if connectorChange.applies {
			conditions = append(conditions, connectorChange.conditions...)
			mutations = append(mutations, connectorChange.mutations...)
		}
		runnerChange, err := repository.prepareRunnerTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, current.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearConnectorTaskChange(connectorChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if runnerChange.applies {
			conditions = append(conditions, runnerChange.conditions...)
			mutations = append(mutations, runnerChange.mutations...)
		}
		rotationChange, err := repository.prepareBackupKeyRotationTaskAcknowledgement(
			ctx, current.Record, taskjournal.TaskStatusAborted, terminalAt, current.ReadRevision,
		)
		if err != nil {
			clearPlatformComponentTaskChange(platformComponentChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer rotationChange.clear()
		conditions = append(conditions, rotationChange.conditions...)
		mutations = append(mutations, rotationChange.mutations...)
		if blueprintAbortChange.applies {
			conditions = append(conditions, blueprintAbortChange.conditions...)
			mutations = append(mutations, blueprintAbortChange.mutations...)
		}
		environmentBinding, err := repository.bindOrdinaryTaskEnvironmentMutation(
			ctx,
			current.Record,
			current.ReadRevision,
			conditions,
			mutations,
			false,
			attachChange.applies,
			routeChange.applies && current.Record.Params[taskjournal.TaskEntryEnvironmentParam] != "",
			serviceChange.applies,
			connectorChange.applies,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
			clear(taskRetentionValue)
			clear(environmentValue)
			etcdstore.ClearMutationValues(zoneMutations)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearConnectorTaskChange(connectorChange)
			clearRunnerTaskChange(runnerChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		var environmentEpochValue []byte
		if environmentBinding != nil {
			conditions = environmentBinding.conditions
			mutations = environmentBinding.mutations
			environmentEpochValue = mutations[len(mutations)-1].Value
		}
		transaction, err := repository.transactZoneRemovalTaskLifecycle(
			ctx, current.Record, zoneRemovalTransactionFailedAcknowledgement, conditions, mutations,
		)
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
		clear(taskRetentionValue)
		clear(environmentValue)
		etcdstore.ClearMutationValues(zoneMutations)
		clearAttachTaskChange(attachChange)
		clearSecretTaskChange(secretChange)
		clearRouteTaskChange(routeChange)
		clearServiceTaskChange(serviceChange)
		clearBackingZoneTaskChange(backingZoneChange)
		clearComponentTaskChange(componentChange)
		clearPlatformComponentTaskChange(platformComponentChange)
		clearConnectorTaskChange(connectorChange)
		clearRunnerTaskChange(runnerChange)
		blueprintAbortChange.clear()
		clear(environmentEpochValue)
		environmentBinding.clear()
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		etcdstore.ClearValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			continue
		}
		return repository.finishUnassignedReleaseAbort(ctx, etcdstore.Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		})
	}
}
