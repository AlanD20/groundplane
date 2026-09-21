package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (repository *TaskRepository) acknowledgeTask(
	ctx context.Context,
	executor taskjournal.TaskExecutor,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus taskjournal.TaskStatus,
	result *taskjournal.TaskResultRecord,
	terminalAt time.Time,
	environmentID string,
) (etcdstore.Versioned[TaskRecord], error) {
	if err := validateTaskAcknowledgement(
		ctx, executor, agentID, agentGeneration, taskID, assignmentID,
		terminalStatus, result, terminalAt, environmentID,
	); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}

	claimKey := taskjournal.TaskExecutionClaimKey(executor, agentID, taskID)
	submittedResult := taskjournal.CloneTaskResult(result)
	submittedTerminalStatus := terminalStatus
	conflicts := 0
	for {
		terminalStatus = submittedTerminalStatus
		result = taskjournal.CloneTaskResult(submittedResult)
		primaryAndAssignment, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
			taskjournal.TaskStorageKey(taskID), claimKey, taskjournal.TaskAssignmentIndexKey(taskID),
		}})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if len(primaryAndAssignment.Values) != 3 || primaryAndAssignment.Values[0] == nil {
			return etcdstore.Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
		}
		taskValue := primaryAndAssignment.Values[0]
		task, err := DecodeTaskRecord(taskValue.Value)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if task.Executor != executor {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "task execution authority changed")
		}
		if acknowledged, handled, err := repository.acknowledgeSpecializedTask(
			ctx, task, executor, agentID, agentGeneration, taskID, assignmentID,
			terminalStatus, result, terminalAt,
		); handled {
			return acknowledged, err
		}
		assignmentValue := primaryAndAssignment.Values[1]
		assignmentIndexValue := primaryAndAssignment.Values[2]
		environmentCreation := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskCreate &&
			recordcodec.ValidateID(ids.KindEnvironment, task.Target) == nil
		environmentRemoval := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskRemove &&
			recordcodec.ValidateID(ids.KindEnvironment, task.Target) == nil
		zoneRemoval := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskRemove &&
			recordcodec.ValidateID(ids.KindNetwork, task.Target) == nil && task.Params[taskjournal.TaskZoneRemovalOperationParam] != ""
		if environmentCreation != (environmentID != "") || (environmentCreation && task.Target != environmentID) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"environment creation Task requires its atomic provisioning acknowledgement",
			)
		}
		if result != nil {
			if err := taskjournal.ValidateTaskResult(*result, task.Steps, terminalStatus); err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
		}
		if assignmentValue == nil {
			return repository.acknowledgeTerminalTaskReplay(
				ctx, task, taskValue, assignmentIndexValue, primaryAndAssignment.ReadRevision,
				executor, agentID, agentGeneration, assignmentID, terminalStatus, result,
				environmentID, environmentRemoval, zoneRemoval,
			)
		}
		assignment, err := decodeAcknowledgementAssignment(
			task, assignmentValue, assignmentIndexValue, executor, agentID, agentGeneration, assignmentID,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		terminalAt, err = nextTaskControllerTimestamp(task.UpdatedAt, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		timeoutEvidenceConditions, err := repository.prepareReleaseTerminalReport(ctx, TaskAssignment{
			Task: etcdstore.Versioned[TaskRecord]{
				Record:       task,
				Revision:     taskValue.ModRevision,
				ReadRevision: primaryAndAssignment.ReadRevision,
			},
			Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{Record: assignment, Revision: assignmentValue.ModRevision},
		}, terminalStatus, result)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		var recoveryAcknowledgement releaseRecoveryAcknowledgement
		var terminalScriptSourceRelease scriptTerminalSourceRelease
		if executor == taskjournal.TaskExecutorAgent && result != nil && task.Params[releaserender.TaskReleasePublicationParam] != "" &&
			assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			recoveryAcknowledgement, err = repository.releaseRecoveryAcknowledgementAtRevision(
				ctx, task, assignment, terminalStatus, *result, primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			if !recoveryAcknowledgement.final {
				return etcdstore.Versioned[TaskRecord]{
					Record:       task,
					Revision:     taskValue.ModRevision,
					ReadRevision: primaryAndAssignment.ReadRevision,
				}, nil
			}
			terminalStatus = recoveryAcknowledgement.status
			resolved := recoveryAcknowledgement.result
			result = &resolved
		}
		if executor == taskjournal.TaskExecutorAgent && result != nil && taskHasScriptClosingReport(task) &&
			(task.Type == taskjournal.TaskScript || !result.ReconciliationRequired) {
			var processed bool
			terminalScriptSourceRelease, processed, err = repository.prepareTerminalScriptSourceRelease(
				ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue,
				recoveryAcknowledgement, terminalStatus, &terminalAt, primaryAndAssignment.ReadRevision,
				submittedTerminalStatus, *submittedResult, false,
			)
			if err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			if processed {
				continue
			}
			defer terminalScriptSourceRelease.clear()
		}
		if executor == taskjournal.TaskExecutorAgent && result != nil && task.Params[releaserender.TaskReleasePublicationParam] != "" &&
			result.ReconciliationRequired {
			transitioned, processed, transitionErr := repository.transitionReleaseAcknowledgementToRecovery(
				ctx, task, taskValue, assignment, assignmentValue, assignmentIndexValue,
				terminalStatus, *result, primaryAndAssignment.ReadRevision, timeoutEvidenceConditions...,
			)
			if transitionErr != nil {
				return etcdstore.Versioned[TaskRecord]{}, transitionErr
			}
			if processed {
				return transitioned, nil
			}
			continue
		}
		if executor == taskjournal.TaskExecutorAgent && task.Params[releaserender.TaskReleasePublicationParam] != "" &&
			task.Type != taskjournal.TaskUpdate {
			processed, err := repository.finalizeReleaseTaskBatch(
				ctx, task, assignment, terminalStatus, *result, agentID, terminalAt,
				primaryAndAssignment.ReadRevision, recoveryAcknowledgement.conditions...,
			)
			if err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			if processed {
				continue
			}
		}
		if environmentRemoval && terminalStatus == taskjournal.TaskStatusCompleted {
			processed, err := repository.finalizeEnvironmentBlueprintRevisionBatch(ctx, task, terminalAt)
			if err != nil {
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			if processed {
				continue
			}
		}
		terminal, err := TransitionTaskStatus(task, taskjournal.TaskStatusRunning, terminalStatus, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		terminal.Result = taskjournal.CloneTaskResult(result)
		if executor == taskjournal.TaskExecutorAgent {
			terminal.TerminalAssignment = &taskjournal.TaskTerminalAssignmentRecord{
				AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
			}
		}
		if err := ValidateTaskRecord(terminal); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		journal, err := repository.prepareTaskTerminalJournal(
			ctx, task, terminal, terminalStatus, terminalAt, assignment,
			taskValue, assignmentValue, assignmentIndexValue, claimKey,
			primaryAndAssignment.ReadRevision, timeoutEvidenceConditions,
			recoveryAcknowledgement, terminalScriptSourceRelease,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		taskRetentionValue := journal.taskRetentionValue
		conditions, mutations := journal.conditions, journal.mutations
		materializationWriter := journal.materializationWriter
		blueprintCandidateChange := blueprintCandidateTerminalChange{}
		if result != nil {
			blueprintCandidateChange, err = repository.prepareBlueprintCandidateTerminalAcknowledgement(
				ctx, task, materializationWriter, assignment, terminalStatus, *result, agentID, terminalAt,
				primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				journal.clearPrimary()
				clear(taskRetentionValue)
				return etcdstore.Versioned[TaskRecord]{}, err
			}
		}
		defer blueprintCandidateChange.clear()
		materializationProjectionChange, err := repository.prepareTaskMaterializationAcknowledgement(
			ctx,
			terminal,
			assignment,
			primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(taskRetentionValue)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer clearTaskMaterializationProjectionChange(materializationProjectionChange)
		if materializationProjectionChange.applies {
			conditions = append(conditions, materializationProjectionChange.conditions...)
			mutations = append(mutations, materializationProjectionChange.mutations...)
		}
		var environmentValue []byte
		if environmentID != "" {
			environmentConditions, environmentMutations, value, err := repository.prepareEnvironmentCreationAcknowledgement(
				ctx,
				task,
				terminalStatus,
				primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				journal.clearPrimary()
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			environmentValue = value
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		if environmentRemoval {
			environmentConditions, environmentMutations, err := repository.prepareEnvironmentRemovalAcknowledgement(
				ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				journal.clearPrimary()
				clear(environmentValue)
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			conditions = append(conditions, environmentConditions...)
			mutations = append(mutations, environmentMutations...)
		}
		if zoneRemoval {
			zoneConditions, zoneMutations, err := repository.prepareZoneRemovalAcknowledgement(
				ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				journal.clearPrimary()
				clear(environmentValue)
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			defer etcdstore.ClearMutationValues(zoneMutations)
			conditions = append(conditions, zoneConditions...)
			mutations = append(mutations, zoneMutations...)
		}
		attachChange, err := repository.prepareAcknowledgedAttachTask(
			ctx, terminal, assignment, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if attachChange.applies {
			conditions = append(conditions, attachChange.conditions...)
			mutations = append(mutations, attachChange.mutations...)
		}
		blueprintAttachChange, err := repository.prepareBlueprintAttachTaskAcknowledgement(
			ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer clearBlueprintAttachTaskChange(blueprintAttachChange)
		if blueprintAttachChange.applies {
			conditions = append(conditions, blueprintAttachChange.conditions...)
			mutations = append(mutations, blueprintAttachChange.mutations...)
		}
		secretChange, err := repository.prepareSecretTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if secretChange.applies {
			conditions = append(conditions, secretChange.conditions...)
			mutations = append(mutations, secretChange.mutations...)
		}
		scriptChange, err := repository.prepareScriptTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if scriptChange.applies {
			conditions = append(conditions, scriptChange.conditions...)
			mutations = append(mutations, scriptChange.mutations...)
		}
		releaseGroupChange, err := repository.prepareReleaseGroupTaskAcknowledgement(
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
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
			ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if routeChange.applies {
			conditions = append(conditions, routeChange.conditions...)
			mutations = append(mutations, routeChange.mutations...)
		}
		serviceChange, err := repository.prepareAcknowledgedServiceTask(
			ctx, terminal, assignment, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if serviceChange.applies {
			conditions = append(conditions, serviceChange.conditions...)
			mutations = append(mutations, serviceChange.mutations...)
		}
		backingZoneChange, err := repository.prepareBackingZoneTaskAcknowledgement(
			ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if backingZoneChange.applies {
			conditions = append(conditions, backingZoneChange.conditions...)
			mutations = append(mutations, backingZoneChange.mutations...)
		}
		componentChange, err := repository.prepareComponentTaskAcknowledgement(
			ctx,
			task,
			terminalStatus,
			terminalAt,
			primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if componentChange.applies {
			conditions = append(conditions, componentChange.conditions...)
			mutations = append(mutations, componentChange.mutations...)
		}
		platformComponentChange, err := repository.preparePlatformComponentTaskAcknowledgement(
			ctx,
			terminal,
			terminalStatus,
			result,
			primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
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
			ctx, terminal, terminalStatus, primaryAndAssignment.ReadRevision, conditions, platformComponentChange,
		)
		if err != nil {
			journal.clearPrimary()
			clear(taskRetentionValue)
			clear(environmentValue)
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
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
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
			ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			journal.clearPrimary()
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			clearConnectorTaskChange(connectorChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if runnerChange.applies {
			conditions = append(conditions, runnerChange.conditions...)
			mutations = append(mutations, runnerChange.mutations...)
		}
		rotationChange, err := repository.prepareBackupKeyRotationTaskAcknowledgement(
			ctx, task, terminalStatus, terminalAt, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		defer rotationChange.clear()
		conditions = append(conditions, rotationChange.conditions...)
		mutations = append(mutations, rotationChange.mutations...)
		conditions, mutations, err = mergeBlueprintCandidateTerminalChange(
			conditions, mutations, blueprintCandidateChange,
		)
		if err != nil {
			journal.clearPrimary()
			clear(taskRetentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			clearConnectorTaskChange(connectorChange)
			clearRunnerTaskChange(runnerChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		environmentBinding, err := repository.bindOrdinaryTaskEnvironmentMutation(
			ctx,
			terminal,
			primaryAndAssignment.ReadRevision,
			conditions,
			mutations,
			materializationProjectionChange.applies,
			attachChange.applies,
			routeChange.applies && task.Params[taskjournal.TaskEntryEnvironmentParam] != "",
			serviceChange.applies,
			connectorChange.applies,
		)
		if err != nil {
			journal.clearPrimary()
			clear(taskRetentionValue)
			clear(environmentValue)
			clearAttachTaskChange(attachChange)
			clearSecretTaskChange(secretChange)
			clearRouteTaskChange(routeChange)
			clearServiceTaskChange(serviceChange)
			clearBackingZoneTaskChange(backingZoneChange)
			clearComponentTaskChange(componentChange)
			clearPlatformComponentTaskChange(platformComponentChange)
			clearConnectorTaskChange(connectorChange)
			clearRunnerTaskChange(runnerChange)
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		var environmentEpochValue []byte
		if environmentBinding != nil {
			conditions = environmentBinding.conditions
			mutations = environmentBinding.mutations
			environmentEpochValue = mutations[len(mutations)-1].Value
		}
		phase := zoneRemovalTransactionFailedAcknowledgement
		if terminalStatus == taskjournal.TaskStatusCompleted {
			phase = zoneRemovalTransactionCompletedAcknowledgement
		}
		transaction, err := repository.transactTaskTerminal(
			ctx,
			terminal,
			phase,
			conditions,
			mutations,
			terminalScriptSourceRelease.advance,
		)
		journal.clearPrimary()
		clear(taskRetentionValue)
		clear(environmentValue)
		clearAttachTaskChange(attachChange)
		clearSecretTaskChange(secretChange)
		clearRouteTaskChange(routeChange)
		clearServiceTaskChange(serviceChange)
		clearBackingZoneTaskChange(backingZoneChange)
		clearComponentTaskChange(componentChange)
		clearPlatformComponentTaskChange(platformComponentChange)
		clearConnectorTaskChange(connectorChange)
		clearRunnerTaskChange(runnerChange)
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
		return etcdstore.Versioned[TaskRecord]{
			Record: terminal, Revision: transaction.Revision, ReadRevision: transaction.Revision,
		}, nil
	}
}
