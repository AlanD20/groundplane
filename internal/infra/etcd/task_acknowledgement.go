package etcd

import (
	"bytes"
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

// AcknowledgeTask atomically records the Agent's terminal acknowledgement,
// removes assignment and active-operation state, terminalizes replay evidence,
// and advances any owned Attach lifecycle. Replaying the same terminal
// acknowledgement is idempotent.
func (repository *TaskRepository) AcknowledgeTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx, taskjournal.TaskExecutorAgent, agentID, agentGeneration, taskID, assignmentID,
		terminalStatus, &result, terminalAt, "",
	)
}

// AcknowledgeEnvironmentCreation atomically terminalizes one Agent Task and
// moves its owned Environment from provisioning to ready or failed.
func (repository *TaskRepository) AcknowledgeEnvironmentCreation(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	taskID string,
	assignmentID string,
	environmentID string,
	terminalStatus taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx,
		taskjournal.TaskExecutorAgent,
		agentID,
		agentGeneration,
		taskID,
		assignmentID,
		terminalStatus,
		&result,
		terminalAt,
		environmentID,
	)
}

// AcknowledgeControllerTask terminalizes one Controller claim. Native Tasks
// have no Compose result; their durable event journal carries execution detail.
func (repository *TaskRepository) AcknowledgeControllerTask(
	ctx context.Context,
	taskID string,
	terminalStatus taskjournal.TaskStatus,
	terminalAt time.Time,
) (etcdstore.Versioned[TaskRecord], error) {
	return repository.acknowledgeTask(
		ctx, taskjournal.TaskExecutorController, "", 0, taskID, "", terminalStatus, nil, terminalAt, "",
	)
}

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
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if !taskjournal.ValidExecutor(executor) || recordcodec.ValidateID(ids.KindTask, taskID) != nil ||
		!taskjournal.IsTerminalTaskStatus(terminalStatus) ||
		(executor == taskjournal.TaskExecutorAgent && (recordcodec.ValidateID(ids.KindAgent, agentID) != nil || agentGeneration == 0 ||
			recordcodec.ValidateID(ids.KindAssignment, assignmentID) != nil || result == nil)) ||
		(executor == taskjournal.TaskExecutorController &&
			(agentID != "" || agentGeneration != 0 || assignmentID != "" || result != nil)) {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindValidationFailed, "task acknowledgement is invalid")
	}
	if err := recordcodec.ValidateTimestamp("task terminal_at", terminalAt); err != nil {
		return etcdstore.Versioned[TaskRecord]{}, err
	}
	if environmentID != "" && recordcodec.ValidateID(ids.KindEnvironment, environmentID) != nil {
		return etcdstore.Versioned[TaskRecord]{}, errs.New(
			errs.KindValidationFailed,
			"environment creation acknowledgement is invalid",
		)
	}

	claimKey := taskExecutionClaimKey(executor, agentID, taskID)
	submittedResult := taskjournal.CloneTaskResult(result)
	submittedTerminalStatus := terminalStatus
	conflicts := 0
	for {
		terminalStatus = submittedTerminalStatus
		result = taskjournal.CloneTaskResult(submittedResult)
		primaryAndAssignment, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
			taskKey(taskID), claimKey, taskAssignmentIndexKey(taskID),
		}})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if len(primaryAndAssignment.Values) != 3 || primaryAndAssignment.Values[0] == nil {
			return etcdstore.Versioned[TaskRecord]{}, errs.Newf(errs.KindTaskNotFound, "task not found: %s", taskID)
		}
		taskValue := primaryAndAssignment.Values[0]
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if task.Executor != executor {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "task execution authority changed")
		}
		if task.Params[TaskResourceKindParam] == TaskResourceHierarchyDeletion {
			if executor == taskjournal.TaskExecutorController {
				return repository.acknowledgeHierarchyDeletionControllerTask(
					ctx, taskID, terminalStatus, terminalAt,
				)
			}
			if result == nil {
				return etcdstore.Versioned[TaskRecord]{}, errs.New(
					errs.KindStateConflict,
					"hierarchy deletion Agent Task requires a result",
				)
			}
			return repository.acknowledgeHierarchyDeletionAgentTask(
				ctx, agentID, agentGeneration, taskID, assignmentID,
				terminalStatus, *result, terminalAt,
			)
		}
		if task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskBackupPrune {
			if executor != taskjournal.TaskExecutorAgent || result == nil {
				return etcdstore.Versioned[TaskRecord]{}, errs.New(
					errs.KindStateConflict,
					"backup Task requires an Agent acknowledgement",
				)
			}
			return repository.acknowledgeBackupTask(
				ctx,
				agentID,
				agentGeneration,
				taskID,
				assignmentID,
				terminalStatus,
				*result,
				terminalAt,
			)
		}
		assignmentValue := primaryAndAssignment.Values[1]
		assignmentIndexValue := primaryAndAssignment.Values[2]
		environmentCreation := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskCreate &&
			recordcodec.ValidateID(ids.KindEnvironment, task.Target) == nil
		environmentRemoval := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskRemove &&
			recordcodec.ValidateID(ids.KindEnvironment, task.Target) == nil
		zoneRemoval := executor == taskjournal.TaskExecutorAgent && task.Type == taskjournal.TaskRemove &&
			recordcodec.ValidateID(ids.KindNetwork, task.Target) == nil && task.Params[TaskZoneRemovalOperationParam] != ""
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
			if assignmentIndexValue != nil {
				return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task assignment index is orphaned")
			}
			if executor == taskjournal.TaskExecutorAgent && result != nil && task.Params[TaskReleasePublicationParam] != "" {
				normalizedStatus, normalizedResult, handled, normalizeErr := repository.normalizeReleaseRecoveryTerminalReplay(
					ctx,
					task,
					terminalStatus,
					*result,
					agentID,
					agentGeneration,
					assignmentID,
					primaryAndAssignment.ReadRevision,
				)
				if normalizeErr != nil {
					return etcdstore.Versioned[TaskRecord]{}, normalizeErr
				}
				if handled {
					terminalStatus, result = normalizedStatus, normalizedResult
				}
			}
			if task.Status == terminalStatus &&
				((result == nil && task.Result == nil) ||
					(result != nil && task.Result != nil && taskResultsEqual(*task.Result, *result))) {
				if executor == taskjournal.TaskExecutorAgent {
					expected := taskjournal.TaskTerminalAssignmentRecord{
						AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
					}
					if task.TerminalAssignment == nil || *task.TerminalAssignment != expected {
						return etcdstore.Versioned[TaskRecord]{}, errs.New(
							errs.KindStateConflict,
							"task terminal assignment identity does not match",
						)
					}
				}
				if err := repository.validateTaskAcknowledgementReplay(
					ctx, task, terminalStatus, primaryAndAssignment.ReadRevision,
					environmentID, environmentRemoval, zoneRemoval,
				); err != nil {
					return etcdstore.Versioned[TaskRecord]{}, err
				}
				return etcdstore.Versioned[TaskRecord]{
					Record: task, Revision: taskValue.ModRevision,
					ReadRevision: primaryAndAssignment.ReadRevision,
				}, nil
			}
			return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindStateConflict, "task has no matching active assignment")
		}
		assignment, err := decodeTaskAssignment(assignmentValue.Value)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if assignmentIndexValue == nil || assignmentIndexValue.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(assignmentIndexValue.Value, assignmentValue.Value) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"task assignment index does not match assignment",
			)
		}
		if assignment.TaskID != task.ID || assignment.Executor != executor ||
			(executor == taskjournal.TaskExecutorAgent && assignment.AssignmentID != assignmentID) ||
			assignment.AgentID != agentID ||
			assignment.AgentGeneration != agentGeneration ||
			assignment.ClaimedTaskRevision >= assignmentValue.ModRevision || task.StartedAt == nil ||
			!assignment.AssignedAt.Equal(*task.StartedAt) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindStateConflict,
				"task assignment does not match the Agent generation",
			)
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
			Assignment: etcdstore.Versioned[TaskAssignmentRecord]{Record: assignment, Revision: assignmentValue.ModRevision},
		}, terminalStatus, result)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		var recoveryAcknowledgement releaseRecoveryAcknowledgement
		var terminalScriptSourceRelease scriptTerminalSourceRelease
		if executor == taskjournal.TaskExecutorAgent && result != nil && task.Params[TaskReleasePublicationParam] != "" &&
			assignment.ExecutionMode == TaskExecutionModeRecoveryOnly {
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
		if executor == taskjournal.TaskExecutorAgent && result != nil && task.Params[TaskReleasePublicationParam] != "" &&
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
		if executor == taskjournal.TaskExecutorAgent && task.Params[TaskReleasePublicationParam] != "" &&
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
		terminal, err := transitionTaskStatus(task, taskjournal.TaskStatusRunning, terminalStatus, terminalAt)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		terminal.Result = taskjournal.CloneTaskResult(result)
		if executor == taskjournal.TaskExecutorAgent {
			terminal.TerminalAssignment = &taskjournal.TaskTerminalAssignmentRecord{
				AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
			}
		}
		if err := validateTaskRecord(terminal); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		transitionedMarker, markerKey, retentionKey, err := prepareTerminalTaskMarker(
			terminal,
			terminalStatus,
			terminalAt,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		materializationEnvironmentID, materializes, err := taskEnvironmentWriter(task)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		lifecycleKey, _, _, err := repository.assignmentLifecycleIndexAtRevision(
			ctx, assignment, assignmentValue, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		companionKeys := []string{
			taskActiveOperationKey(task.OperationID), markerKey, taskQueueKey(task.Executor, task.ID), retentionKey,
			lifecycleKey,
		}
		writerKey := ""
		materializationWriter := taskMaterializationWriterRecord{}
		if materializes {
			writerKey = taskMaterializationWriterKey(materializationEnvironmentID)
			companionKeys = append(companionKeys, writerKey)
		}
		companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys:     companionKeys,
			Revision: primaryAndAssignment.ReadRevision,
		})
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if len(companions.Values) != len(companionKeys) || companions.Values[0] == nil || companions.Values[1] == nil ||
			companions.Values[2] != nil || companions.Values[3] != nil || companions.Values[4] == nil ||
			companions.Values[4].ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(companions.Values[4].Value, assignmentValue.Value) {
			return etcdstore.Versioned[TaskRecord]{}, errs.New(
				errs.KindInternal,
				"running Task lifecycle records are inconsistent",
			)
		}
		if err := validateTaskLifecycleCompanions(task, companions.Values[0], companions.Values[1]); err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		if materializes {
			if companions.Values[5] == nil {
				return etcdstore.Versioned[TaskRecord]{}, errs.New(errs.KindInternal, "task materialization writer is missing")
			}
			materializationWriter, err = decodeTaskMaterializationWriter(companions.Values[5].Value)
			if err != nil || validateTaskMaterializationWriterForTask(
				materializationWriter, task, materializationEnvironmentID,
			) != nil {
				return etcdstore.Versioned[TaskRecord]{}, corruptTaskMaterializationWriter()
			}
		}
		transitionedMarker, err = hydrateTerminalTaskMarker(transitionedMarker, companions.Values[1].Value)
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, err
		}
		terminalValue, err := encodeTaskRecord(terminal)
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
			{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: claimKey, ModRevision: assignmentValue.ModRevision},
			{Key: taskAssignmentIndexKey(task.ID), ModRevision: assignmentIndexValue.ModRevision},
			{Key: taskActiveOperationKey(task.OperationID), ModRevision: companions.Values[0].ModRevision},
			{Key: markerKey, ModRevision: companions.Values[1].ModRevision},
			{Key: taskQueueKey(task.Executor, task.ID)},
			{Key: retentionKey},
			{Key: taskRetentionKey},
			{Key: lifecycleKey, ModRevision: companions.Values[4].ModRevision},
		}
		conditions = append(conditions, timeoutEvidenceConditions...)
		mutations := []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: taskKey(task.ID), Value: terminalValue},
			{Type: etcdstore.MutationDelete, Key: claimKey},
			{Type: etcdstore.MutationDelete, Key: taskAssignmentIndexKey(task.ID)},
			{Type: etcdstore.MutationDelete, Key: taskActiveOperationKey(task.OperationID)},
			{Type: etcdstore.MutationPut, Key: markerKey, Value: markerValue},
			{Type: etcdstore.MutationPut, Key: retentionKey, Value: retentionValue},
			{Type: etcdstore.MutationPut, Key: taskRetentionKey, Value: taskRetentionValue},
			{Type: etcdstore.MutationDelete, Key: lifecycleKey},
		}
		if recoveryAcknowledgement.final {
			conditions = append(conditions, recoveryAcknowledgement.conditions...)
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: releaseRecoveryKey(task.ID)})
		}
		conditions = append(conditions, terminalScriptSourceRelease.conditions...)
		mutations = append(mutations, terminalScriptSourceRelease.mutations...)
		if materializes {
			conditions = append(conditions, etcdstore.Condition{Key: writerKey, ModRevision: companions.Values[5].ModRevision})
			mutations = append(mutations, etcdstore.Mutation{Type: etcdstore.MutationDelete, Key: writerKey})
		}
		blueprintCandidateChange := blueprintCandidateTerminalChange{}
		if result != nil {
			blueprintCandidateChange, err = repository.prepareBlueprintCandidateTerminalAcknowledgement(
				ctx, task, materializationWriter, assignment, terminalStatus, *result, agentID, terminalAt,
				primaryAndAssignment.ReadRevision,
			)
			if err != nil {
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
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
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
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
				clear(terminalValue)
				clear(markerValue)
				clear(retentionValue)
				clear(environmentValue)
				return etcdstore.Versioned[TaskRecord]{}, err
			}
			defer clearMutationValues(zoneMutations)
			conditions = append(conditions, zoneConditions...)
			mutations = append(mutations, zoneMutations...)
		}
		attachChange, err := repository.prepareAcknowledgedAttachTask(
			ctx, terminal, assignment, primaryAndAssignment.ReadRevision,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
			routeChange.applies && task.Params[TaskEntryEnvironmentParam] != "",
			serviceChange.applies,
			connectorChange.applies,
		)
		if err != nil {
			clear(terminalValue)
			clear(markerValue)
			clear(retentionValue)
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
		clear(terminalValue)
		clear(markerValue)
		clear(retentionValue)
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
		clearKeyValues(transaction.FailureReads)
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
