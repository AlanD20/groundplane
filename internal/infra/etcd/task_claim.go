package etcd

import (
	"context"
	"time"

	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	releaserender "github.com/AlanD20/groundplane/internal/infra/etcd/releaserender"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ClaimNextTask claims the oldest queued Task by ascending ULID. Queue
// membership, pending Task state, active-operation membership, and the absent
// assignment are compared in the same transaction.
func (repository *TaskRepository) ClaimNextTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	assignedAt time.Time,
	maximum int32,
) (TaskAssignment, bool, error) {
	claimCtx, release, err := repository.lockAgentDispatch(ctx)
	if err != nil {
		return TaskAssignment{}, false, err
	}
	defer release()
	if err := repository.ensureAgentDispatchCapacity(claimCtx, agentID, agentGeneration, maximum); err != nil {
		if kind, ok := errs.KindOf(err); ok && kind == errs.KindResourceInUse {
			return TaskAssignment{}, false, nil
		}
		return TaskAssignment{}, false, err
	}
	return repository.claimNextTask(claimCtx, taskjournal.TaskExecutorAgent, agentID, agentGeneration, assignedAt)
}

// ClaimNextControllerTask claims the oldest native Controller Task without
// manufacturing an Agent identity or assignment generation.
func (repository *TaskRepository) ClaimNextControllerTask(
	ctx context.Context,
	assignedAt time.Time,
) (TaskAssignment, bool, error) {
	return repository.claimNextTask(ctx, taskjournal.TaskExecutorController, "", 0, assignedAt)
}

func (repository *TaskRepository) claimNextTask(
	ctx context.Context,
	executor taskjournal.TaskExecutor,
	agentID string,
	agentGeneration uint64,
	assignedAt time.Time,
) (TaskAssignment, bool, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return TaskAssignment{}, false, err
	}
	if executor != taskjournal.TaskExecutorAgent && executor != taskjournal.TaskExecutorController ||
		(executor == taskjournal.TaskExecutorAgent && (recordcodec.ValidateID(ids.KindAgent, agentID) != nil || agentGeneration == 0)) ||
		(executor == taskjournal.TaskExecutorController && (agentID != "" || agentGeneration != 0)) {
		return TaskAssignment{}, false, errs.New(errs.KindValidationFailed, "task execution claim identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("task assignment assigned_at", assignedAt); err != nil {
		return TaskAssignment{}, false, err
	}

	conflicts := 0
	settledDNSRemovals := 0
	for {
		candidate, found, err := repository.nextTaskClaimCandidate(ctx, executor, assignedAt)
		if err != nil || !found {
			return TaskAssignment{}, false, err
		}
		if candidate.advanced {
			settledDNSRemovals++
			if settledDNSRemovals == 24 {
				return TaskAssignment{}, false, nil
			}
			continue
		}
		queued := candidate.queued
		taskValue := candidate.taskValue
		task := candidate.task
		claimAt, err := nextTaskControllerTimestamp(task.UpdatedAt, assignedAt)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		activeKey := taskjournal.TaskActiveOperationKey(task.OperationID)
		assignmentKey := taskjournal.TaskExecutionClaimKey(executor, agentID, task.ID)
		assignmentIndexKey := taskjournal.TaskAssignmentIndexKey(task.ID)
		deadline := TaskForwardDeadline(task, claimAt)
		if task.Type == taskjournal.TaskBackup || task.Type == taskjournal.TaskBackupPrune ||
			task.Type == taskjournal.TaskRestore {
			return TaskAssignment{}, false, errs.New(errs.KindStateConflict, "Backup and Restore require native work-item assignment")
		}
		recoveryDeadline := deadline.Add(time.Duration(task.TimeoutSeconds) * time.Second)
		timeoutIndexKey := taskjournal.TaskTimeoutIndexKey(task.ID, deadline)
		companionKeys := []string{activeKey, assignmentKey, assignmentIndexKey, timeoutIndexKey}
		if candidate.writerKey != "" {
			companionKeys = append(companionKeys, candidate.writerKey)
		}
		companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
			Keys: companionKeys, Revision: candidate.readRevision,
		})
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if len(companions.Values) != len(companionKeys) || companions.Values[0] == nil || companions.Values[1] != nil ||
			companions.Values[2] != nil || companions.Values[3] != nil {
			return TaskAssignment{}, false, errs.New(
				errs.KindInternal,
				"queued Task lifecycle records are inconsistent",
			)
		}
		activeTaskID, err := idempotencyrecord.DecodeTaskReference(companions.Values[0].Value)
		if err != nil || activeTaskID != task.ID {
			return TaskAssignment{}, false, errs.New(
				errs.KindInternal,
				"active-operation record does not match queued Task",
			)
		}
		if candidate.writerKey != "" && companions.Values[4] != nil {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskAssignment{}, false, err
			}
			continue
		}
		childConditions, childMutations, childReady, err := repository.prepareBlueprintChildClaim(ctx, task)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		if !childReady {
			return TaskAssignment{}, false, nil
		}
		running, err := TransitionTaskStatus(
			task,
			taskjournal.TaskStatusPending,
			taskjournal.TaskStatusRunning,
			claimAt,
		)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignment := taskassignments.TaskAssignmentRecord{
			AssignmentID: ids.New(ids.KindAssignment),
			TaskID:       task.ID, Executor: executor, AgentID: agentID, AgentGeneration: agentGeneration,
			ClaimedTaskRevision: taskValue.ModRevision, AssignedAt: claimAt, Deadline: deadline,
			RecoveryDeadline: recoveryDeadline, ExecutionMode: taskassignments.TaskExecutionModeForward, ExecutionEpoch: 1,
		}
		runningValue, err := EncodeTaskRecord(running)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignmentValue, err := taskassignments.EncodeTaskAssignment(assignment)
		if err != nil {
			clear(runningValue)
			return TaskAssignment{}, false, err
		}
		conditions := []etcdstore.Condition{
			{Key: queued.Key, ModRevision: queued.ModRevision},
			{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
			{Key: activeKey, ModRevision: companions.Values[0].ModRevision},
			{Key: assignmentKey},
			{Key: assignmentIndexKey},
			{Key: timeoutIndexKey},
		}
		conditions = append(conditions, candidate.dnsConditions...)
		mutations := []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: runningValue},
			{Type: etcdstore.MutationDelete, Key: queued.Key},
			{Type: etcdstore.MutationPut, Key: assignmentKey, Value: assignmentValue},
			{Type: etcdstore.MutationPut, Key: assignmentIndexKey, Value: assignmentValue},
			{Type: etcdstore.MutationPut, Key: timeoutIndexKey, Value: assignmentValue},
		}
		conditions = append(conditions, childConditions...)
		mutations = append(mutations, childMutations...)
		hookInputConditions, err := repository.backingHookInputClaimConditions(ctx, task, candidate.readRevision)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		conditions = append(conditions, hookInputConditions...)
		scriptSource, scriptSourceReady, err :=
			repository.prepareScriptTaskClaimSourceAuthority(
				ctx, task, taskValue.ModRevision, candidate.readRevision,
			)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		if !scriptSourceReady {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, nil
		}
		defer scriptSource.Clear()
		conditions = append(conditions, scriptSource.conditions...)
		mutations = append(mutations, scriptSource.mutations...)
		requirementEvidence, requirementApplies, requirementReady, err :=
			repository.observeBlueprintRequirementGateForClaim(ctx, task, candidate.readRevision)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		if requirementApplies && !requirementReady {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, nil
		}
		var writerValue []byte
		if candidate.writerKey != "" {
			writer, writerConditions, writerErr := repository.prepareTaskMaterializationWriter(
				ctx, task, candidate.environmentID, candidate.readRevision,
			)
			if writerErr != nil {
				clear(runningValue)
				clear(assignmentValue)
				return TaskAssignment{}, false, writerErr
			}
			writerValue, err = encodeTaskMaterializationWriter(writer)
			if err != nil {
				clear(runningValue)
				clear(assignmentValue)
				return TaskAssignment{}, false, err
			}
			conditions = append(conditions, writerConditions...)
			conditions = append(conditions, etcdstore.Condition{Key: candidate.writerKey})
			mutations = append(mutations, etcdstore.Mutation{
				Type: etcdstore.MutationPut, Key: candidate.writerKey, Value: writerValue,
			})
			if taskHasBlueprintCandidateAppliedAuthority(task) {
				authority, authorityDigest, authorityConditions, authorityErr := repository.prepareBlueprintRestorationAuthority(
					ctx,
					task,
					writer,
					candidate.readRevision,
				)
				if authorityErr != nil {
					etcdstore.ClearMutationValues(mutations)
					return TaskAssignment{}, false, authorityErr
				}
				assignment.RestorationAuthority, assignment.RestorationAuthoritySHA256 = &authority, authorityDigest
				updatedAssignmentValue, encodeErr := taskassignments.EncodeTaskAssignment(assignment)
				if encodeErr != nil {
					etcdstore.ClearMutationValues(mutations)
					return TaskAssignment{}, false, encodeErr
				}
				clear(assignmentValue)
				assignmentValue = updatedAssignmentValue
				mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
				conditions = append(conditions, authorityConditions...)
				conditions = append(conditions, etcdstore.Condition{Key: taskassignments.ReleaseRecoveryKey(task.ID)})
				epochCondition, epochMutation, claimErr :=
					repository.prepareBlueprintCandidateClaimEpoch(
						ctx, task, writer, candidate.readRevision, requirementEvidence.gateRevision,
					)
				if claimErr != nil {
					etcdstore.ClearMutationValues(mutations)
					return TaskAssignment{}, false, claimErr
				}
				conditions = append(conditions, epochCondition)
				mutations = append(mutations, epochMutation)
			}
		}
		if authoredBlueprintServiceRemovalTask(task) {
			procedure, _, absenceConditions, absence, absenceErr := repository.serviceAbsenceProcedureAtRevision(
				ctx, task, candidate.readRevision,
			)
			if absenceErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, absenceErr
			}
			if absence {
				assignment.ServiceAbsenceProcedureSHA256, err = serviceAbsenceAuthorityDigest(procedure)
				if err != nil {
					etcdstore.ClearMutationValues(mutations)
					return TaskAssignment{}, false, err
				}
				conditions = append(conditions, absenceConditions...)
				conditions = append(
					conditions,
					etcdstore.Condition{Key: taskassignments.ServiceAbsenceRecoveryKey(task.ID)},
				)
			} else {
				authority, authorityDigest, authorityConditions, authorityErr :=
					repository.prepareAuthoredServiceRestorationAuthority(ctx, task, candidate.readRevision)
				if authorityErr != nil {
					etcdstore.ClearMutationValues(mutations)
					return TaskAssignment{}, false, authorityErr
				}
				assignment.RestorationAuthority, assignment.RestorationAuthoritySHA256 = &authority, authorityDigest
				conditions = append(conditions, authorityConditions...)
				conditions = append(conditions, etcdstore.Condition{Key: taskassignments.ReleaseRecoveryKey(task.ID)})
			}
			updatedAssignmentValue, encodeErr := taskassignments.EncodeTaskAssignment(assignment)
			if encodeErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, encodeErr
			}
			clear(assignmentValue)
			assignmentValue = updatedAssignmentValue
			mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
		}
		attachConditions, attachAssignmentValue, attachClaimErr := repository.prepareAttachRemovalRecoveryClaim(
			ctx, task, &assignment, candidate.readRevision)
		if attachClaimErr != nil {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, attachClaimErr
		}
		if attachAssignmentValue != nil {
			conditions = append(conditions, attachConditions...)
			clear(assignmentValue)
			assignmentValue = attachAssignmentValue
			mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
		}
		if authoredBlueprintZoneRemovalTask(task) || taskjournal.IsReleaseNetworkChild(task.Params) {
			procedure, _, networkConditions, found, procedureErr := repository.managedNetworkRecoveryProcedureAtRevision(
				ctx,
				task,
				candidate.readRevision,
			)
			if procedureErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, procedureErr
			}
			if !found {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, taskassignments.CorruptTaskAssignment()
			}
			assignment.ManagedNetworkProcedureSHA256, err = managedNetworkRecoveryAuthorityDigest(procedure)
			if err != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, err
			}
			conditions = append(conditions, networkConditions...)
			conditions = append(
				conditions,
				etcdstore.Condition{Key: taskassignments.ManagedNetworkRecoveryKey(task.ID)},
			)
			updatedAssignmentValue, encodeErr := taskassignments.EncodeTaskAssignment(assignment)
			if encodeErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, encodeErr
			}
			clear(assignmentValue)
			assignmentValue = updatedAssignmentValue
			mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
		}
		if !taskHasBlueprintCandidateAppliedAuthority(task) &&
			task.Params[releaserender.TaskReleasePublicationParam] != "" {
			authority, authorityDigest, authorityConditions, authorityErr := repository.prepareOrdinaryRestorationAuthority(
				ctx,
				task,
				candidate.readRevision,
			)
			if authorityErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, authorityErr
			}
			assignment.RestorationAuthority, assignment.RestorationAuthoritySHA256 = &authority, authorityDigest
			updatedAssignmentValue, encodeErr := taskassignments.EncodeTaskAssignment(assignment)
			if encodeErr != nil {
				etcdstore.ClearMutationValues(mutations)
				return TaskAssignment{}, false, encodeErr
			}
			clear(assignmentValue)
			assignmentValue = updatedAssignmentValue
			mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
			conditions = append(conditions, authorityConditions...)
			conditions = append(conditions, etcdstore.Condition{Key: taskassignments.ReleaseRecoveryKey(task.ID)})
		}
		attachChange, err := repository.prepareAttachTaskClaim(ctx, task, candidate.readRevision)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		if attachChange.applies {
			conditions = append(conditions, attachChange.conditions...)
			if attachChange.mutates {
				mutations = append(mutations, attachChange.mutations...)
			}
		}
		blueprintAttachChange, err := repository.prepareBlueprintAttachTaskClaim(ctx, task, candidate.readRevision)
		if err != nil {
			etcdstore.ClearMutationValues(mutations)
			clearAttachTaskChange(attachChange)
			return TaskAssignment{}, false, err
		}
		if blueprintAttachChange.applies {
			conditions = append(conditions, blueprintAttachChange.conditions...)
			mutations = append(mutations, blueprintAttachChange.mutations...)
		}
		if requirementApplies {
			conditions = append(conditions, requirementEvidence.conditions...)
		}
		transaction, err := repository.store.Transact(ctx, conditions, mutations)
		etcdstore.ClearMutationValues(mutations)
		clearAttachTaskChange(attachChange)
		clearBlueprintAttachTaskChange(blueprintAttachChange)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		etcdstore.ClearValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskAssignment{}, false, err
			}
			continue
		}
		return TaskAssignment{
			Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
				Record: assignment, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			},
			Task: etcdstore.Versioned[TaskRecord]{
				Record: running, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			},
		}, true, nil
	}
}
