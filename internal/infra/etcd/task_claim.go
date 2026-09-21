package etcd

import (
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	idempotencyrecord "github.com/AlanD20/groundplane/internal/infra/etcd/idempotency"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// ClaimNextTask claims the oldest queued Task by ascending ULID. Queue
// membership, pending Task state, active-operation membership, and the absent
// assignment are compared in the same transaction.
func (repository *TaskRepository) ClaimNextTask(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	assignedAt time.Time,
) (TaskAssignment, bool, error) {
	return repository.claimNextTask(ctx, taskjournal.TaskExecutorAgent, agentID, agentGeneration, assignedAt)
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
	if !taskjournal.ValidExecutor(executor) ||
		(executor == taskjournal.TaskExecutorAgent && (recordcodec.ValidateID(ids.KindAgent, agentID) != nil || agentGeneration == 0)) ||
		(executor == taskjournal.TaskExecutorController && (agentID != "" || agentGeneration != 0)) {
		return TaskAssignment{}, false, errs.New(errs.KindValidationFailed, "task execution claim identity is invalid")
	}
	if err := recordcodec.ValidateTimestamp("task assignment assigned_at", assignedAt); err != nil {
		return TaskAssignment{}, false, err
	}

	conflicts := 0
	for {
		candidate, found, err := repository.nextTaskClaimCandidate(ctx, executor)
		if err != nil || !found {
			return TaskAssignment{}, false, err
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
		deadline := claimAt.Add(time.Duration(task.TimeoutSeconds) * time.Second)
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
		running, err := transitionTaskStatus(task, taskjournal.TaskStatusPending, taskjournal.TaskStatusRunning, claimAt)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignment := TaskAssignmentRecord{
			AssignmentID: ids.New(ids.KindAssignment),
			TaskID:       task.ID, Executor: executor, AgentID: agentID, AgentGeneration: agentGeneration,
			ClaimedTaskRevision: taskValue.ModRevision, AssignedAt: claimAt, Deadline: deadline,
			RecoveryDeadline: recoveryDeadline, ExecutionMode: TaskExecutionModeForward, ExecutionEpoch: 1,
		}
		runningValue, err := encodeTaskRecord(running)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		assignmentValue, err := encodeTaskAssignment(assignment)
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
		mutations := []etcdstore.Mutation{
			{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: runningValue},
			{Type: etcdstore.MutationDelete, Key: queued.Key},
			{Type: etcdstore.MutationPut, Key: assignmentKey, Value: assignmentValue},
			{Type: etcdstore.MutationPut, Key: assignmentIndexKey, Value: assignmentValue},
			{Type: etcdstore.MutationPut, Key: timeoutIndexKey, Value: assignmentValue},
		}
		hookInputConditions, err := repository.backingHookInputClaimConditions(ctx, task, candidate.readRevision)
		if err != nil {
			clearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		conditions = append(conditions, hookInputConditions...)
		scriptSource, scriptSourceReady, err :=
			repository.prepareScriptTaskClaimSourceAuthority(
				ctx, task, taskValue.ModRevision, candidate.readRevision,
			)
		if err != nil {
			clearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		if !scriptSourceReady {
			clearMutationValues(mutations)
			return TaskAssignment{}, false, nil
		}
		defer scriptSource.Clear()
		conditions = append(conditions, scriptSource.conditions...)
		mutations = append(mutations, scriptSource.mutations...)
		requirementEvidence, requirementApplies, requirementReady, err :=
			repository.observeBlueprintRequirementGateForClaim(ctx, task, candidate.readRevision)
		if err != nil {
			clearMutationValues(mutations)
			return TaskAssignment{}, false, err
		}
		if requirementApplies && !requirementReady {
			clearMutationValues(mutations)
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
					clearMutationValues(mutations)
					return TaskAssignment{}, false, authorityErr
				}
				assignment.RestorationAuthority, assignment.RestorationAuthoritySHA256 = &authority, authorityDigest
				updatedAssignmentValue, encodeErr := encodeTaskAssignment(assignment)
				if encodeErr != nil {
					clearMutationValues(mutations)
					return TaskAssignment{}, false, encodeErr
				}
				clear(assignmentValue)
				assignmentValue = updatedAssignmentValue
				mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
				conditions = append(conditions, authorityConditions...)
				conditions = append(conditions, etcdstore.Condition{Key: releaseRecoveryKey(task.ID)})
				epochCondition, epochMutation, claimErr :=
					repository.prepareBlueprintCandidateClaimEpoch(
						ctx, task, writer, candidate.readRevision, requirementEvidence.gateRevision,
					)
				if claimErr != nil {
					clearMutationValues(mutations)
					return TaskAssignment{}, false, claimErr
				}
				conditions = append(conditions, epochCondition)
				mutations = append(mutations, epochMutation)
			}
		}
		if !taskHasBlueprintCandidateAppliedAuthority(task) && task.Params[TaskReleasePublicationParam] != "" {
			authority, authorityDigest, authorityConditions, authorityErr := repository.prepareOrdinaryRestorationAuthority(
				ctx,
				task,
				candidate.readRevision,
			)
			if authorityErr != nil {
				clearMutationValues(mutations)
				return TaskAssignment{}, false, authorityErr
			}
			assignment.RestorationAuthority, assignment.RestorationAuthoritySHA256 = &authority, authorityDigest
			updatedAssignmentValue, encodeErr := encodeTaskAssignment(assignment)
			if encodeErr != nil {
				clearMutationValues(mutations)
				return TaskAssignment{}, false, encodeErr
			}
			clear(assignmentValue)
			assignmentValue = updatedAssignmentValue
			mutations[2].Value, mutations[3].Value, mutations[4].Value = assignmentValue, assignmentValue, assignmentValue
			conditions = append(conditions, authorityConditions...)
			conditions = append(conditions, etcdstore.Condition{Key: releaseRecoveryKey(task.ID)})
		}
		attachChange, err := repository.prepareAttachTaskClaim(ctx, task, candidate.readRevision)
		if err != nil {
			clearMutationValues(mutations)
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
			clearMutationValues(mutations)
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
		clearMutationValues(mutations)
		clearAttachTaskChange(attachChange)
		clearBlueprintAttachTaskChange(blueprintAttachChange)
		if err != nil {
			return TaskAssignment{}, false, err
		}
		clearKeyValues(transaction.FailureReads)
		if !transaction.Succeeded {
			conflicts++
			if err := repository.retryPolicy.waitAfterConflict(ctx, conflicts); err != nil {
				return TaskAssignment{}, false, err
			}
			continue
		}
		return TaskAssignment{
			Assignment: etcdstore.Versioned[TaskAssignmentRecord]{
				Record: assignment, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			},
			Task: etcdstore.Versioned[TaskRecord]{
				Record: running, Revision: transaction.Revision, ReadRevision: transaction.Revision,
			},
		}, true, nil
	}
}

const taskClaimQueuePageSize = 64

type taskClaimCandidate struct {
	queued        etcdstore.KeyValue
	taskValue     *etcdstore.KeyValue
	task          TaskRecord
	readRevision  int64
	writerKey     string
	environmentID string
}

func (repository *TaskRepository) nextTaskClaimCandidate(
	ctx context.Context,
	executor taskjournal.TaskExecutor,
) (taskClaimCandidate, bool, error) {
	prefix := taskjournal.TaskQueueScopePrefix(executor)
	start := ""
	var revision int64
	for {
		page, err := repository.store.Range(ctx, etcdstore.RangeRequest{
			Prefix: prefix, StartExclusive: start, Limit: taskClaimQueuePageSize, Revision: revision,
		})
		if err != nil {
			return taskClaimCandidate{}, false, err
		}
		if revision == 0 {
			revision = page.ReadRevision
		}
		if page.ReadRevision != revision {
			return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "task queue scan changed MVCC revision")
		}
		for _, queued := range page.Values {
			taskID, err := taskjournal.TaskIDFromQueueKey(executor, queued.Key)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			referencedTaskID, err := idempotencyrecord.DecodeTaskReference(queued.Value)
			if err != nil || referencedTaskID != taskID {
				return taskClaimCandidate{}, false, errs.New(
					errs.KindInternal,
					"task queue record does not match its key",
				)
			}
			taskRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: []string{taskjournal.TaskStorageKey(taskID)}, Revision: revision,
			})
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if len(taskRead.Values) != 1 || taskRead.Values[0] == nil {
				return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "queued Task primary is missing")
			}
			taskValue := taskRead.Values[0]
			task, err := decodeTaskRecord(taskValue.Value)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			if task.ID != taskID || task.Executor != executor || task.Status != taskjournal.TaskStatusPending ||
				task.idempotencyMarker == nil && !isMarkerlessHierarchyDeletionAgentChild(task) {
				return taskClaimCandidate{}, false, errs.New(errs.KindInternal, "queued Task is not claimable")
			}
			environmentID, materializes, err := taskEnvironmentWriter(task)
			if err != nil {
				return taskClaimCandidate{}, false, err
			}
			writerKey := ""
			if materializes {
				writerKey = taskjournal.TaskMaterializationWriterKey(environmentID)
				writerRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
					Keys: []string{writerKey}, Revision: revision,
				})
				if err != nil {
					return taskClaimCandidate{}, false, err
				}
				if len(writerRead.Values) != 1 {
					return taskClaimCandidate{}, false, errs.New(
						errs.KindInternal,
						"materialization writer read is incomplete",
					)
				}
				if writerRead.Values[0] != nil {
					writer, err := decodeTaskMaterializationWriter(writerRead.Values[0].Value)
					if err != nil || writer.EnvironmentID != environmentID || writer.TaskID == task.ID {
						return taskClaimCandidate{}, false, corruptTaskMaterializationWriter()
					}
					continue
				}
			}
			return taskClaimCandidate{
				queued: queued, taskValue: taskValue, task: task, readRevision: revision,
				writerKey: writerKey, environmentID: environmentID,
			}, true, nil
		}
		if !page.More {
			return taskClaimCandidate{}, false, nil
		}
		if len(page.Values) == 0 {
			return taskClaimCandidate{}, false, errs.New(
				errs.KindInternal,
				"task queue page is empty before completion",
			)
		}
		start = page.Values[len(page.Values)-1].Key
	}
}
