package etcd

import (
	"bytes"
	"context"
	"github.com/AlanD20/groundplane/internal/common/ids"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// ListAgentAssignments restores the complete bounded assignment set for one
// exact Agent generation at one MVCC revision. The caller supplies the
// authorized max-concurrency bound from the Agent config.
func (repository *TaskRepository) ListAgentAssignments(
	ctx context.Context,
	agentID string,
	agentGeneration uint64,
	maximum int32,
) ([]TaskAssignment, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	if recordcodec.ValidateID(ids.KindAgent, agentID) != nil || agentGeneration == 0 || maximum <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "agent assignment query is invalid")
	}
	assignments, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: taskjournal.TaskAssignmentScopePrefix(agentID),
		Limit:  int64(maximum) + 1,
	})
	if err != nil {
		return nil, err
	}
	if assignments.More || len(assignments.Values) > int(maximum) {
		return nil, errs.New(errs.KindInternal, "agent assignments exceed configured concurrency")
	}
	if len(assignments.Values) == 0 {
		return []TaskAssignment{}, nil
	}

	records := make([]taskassignments.TaskAssignmentRecord, len(assignments.Values))
	companionKeys := make([]string, 0, len(assignments.Values)*4)
	for index, value := range assignments.Values {
		taskID, err := taskjournal.TaskIDFromAssignmentKey(agentID, value.Key)
		if err != nil {
			return nil, err
		}
		record, err := taskassignments.DecodeTaskAssignment(value.Value)
		if err != nil {
			return nil, err
		}
		if record.TaskID != taskID || record.AgentID != agentID {
			return nil, errs.New(errs.KindInternal, "task assignment record does not match its key")
		}
		if record.AgentGeneration != agentGeneration {
			return nil, errs.New(errs.KindStateConflict, "durable Task assignment belongs to another Agent generation")
		}
		if record.ClaimedTaskRevision >= value.ModRevision {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		records[index] = record
		timeoutDeadline := record.Deadline
		if record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
			timeoutDeadline = record.RecoveryDeadline
		}
		companionKeys = append(
			companionKeys,
			taskjournal.TaskStorageKey(taskID),
			taskjournal.TaskAssignmentIndexKey(taskID),
			taskjournal.TaskTimeoutIndexKey(taskID, timeoutDeadline),
			taskjournal.TaskRecoveryProofRequiredKey(taskID),
		)
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: companionKeys, Revision: assignments.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if len(companions.Values) != len(companionKeys) {
		return nil, errs.New(errs.KindInternal, "agent assignment companion read is incomplete")
	}
	result := make([]TaskAssignment, len(records))
	for index, record := range records {
		taskValue := companions.Values[index*4]
		indexValue := companions.Values[index*4+1]
		timeoutValue := companions.Values[index*4+2]
		proofRequiredValue := companions.Values[index*4+3]
		assignmentValue := assignments.Values[index]
		proofRequired := proofRequiredValue != nil
		if taskValue == nil || indexValue == nil ||
			timeoutValue == nil == (record.ExecutionMode == taskassignments.TaskExecutionModeForward || !proofRequired) ||
			proofRequired && record.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly {
			return nil, errs.New(errs.KindInternal, "assigned Task companion is missing")
		}
		lifecycleValue := timeoutValue
		if proofRequired {
			lifecycleValue = proofRequiredValue
		}
		if indexValue.ModRevision != assignmentValue.ModRevision ||
			lifecycleValue.ModRevision != assignmentValue.ModRevision ||
			!bytes.Equal(indexValue.Value, assignmentValue.Value) ||
			!bytes.Equal(lifecycleValue.Value, assignmentValue.Value) {
			return nil, errs.New(errs.KindInternal, "durable Task assignment copies do not match")
		}
		task, err := decodeTaskRecord(taskValue.Value)
		if err != nil {
			return nil, err
		}
		if task.ID != record.TaskID || task.Status != taskjournal.TaskStatusRunning ||
			task.StartedAt == nil || !task.StartedAt.Equal(record.AssignedAt) ||
			!record.Deadline.Equal(record.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
			!record.RecoveryDeadline.Equal(record.Deadline.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
			taskValue.ModRevision < assignmentValue.ModRevision ||
			task.idempotencyMarker == nil && !isMarkerlessHierarchyDeletionAgentChild(task) {
			return nil, errs.New(errs.KindInternal, "durable Task assignment and Task are inconsistent")
		}
		environmentID, materializes, err := taskMaterializationEnvironment(task)
		if err != nil {
			return nil, err
		}
		if materializes {
			writerKeys := []string{taskjournal.TaskMaterializationWriterKey(environmentID)}
			writerRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
				Keys: writerKeys, Revision: assignments.ReadRevision,
			})
			if err != nil {
				return nil, err
			}
			if len(writerRead.Values) != len(writerKeys) || writerRead.Values[0] == nil {
				return nil, errs.New(errs.KindInternal, "assigned Task materialization writer is missing")
			}
			writer, err := decodeTaskMaterializationWriter(writerRead.Values[0].Value)
			if err != nil || validateTaskMaterializationWriterForTask(writer, task, environmentID) != nil {
				return nil, corruptTaskMaterializationWriter()
			}
		}
		var recovery *taskassignments.ReleaseRecoveryDirective
		if task.Params[TaskReleasePublicationParam] != "" {
			_, procedure, descriptorErr := repository.candidateReleaseDescriptorAtRevision(
				ctx, task, assignments.ReadRevision,
			)
			if descriptorErr != nil || validateAssignmentRestorationDescriptor(task, record, procedure) != nil {
				return nil, taskassignments.CorruptTaskAssignment()
			}
			if record.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
				recovery, err = repository.releaseRecoveryDirectiveAtRevision(
					ctx, task, record, procedure, assignments.ReadRevision,
				)
				if err != nil {
					return nil, err
				}
			}
		}
		result[index] = TaskAssignment{
			Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
				Record: record, Revision: assignmentValue.ModRevision,
				ReadRevision: assignments.ReadRevision,
			},
			Task: etcdstore.Versioned[TaskRecord]{
				Record: task, Revision: taskValue.ModRevision,
				ReadRevision: assignments.ReadRevision,
			},
			ReleaseRecovery:       recovery,
			RecoveryProofRequired: proofRequired,
		}
	}
	return result, nil
}

func isMarkerlessHierarchyDeletionAgentChild(task TaskRecord) bool {
	return task.Executor == taskjournal.TaskExecutorAgent && task.Actor == taskjournal.TaskActorSystem &&
		task.Params[taskjournal.TaskResourceKindParam] == taskjournal.TaskResourceHierarchyDeletion
}

// ListControllerTaskClaims restores the single native Controller execution
// claim at one MVCC revision. The MVP runs one Controller worker serially, so
// more than one durable claim is corruption rather than hidden concurrency.
func (repository *TaskRepository) ListControllerTaskClaims(
	ctx context.Context,
) ([]TaskAssignment, error) {
	if err := etcdstore.ValidateContext(ctx); err != nil {
		return nil, err
	}
	claims, err := repository.store.Range(ctx, etcdstore.RangeRequest{
		Prefix: taskjournal.ControllerTaskClaimPrefix,
		Limit:  2,
	})
	if err != nil {
		return nil, err
	}
	if claims.More || len(claims.Values) > 1 {
		return nil, errs.New(errs.KindInternal, "controller Task claims exceed serial execution")
	}
	if len(claims.Values) == 0 {
		return []TaskAssignment{}, nil
	}
	claimValue := claims.Values[0]
	claim, err := taskassignments.DecodeTaskAssignment(claimValue.Value)
	if err != nil {
		return nil, err
	}
	if claim.Executor != taskjournal.TaskExecutorController || claimValue.Key != taskjournal.ControllerTaskClaimKey(claim.TaskID) ||
		claim.ClaimedTaskRevision >= claimValue.ModRevision {
		return nil, errs.New(errs.KindInternal, "controller Task claim does not match its key")
	}
	companions, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{
			taskjournal.TaskStorageKey(claim.TaskID),
			taskjournal.TaskAssignmentIndexKey(claim.TaskID),
			taskjournal.TaskTimeoutIndexKey(claim.TaskID, claim.Deadline),
		},
		Revision: claims.ReadRevision,
	})
	if err != nil {
		return nil, err
	}
	if len(companions.Values) != 3 || companions.Values[0] == nil ||
		companions.Values[1] == nil || companions.Values[2] == nil {
		return nil, errs.New(errs.KindInternal, "claimed Controller Task companion is missing")
	}
	taskValue := companions.Values[0]
	if companions.Values[1].ModRevision != claimValue.ModRevision ||
		companions.Values[2].ModRevision != claimValue.ModRevision ||
		!bytes.Equal(companions.Values[1].Value, claimValue.Value) ||
		!bytes.Equal(companions.Values[2].Value, claimValue.Value) {
		return nil, errs.New(errs.KindInternal, "Controller Task assignment copies do not match")
	}
	task, err := decodeTaskRecord(taskValue.Value)
	if err != nil {
		return nil, err
	}
	if task.ID != claim.TaskID || task.Executor != taskjournal.TaskExecutorController ||
		task.Status != taskjournal.TaskStatusRunning || task.StartedAt == nil ||
		!task.StartedAt.Equal(claim.AssignedAt) ||
		!claim.Deadline.Equal(claim.AssignedAt.Add(time.Duration(task.TimeoutSeconds)*time.Second)) ||
		taskValue.ModRevision < claimValue.ModRevision || task.idempotencyMarker == nil {
		return nil, errs.New(errs.KindInternal, "controller Task claim and Task are inconsistent")
	}
	return []TaskAssignment{{
		Assignment: etcdstore.Versioned[taskassignments.TaskAssignmentRecord]{
			Record: claim, Revision: claimValue.ModRevision, ReadRevision: claims.ReadRevision,
		},
		Task: etcdstore.Versioned[TaskRecord]{
			Record: task, Revision: taskValue.ModRevision, ReadRevision: claims.ReadRevision,
		},
	}}, nil
}
