package etcd

import (
	"bytes"
	"context"
	"math"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) readAttachRemovalRecovery(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord, revision int64,
) (taskassignments.AttachRemovalRecoveryRecord, *keyvalue.KeyValue, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskassignments.AttachRemovalRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return taskassignments.AttachRemovalRecoveryRecord{}, nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return taskassignments.AttachRemovalRecoveryRecord{}, nil, taskassignments.CorruptTaskAssignment()
	}
	value := read.Values[0]
	record, err := taskassignments.DecodeAttachRemovalRecoveryRecord(value.Value)
	if err != nil || taskassignments.ValidateAttachRemovalRecoveryAssignment(record, assignment) != nil ||
		record.Authority.OperationID != task.OperationID || record.Authority.PlanHash != task.PlanHash ||
		record.Authority.EnvironmentID != task.Owner.EnvironmentID || record.Authority.AttachID != task.Target ||
		record.EvidenceRevision > revision {
		clear(value.Value)
		return taskassignments.AttachRemovalRecoveryRecord{}, nil, taskassignments.CorruptTaskAssignment()
	}
	return record, value, nil
}

func (repository *TaskRepository) transitionAttachRemovalAcknowledgementToRecovery(
	ctx context.Context, task TaskRecord, taskValue *keyvalue.KeyValue,
	assignment taskassignments.TaskAssignmentRecord, assignmentValue, assignmentIndexValue *keyvalue.KeyValue,
	status taskjournal.TaskStatus, result taskjournal.TaskResultRecord, revision int64,
) (keyvalue.Versioned[TaskRecord], bool, error) {
	if assignment.AttachRemovalAuthoritySHA256 == "" || !result.ReconciliationRequired ||
		assignment.ExecutionMode != taskassignments.TaskExecutionModeForward || assignment.ExecutionEpoch == math.MaxUint32 {
		return keyvalue.Versioned[TaskRecord]{}, false, errs.New(
			errs.KindStateConflict,
			"Detach recovery authority changed",
		)
	}
	authority, sourceConditions, found, err := repository.attachRemovalRecoveryAuthorityAtRevision(ctx, task, revision)
	digest, digestErr := taskassignments.AttachRemovalRecoveryAuthoritySHA256(authority)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	if !found || digestErr != nil || digest != assignment.AttachRemovalAuthoritySHA256 {
		return keyvalue.Versioned[TaskRecord]{}, false, taskassignments.CorruptTaskAssignment()
	}
	evidence, err := repository.attachRemovalMutationEvidenceAtRevision(ctx, task, assignment, authority, revision)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	record, err := taskassignments.CaptureAttachRemovalRecovery(
		authority,
		assignment,
		status,
		result,
		evidence,
		revision,
	)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	recordValue, err := taskassignments.EncodeAttachRemovalRecoveryRecord(record)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	defer clear(recordValue)
	recoveryDigest, err := taskassignments.AttachRemovalRecoveryRecordSHA256(record)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	next := assignment
	next.ExecutionMode, next.ExecutionEpoch = taskassignments.TaskExecutionModeRecoveryOnly, assignment.ExecutionEpoch+1
	next.ReleaseRecoveryRecordSHA256 = recoveryDigest
	nextValue, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	defer clear(nextValue)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)
	timeout, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{timeoutKey}, Revision: revision},
	)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	if timeout == nil || timeout.ReadRevision != revision || len(timeout.Values) != 1 || timeout.Values[0] == nil ||
		timeout.Values[0].ModRevision != assignmentValue.ModRevision || !bytes.Equal(timeout.Values[0].Value, assignmentValue.Value) {
		return keyvalue.Versioned[TaskRecord]{}, false, errs.New(
			errs.KindStateConflict,
			"Detach recovery timeout owner changed",
		)
	}
	defer keyvalue.ClearValues(timeout.Values)
	claim, err := repository.prepareBlueprintChildAssignmentEpoch(ctx, task, assignment.ExecutionEpoch, true)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	defer claim.Clear()
	conditions := []keyvalue.Condition{
		{Key: taskClosingReportKey(task)},
		{Key: taskValue.Key, ModRevision: taskValue.ModRevision},
		{Key: assignmentValue.Key, ModRevision: assignmentValue.ModRevision},
		{Key: assignmentIndexValue.Key, ModRevision: assignmentIndexValue.ModRevision},
		{Key: timeoutKey, ModRevision: timeout.Values[0].ModRevision},
		{Key: taskassignments.AttachRemovalRecoveryKey(task.ID)},
		{Key: taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline)},
		{Key: taskjournal.TaskRecoveryProofRequiredKey(task.ID)},
	}
	conditions = append(conditions, sourceConditions...)
	conditions = append(conditions, claim.Conditions()...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskValue.Key, Value: taskValue.Value},
		{Type: keyvalue.MutationPut, Key: assignmentValue.Key, Value: nextValue},
		{Type: keyvalue.MutationPut, Key: assignmentIndexValue.Key, Value: nextValue},
		{Type: keyvalue.MutationDelete, Key: timeoutKey},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline),
			Value: nextValue,
		},
		{Type: keyvalue.MutationPut, Key: taskassignments.AttachRemovalRecoveryKey(task.ID), Value: recordValue},
	}
	mutations = append(mutations, claim.Mutations()...)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, false, err
	}
	keyvalue.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return keyvalue.Versioned[TaskRecord]{}, false, nil
	}
	return keyvalue.Versioned[TaskRecord]{
		Record:       task,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, true, nil
}
