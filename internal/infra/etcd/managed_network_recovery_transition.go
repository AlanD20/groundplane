package etcd

import (
	"bytes"
	"context"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) transitionManagedNetworkAcknowledgementToRecovery(
	ctx context.Context,
	task TaskRecord,
	taskValue *keyvalue.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue, assignmentIndexValue *keyvalue.KeyValue,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (keyvalue.Versioned[TaskRecord], bool, error) {
	if assignment.ManagedNetworkProcedureSHA256 == "" || !result.ReconciliationRequired {
		return keyvalue.Versioned[TaskRecord]{}, false, nil
	}
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeForward ||
		!taskassignments.ValidTerminalTaskStatus(status) || result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != "" || assignment.ExecutionEpoch == math.MaxUint32 ||
		assignment.RestorationAuthority != nil ||
		assignment.ServiceAbsenceProcedureSHA256 != "" {
		return keyvalue.Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"managed network recovery authority changed",
		)
	}
	procedure, _, sourceConditions, found, err := repository.managedNetworkRecoveryProcedureAtRevision(
		ctx, task, revision,
	)
	if err != nil || !found {
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, true, err
		}
		return keyvalue.Versioned[TaskRecord]{}, true, taskassignments.CorruptTaskAssignment()
	}
	digest, err := managedNetworkRecoveryAuthorityDigest(procedure)
	if err != nil || digest != assignment.ManagedNetworkProcedureSHA256 {
		return keyvalue.Versioned[TaskRecord]{}, true, taskassignments.CorruptTaskAssignment()
	}
	mutationEvidence, err := repository.managedNetworkMutationEvidenceAtRevision(
		ctx, task, assignment, procedure, revision,
	)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	if len(mutationEvidence) == 0 {
		return keyvalue.Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"managed network operation has no durable mutation evidence",
		)
	}
	reportDigest, err := taskassignments.CanonicalPrimaryReportSHA256(status, result)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	record := taskassignments.ManagedNetworkRecoveryRecord{
		Schema: 2, TaskID: task.ID, AssignmentID: assignment.AssignmentID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, ProcedureSHA256: digest,
		PrimaryReportSHA256: reportDigest, PrimaryStatus: status, PrimaryResult: *taskjournal.TaskResultToData(&result),
		RecoveryDeadline: assignment.RecoveryDeadline,
		ForwardStepIDs: executionplan.NetworkRecoveryForwardSteps(
			procedure,
		), SettlementStepIDs: executionplan.NetworkRecoverySettlementSteps(procedure),
		OriginalExecutionEpoch: assignment.ExecutionEpoch,
		MutationEvidence:       slices.Clone(mutationEvidence), EvidenceRevision: revision,
	}
	recoveryValue, err := taskassignments.EncodeManagedNetworkRecoveryRecord(record)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	defer clear(recoveryValue)
	recoveryDigest, err := taskassignments.ManagedNetworkRecoveryRecordSHA256(record)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	next := assignment
	next.ExecutionMode, next.ExecutionEpoch = taskassignments.TaskExecutionModeRecoveryOnly, assignment.ExecutionEpoch+1
	next.ReleaseRecoveryRecordSHA256 = recoveryDigest
	nextValue, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	defer clear(nextValue)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)
	timeoutRead, err := repository.store.GetMany(
		ctx,
		keyvalue.GetManyRequest{Keys: []string{timeoutKey}, Revision: revision},
	)
	if err != nil || timeoutRead == nil || timeoutRead.ReadRevision != revision || len(timeoutRead.Values) != 1 ||
		timeoutRead.Values[0] == nil || timeoutRead.Values[0].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(timeoutRead.Values[0].Value, assignmentValue.Value) {
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, true, err
		}
		return keyvalue.Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"managed network recovery timeout authority changed",
		)
	}
	conditions := []keyvalue.Condition{
		{Key: taskClosingReportKey(task)},
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: assignmentValue.Key, ModRevision: assignmentValue.ModRevision},
		{Key: assignmentIndexValue.Key, ModRevision: assignmentIndexValue.ModRevision},
		{Key: timeoutKey, ModRevision: timeoutRead.Values[0].ModRevision},
		{Key: taskassignments.ManagedNetworkRecoveryKey(task.ID)},
		{Key: taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline)},
		{Key: taskjournal.TaskRecoveryProofRequiredKey(task.ID)},
	}
	conditions = append(conditions, sourceConditions...)
	claim, err := repository.prepareBlueprintChildAssignmentEpoch(ctx, task, assignment.ExecutionEpoch, true)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	defer claim.Clear()
	conditions = append(conditions, claim.Conditions()...)
	mutations := []keyvalue.Mutation{
		{Type: keyvalue.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue.Value},
		{Type: keyvalue.MutationPut, Key: assignmentValue.Key, Value: nextValue},
		{Type: keyvalue.MutationPut, Key: assignmentIndexValue.Key, Value: nextValue},
		{Type: keyvalue.MutationDelete, Key: timeoutKey},
		{
			Type:  keyvalue.MutationPut,
			Key:   taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline),
			Value: nextValue,
		},
		{Type: keyvalue.MutationPut, Key: taskassignments.ManagedNetworkRecoveryKey(task.ID), Value: recoveryValue},
	}
	mutations = append(mutations, claim.Mutations()...)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	keyvalue.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return keyvalue.Versioned[TaskRecord]{}, false, nil
	}
	return keyvalue.Versioned[TaskRecord]{
		Record: task, Revision: transaction.Revision, ReadRevision: transaction.Revision,
	}, true, nil
}

func (repository *TaskRepository) managedNetworkRecoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskassignments.ManagedNetworkRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if err != nil {
			return releaseRecoveryAcknowledgement{}, err
		}
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeManagedNetworkRecoveryRecord(read.Values[0].Value)
	digest, digestErr := taskassignments.ManagedNetworkRecoveryRecordSHA256(record)
	if err != nil || digestErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
		record.ProcedureSHA256 != assignment.ManagedNetworkProcedureSHA256 || record.TaskID != task.ID ||
		record.AssignmentID != assignment.AssignmentID || record.OperationID != task.OperationID ||
		record.PlanHash != task.PlanHash || assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
		record.OriginalExecutionEpoch+1 != assignment.ExecutionEpoch ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	resolved := releaseRecoveryAcknowledgement{value: read.Values[0]}
	if result.ExecutionEpoch < assignment.ExecutionEpoch && result.ReleaseRecoveryRecordSHA256 == "" {
		replayDigest, replayErr := taskassignments.CanonicalPrimaryReportSHA256(status, result)
		if replayErr != nil || replayDigest != record.PrimaryReportSHA256 {
			return releaseRecoveryAcknowledgement{}, errs.New(
				errs.KindStateConflict,
				"old managed network recovery acknowledgement changed",
			)
		}
		return resolved, nil
	}
	if result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"managed network recovery acknowledgement epoch changed",
		)
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired {
		return resolved, nil
	}
	procedure, _, sourceConditions, found, err := repository.managedNetworkRecoveryProcedureAtRevision(
		ctx, task, revision,
	)
	procedureDigest, procedureDigestErr := managedNetworkRecoveryAuthorityDigest(procedure)
	if err != nil || !found || procedureDigestErr != nil ||
		procedureDigest != assignment.ManagedNetworkProcedureSHA256 ||
		!slices.Equal(executionplan.NetworkRecoveryForwardSteps(procedure), record.ForwardStepIDs) ||
		!slices.Equal(
			executionplan.NetworkRecoverySettlementSteps(procedure),
			record.SettlementStepIDs,
		) || result.Kind != taskjournal.TaskResultCompose ||
		len(
			result.Projects,
		) != 0 || len(result.ProxyEvidence) != 0 || len(result.RecreateEvidence) != 0 || len(result.RollingEvidence) != 0 ||
		result.CandidateAbsenceEvidence != nil || result.ServiceAbsenceEvidence != nil || len(result.BackingObservations) != 0 ||
		result.DNSResolverCandidateObservation != nil || result.DNSResolverRollbackObservation != nil {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"managed network recovery proof is invalid",
		)
	}
	for _, stepID := range record.SettlementStepIDs {
		if err := repository.validateManagedNetworkRecoveryCompletedEvent(
			ctx, task, assignment.AssignmentID, assignment.AgentID, assignment.AgentGeneration,
			assignment.ExecutionEpoch, stepID, revision,
		); err != nil {
			return releaseRecoveryAcknowledgement{}, err
		}
	}
	primary, primaryErr := taskjournal.TaskResultFromData(&record.PrimaryResult)
	if primaryErr != nil || primary == nil {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	primary.ReconciliationRequired = false
	primary.ExecutionEpoch = assignment.ExecutionEpoch
	primary.ReleaseRecoveryRecordSHA256 = assignment.ReleaseRecoveryRecordSHA256
	resolved.final, resolved.status, resolved.result = true, record.PrimaryStatus, *primary
	resolved.conditions = append(sourceConditions, keyvalue.Condition{
		Key: taskassignments.ManagedNetworkRecoveryKey(task.ID), ModRevision: read.Values[0].ModRevision,
	})
	return resolved, nil
}

func (repository *TaskRepository) normalizeManagedNetworkRecoveryTerminalReplay(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	agentGeneration uint64,
	assignmentID string,
	revision int64,
) (taskjournal.TaskStatus, *taskjournal.TaskResultRecord, bool, error) {
	if result.ReleaseRecoveryRecordSHA256 == "" || !authoredBlueprintZoneRemovalTask(task) {
		return status, nil, false, nil
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired || task.Result == nil ||
		!taskassignments.ValidTerminalTaskStatus(task.Status) || task.TerminalAssignment == nil ||
		task.Result.ExecutionEpoch != result.ExecutionEpoch ||
		task.Result.ReleaseRecoveryRecordSHA256 != result.ReleaseRecoveryRecordSHA256 ||
		task.TerminalAssignment.AssignmentID != assignmentID || task.TerminalAssignment.AgentID != agentID ||
		task.TerminalAssignment.AgentGeneration != agentGeneration ||
		result.Kind != taskjournal.TaskResultCompose || len(result.Projects) != 0 ||
		len(result.ProxyEvidence) != 0 || len(result.RecreateEvidence) != 0 || len(result.RollingEvidence) != 0 ||
		result.CandidateAbsenceEvidence != nil || result.ServiceAbsenceEvidence != nil || len(result.BackingObservations) != 0 ||
		result.DNSResolverCandidateObservation != nil || result.DNSResolverRollbackObservation != nil {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal managed network recovery replay authority changed",
		)
	}
	if err := repository.validateManagedNetworkRecoveryCompletedEvent(
		ctx, task, assignmentID, agentID, agentGeneration, result.ExecutionEpoch, task.Steps[0].ID, revision,
	); err != nil {
		return status, nil, true, err
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskassignments.ManagedNetworkRecoveryKey(task.ID), taskjournal.TaskActiveOperationKey(task.OperationID),
	}, Revision: revision})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if err != nil {
			return status, nil, true, err
		}
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal managed network recovery cleanup is unproven",
		)
	}
	if read.Values[0] != nil || read.Values[1] != nil {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal managed network recovery authority remains",
		)
	}
	return task.Status, taskjournal.CloneTaskResult(task.Result), true, nil
}
