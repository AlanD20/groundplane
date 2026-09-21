package etcd

import (
	"bytes"
	"context"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/recordcodec"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"math"
	"slices"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

const releaseRecoveryProofExecutionBudget = 5 * time.Minute

func (repository *TaskRepository) transitionReleaseAcknowledgementToRecovery(
	ctx context.Context,
	task TaskRecord,
	taskValue *etcdstore.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue *etcdstore.KeyValue,
	assignmentIndexValue *etcdstore.KeyValue,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
	evidenceConditions ...etcdstore.Condition,
) (etcdstore.Versioned[TaskRecord], bool, error) {
	if task.Params[TaskReleasePublicationParam] == "" || !result.ReconciliationRequired {
		return etcdstore.Versioned[TaskRecord]{}, false, nil
	}
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeForward || result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != "" || assignment.ExecutionEpoch == math.MaxUint32 ||
		assignment.RestorationAuthority == nil {
		return etcdstore.Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"release recovery acknowledgement authority changed",
		)
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	if err != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, taskassignments.CorruptTaskAssignment()
	}
	stepIDs, err := releaseRestorationStepIDs(procedure, assignment.RestorationAuthority.Candidates)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	reportDigest, err := taskassignments.CanonicalPrimaryReportSHA256(status, result)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	_, mutationEvidence, err := repository.releaseCandidateMutationEvidenceAtRevision(
		ctx,
		task,
		assignment,
		procedure,
		revision,
	)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	record := taskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: task.ID, AssignmentID: assignment.AssignmentID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, RestorationAuthoritySHA256: assignment.RestorationAuthoritySHA256,
		PrimaryReportSHA256: reportDigest, PrimaryStatus: status, PrimaryResult: result,
		RecoveryDeadline: assignment.RecoveryDeadline,
		MutationEvidence: mutationEvidence,
		RecoveryStepIDs:  stepIDs, Cursor: 0, Phase: taskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: revision,
	}
	recoveryValue, err := taskassignments.EncodeReleaseRecoveryRecord(record)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	defer clear(recoveryValue)
	recoveryDigest, err := taskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	next := assignment
	next.ExecutionMode = taskassignments.TaskExecutionModeRecoveryOnly
	next.ExecutionEpoch++
	next.ReleaseRecoveryRecordSHA256 = recoveryDigest
	nextValue, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	defer clear(nextValue)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)
	timeoutRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{timeoutKey}, Revision: revision})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	if timeoutRead == nil || timeoutRead.ReadRevision != revision || len(timeoutRead.Values) != 1 ||
		timeoutRead.Values[0] == nil || timeoutRead.Values[0].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(timeoutRead.Values[0].Value, assignmentValue.Value) {
		return etcdstore.Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"release recovery timeout authority changed",
		)
	}
	conditions := []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: assignmentValue.Key, ModRevision: assignmentValue.ModRevision},
		{Key: assignmentIndexValue.Key, ModRevision: assignmentIndexValue.ModRevision},
		{Key: timeoutKey, ModRevision: timeoutRead.Values[0].ModRevision},
		{Key: taskassignments.ReleaseRecoveryKey(task.ID)},
		{Key: taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline)},
		{Key: taskjournal.TaskRecoveryProofRequiredKey(task.ID)},
	}
	conditions = append(conditions, evidenceConditions...)
	transaction, err := repository.store.Transact(ctx, conditions, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue.Value},
		{Type: etcdstore.MutationPut, Key: assignmentValue.Key, Value: nextValue},
		{Type: etcdstore.MutationPut, Key: assignmentIndexValue.Key, Value: nextValue},
		{Type: etcdstore.MutationDelete, Key: timeoutKey},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline), Value: nextValue},
		{Type: etcdstore.MutationPut, Key: taskassignments.ReleaseRecoveryKey(task.ID), Value: recoveryValue},
	})
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[TaskRecord]{}, false, nil
	}
	return etcdstore.Versioned[TaskRecord]{
		Record:       task,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, true, nil
}

func (repository *TaskRepository) releaseEffectEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) (bool, []etcdstore.Condition, error) {
	effect, _, err := repository.releaseCandidateMutationEvidenceAtRevision(ctx, task, assignment, procedure, revision)
	if err != nil {
		return false, nil, err
	}
	scriptEffect, scriptConditions, err := repository.releaseScriptEffectEvidenceAtRevision(
		ctx,
		task,
		assignment,
		revision,
	)
	if err != nil {
		return false, nil, err
	}
	componentEffect, err := repository.releaseComponentEffectAtRevision(ctx, task, assignment, revision)
	if err != nil {
		return false, nil, err
	}
	return effect || scriptEffect || componentEffect, scriptConditions, nil
}

func (repository *TaskRepository) releaseRecoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	read, err := repository.store.GetMany(
		ctx,
		etcdstore.GetManyRequest{Keys: []string{taskassignments.ReleaseRecoveryKey(task.ID)}, Revision: revision},
	)
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if err != nil {
			return releaseRecoveryAcknowledgement{}, err
		}
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeReleaseRecoveryRecord(read.Values[0].Value)
	digest, digestErr := taskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil || digestErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
		record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID ||
		record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	resolved := releaseRecoveryAcknowledgement{record: record, value: read.Values[0]}
	if result.ExecutionEpoch < assignment.ExecutionEpoch && result.ReleaseRecoveryRecordSHA256 == "" {
		replayDigest, replayErr := taskassignments.CanonicalPrimaryReportSHA256(status, result)
		if replayErr != nil || replayDigest != record.PrimaryReportSHA256 {
			return releaseRecoveryAcknowledgement{}, errs.New(
				errs.KindStateConflict,
				"old release recovery acknowledgement changed",
			)
		}
		return resolved, nil
	}
	if result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"release recovery acknowledgement epoch changed",
		)
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired {
		return resolved, nil
	}
	if record.Phase != taskassignments.ReleaseRecoveryPhaseProven || int(record.Cursor) != len(record.RecoveryStepIDs) {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"release recovery proof is incomplete",
		)
	}
	procedure, kinds, conditions, err := repository.recoveryProofSelectionAtRevision(ctx, task, assignment, revision)
	if err != nil {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"release recovery authority selection failed",
		)
	}
	if err := validateReleaseRecoveryProof(assignment, procedure, result, kinds); err != nil {
		return releaseRecoveryAcknowledgement{}, errs.Wrap(errs.KindStateConflict, err)
	}
	componentEffect, err := repository.releaseComponentEffectAtRevision(ctx, task, assignment, revision)
	if err != nil {
		return releaseRecoveryAcknowledgement{}, err
	}
	if componentEffect {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"Component effects remain unproven by workload restoration",
		)
	}
	primary := taskjournal.CloneTaskResult(&record.PrimaryResult)
	if primary == nil {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	primary.ReconciliationRequired = false
	primary.Projects = slices.Clone(result.Projects)
	primary.ProxyEvidence = slices.Clone(result.ProxyEvidence)
	primary.RecreateEvidence = slices.Clone(result.RecreateEvidence)
	primary.CandidateAbsenceEvidence = nil
	if result.CandidateAbsenceEvidence != nil {
		evidence := *result.CandidateAbsenceEvidence
		evidence.Candidates = slices.Clone(result.CandidateAbsenceEvidence.Candidates)
		primary.CandidateAbsenceEvidence = &evidence
	}
	primary.ExecutionEpoch = assignment.ExecutionEpoch
	primary.ReleaseRecoveryRecordSHA256 = assignment.ReleaseRecoveryRecordSHA256
	resolved.final, resolved.status, resolved.result = true, record.PrimaryStatus, *primary
	resolved.conditions = append(
		conditions,
		etcdstore.Condition{Key: taskassignments.ReleaseRecoveryKey(task.ID), ModRevision: read.Values[0].ModRevision},
	)
	return resolved, nil
}

func composeServiceReleaseID(service *agentpb.ComposeService) string {
	for _, label := range service.GetExpectedLabels() {
		if label.GetKey() == "com.groundplane.release-id" {
			return label.GetValue()
		}
	}
	return ""
}

func (repository *TaskRepository) normalizeReleaseRecoveryTerminalReplay(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	agentGeneration uint64,
	assignmentID string,
	revision int64,
) (taskjournal.TaskStatus, *taskjournal.TaskResultRecord, bool, error) {
	if result.ReleaseRecoveryRecordSHA256 == "" {
		return status, nil, false, nil
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired ||
		!recordcodec.ValidSHA256(result.ReleaseRecoveryRecordSHA256) {
		return status, nil, true, errs.New(errs.KindStateConflict, "terminal release recovery replay status changed")
	}
	if task.Result == nil || !taskassignments.ValidTerminalTaskStatus(task.Status) || task.TerminalAssignment == nil ||
		task.Result.ExecutionEpoch != result.ExecutionEpoch ||
		task.Result.ReleaseRecoveryRecordSHA256 != result.ReleaseRecoveryRecordSHA256 ||
		task.TerminalAssignment.AssignmentID != assignmentID || task.TerminalAssignment.AgentID != agentID ||
		task.TerminalAssignment.AgentGeneration != agentGeneration {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay authority changed",
		)
	}
	if !slices.Equal(task.Result.Projects, result.Projects) {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay project evidence changed",
		)
	}
	if !slices.Equal(task.Result.ProxyEvidence, result.ProxyEvidence) {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay proxy evidence changed",
		)
	}
	if !slices.Equal(task.Result.RecreateEvidence, result.RecreateEvidence) {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay recreate evidence changed",
		)
	}
	if !taskjournal.TaskCandidateAbsenceEvidenceEqual(task.Result.CandidateAbsenceEvidence, result.CandidateAbsenceEvidence) {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay absence evidence changed",
		)
	}
	keys := []string{taskassignments.ReleaseRecoveryKey(task.ID), taskjournal.TaskActiveOperationKey(task.OperationID)}
	steps, stepsErr := releaseHookExecutionSteps(task)
	if stepsErr != nil {
		return status, nil, true, stepsErr
	}
	if len(steps) != 0 {
		keys = append(keys, scriptSourceRootKey(task.OperationID))
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) {
		if err != nil {
			return status, nil, true, err
		}
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery record cleanup is unproven",
		)
	}
	for _, value := range read.Values {
		if value != nil {
			return status, nil, true, errs.New(
				errs.KindStateConflict,
				"terminal release recovery authority cleanup is unproven",
			)
		}
	}
	return task.Status, taskjournal.CloneTaskResult(task.Result), true, nil
}

func (repository *TaskRepository) candidateReleaseTimeoutResult(
	ctx context.Context,
	assignment TaskAssignment,
) (taskjournal.TaskResultRecord, bool, error) {
	record := assignment.Assignment.Record
	if assignment.Task.Record.Params[TaskReleasePublicationParam] == "" ||
		record.ExecutionMode != taskassignments.TaskExecutionModeForward {
		return taskjournal.TaskResultRecord{}, false, nil
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(
		ctx, assignment.Task.Record, assignment.Task.ReadRevision,
	)
	if err != nil || validateAssignmentRestorationDescriptor(assignment.Task.Record, record, procedure) != nil {
		return taskjournal.TaskResultRecord{}, true, taskassignments.CorruptTaskAssignment()
	}
	result := taskjournal.TaskResultRecord{
		Kind: taskjournal.TaskResultCompose, Diagnostic: taskjournal.TaskResultDiagnosticTimeoutBeforeEffect,
		ExecutionEpoch: record.ExecutionEpoch,
	}
	return result, true, nil
}

func (repository *TaskRepository) assignmentLifecycleIndexAtRevision(
	ctx context.Context,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue *etcdstore.KeyValue,
	revision int64,
) (string, *etcdstore.KeyValue, bool, error) {
	deadline := assignment.Deadline
	if assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly {
		deadline = assignment.RecoveryDeadline
	}
	timeoutKey := taskjournal.TaskTimeoutIndexKey(assignment.TaskID, deadline)
	proofKey := taskjournal.TaskRecoveryProofRequiredKey(assignment.TaskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{timeoutKey, proofKey}, Revision: revision})
	if err != nil {
		return "", nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return "", nil, false, taskassignments.CorruptTaskAssignment()
	}
	timeoutValue, proofValue := read.Values[0], read.Values[1]
	proofRequired := proofValue != nil
	if timeoutValue == nil == !proofRequired ||
		proofRequired && assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly {
		return "", nil, false, taskassignments.CorruptTaskAssignment()
	}
	selectedKey, selectedValue := timeoutKey, timeoutValue
	if proofRequired {
		selectedKey, selectedValue = proofKey, proofValue
	}
	if selectedValue.ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(selectedValue.Value, assignmentValue.Value) {
		return "", nil, false, taskassignments.CorruptTaskAssignment()
	}
	return selectedKey, selectedValue, proofRequired, nil
}

func (repository *TaskRepository) markReleaseRecoveryProofRequired(
	ctx context.Context,
	assignment taskassignments.TaskAssignmentRecord,
	revision int64,
	observedAt time.Time,
) (bool, error) {
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly || assignment.RestorationAuthority == nil ||
		revision <= 0 ||
		!observedAt.Equal(observedAt.UTC()) ||
		observedAt.Before(assignment.RecoveryDeadline) {
		return false, errs.New(errs.KindStateConflict, "release recovery deadline authority changed")
	}
	assignmentValue, err := taskassignments.EncodeTaskAssignment(assignment)
	if err != nil {
		return false, err
	}
	defer clear(assignmentValue)
	claimKey := taskjournal.TaskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID)
	indexKey := taskjournal.TaskAssignmentIndexKey(assignment.TaskID)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(assignment.TaskID, assignment.RecoveryDeadline)
	proofKey := taskjournal.TaskRecoveryProofRequiredKey(assignment.TaskID)
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskjournal.TaskStorageKey(assignment.TaskID), claimKey, indexKey, timeoutKey, proofKey, taskassignments.ReleaseRecoveryKey(assignment.TaskID),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 6 || read.Values[0] == nil ||
		read.Values[1] == nil || read.Values[2] == nil || read.Values[5] == nil {
		return false, taskassignments.CorruptTaskAssignment()
	}
	if read.Values[3] == nil {
		if read.Values[4] != nil && bytes.Equal(read.Values[4].Value, assignmentValue) {
			return false, nil
		}
		return false, errs.New(errs.KindStateConflict, "release recovery deadline authority changed")
	}
	if read.Values[4] != nil || read.Values[1].ModRevision != read.Values[2].ModRevision ||
		read.Values[1].ModRevision != read.Values[3].ModRevision ||
		!bytes.Equal(read.Values[1].Value, assignmentValue) || !bytes.Equal(read.Values[2].Value, assignmentValue) ||
		!bytes.Equal(read.Values[3].Value, assignmentValue) {
		return false, taskassignments.CorruptTaskAssignment()
	}
	task, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != assignment.TaskID || task.OperationID != assignment.RestorationAuthority.OperationID ||
		task.PlanHash != assignment.RestorationAuthority.PlanHash || task.Status != taskjournal.TaskStatusRunning {
		return false, taskassignments.CorruptTaskAssignment()
	}
	recovery, err := taskassignments.DecodeReleaseRecoveryRecord(read.Values[5].Value)
	if err != nil || recovery.TaskID != task.ID || recovery.AssignmentID != assignment.AssignmentID ||
		recovery.OperationID != task.OperationID || recovery.PlanHash != task.PlanHash ||
		recovery.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!recovery.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return false, taskassignments.CorruptTaskAssignment()
	}
	digest, err := taskassignments.ReleaseRecoveryRecordSHA256(recovery)
	if err != nil || digest != assignment.ReleaseRecoveryRecordSHA256 {
		return false, taskassignments.CorruptTaskAssignment()
	}
	next := assignment
	next.RecoveryExecutionDeadline = observedAt.Add(releaseRecoveryProofExecutionBudget)
	nextValue, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return false, err
	}
	defer clear(nextValue)
	transaction, err := repository.store.Transact(ctx, []etcdstore.Condition{
		{Key: taskjournal.TaskStorageKey(task.ID), ModRevision: read.Values[0].ModRevision},
		{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		{Key: indexKey, ModRevision: read.Values[2].ModRevision},
		{Key: timeoutKey, ModRevision: read.Values[3].ModRevision},
		{Key: proofKey},
		{Key: taskassignments.ReleaseRecoveryKey(task.ID), ModRevision: read.Values[5].ModRevision},
	}, []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: read.Values[0].Value},
		{Type: etcdstore.MutationPut, Key: claimKey, Value: nextValue},
		{Type: etcdstore.MutationPut, Key: indexKey, Value: nextValue},
		{Type: etcdstore.MutationDelete, Key: timeoutKey},
		{Type: etcdstore.MutationPut, Key: proofKey, Value: nextValue},
	})
	if err != nil {
		return false, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	return transaction.Succeeded, nil
}
