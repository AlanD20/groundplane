package etcd

import (
	"bytes"
	"context"
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
	taskValue *KeyValue,
	assignment TaskAssignmentRecord,
	assignmentValue *KeyValue,
	assignmentIndexValue *KeyValue,
	status TaskStatus,
	result TaskResultRecord,
	revision int64,
	evidenceConditions ...Condition,
) (Versioned[TaskRecord], bool, error) {
	if task.Params[TaskReleasePublicationParam] == "" || !result.ReconciliationRequired {
		return Versioned[TaskRecord]{}, false, nil
	}
	if assignment.ExecutionMode != TaskExecutionModeForward || result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != "" || assignment.ExecutionEpoch == math.MaxUint32 ||
		assignment.RestorationAuthority == nil {
		return Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"release recovery acknowledgement authority changed",
		)
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(ctx, task, revision)
	if err != nil || validateAssignmentRestorationDescriptor(task, assignment, procedure) != nil {
		return Versioned[TaskRecord]{}, true, corruptTaskAssignment()
	}
	stepIDs, err := releaseRestorationStepIDs(procedure, assignment.RestorationAuthority.Candidates)
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	reportDigest, err := canonicalPrimaryReportSHA256(status, result)
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	_, mutationEvidence, err := repository.releaseCandidateMutationEvidenceAtRevision(
		ctx,
		task,
		assignment,
		procedure,
		revision,
	)
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	record := releaseRecoveryRecord{
		Schema: 1, TaskID: task.ID, AssignmentID: assignment.AssignmentID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, RestorationAuthoritySHA256: assignment.RestorationAuthoritySHA256,
		PrimaryReportSHA256: reportDigest, PrimaryStatus: status, PrimaryResult: result,
		RecoveryDeadline: assignment.RecoveryDeadline,
		MutationEvidence: mutationEvidence,
		RecoveryStepIDs:  stepIDs, Cursor: 0, Phase: ReleaseRecoveryPhaseProbe, EvidenceRevision: revision,
	}
	recoveryValue, err := encodeReleaseRecoveryRecord(record)
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	defer clear(recoveryValue)
	recoveryDigest, err := releaseRecoveryRecordSHA256(record)
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	next := assignment
	next.ExecutionMode = TaskExecutionModeRecoveryOnly
	next.ExecutionEpoch++
	next.ReleaseRecoveryRecordSHA256 = recoveryDigest
	nextValue, err := encodeTaskAssignment(next)
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	defer clear(nextValue)
	timeoutKey := taskTimeoutIndexKey(task.ID, assignment.Deadline)
	timeoutRead, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{timeoutKey}, Revision: revision})
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	if timeoutRead == nil || timeoutRead.ReadRevision != revision || len(timeoutRead.Values) != 1 ||
		timeoutRead.Values[0] == nil || timeoutRead.Values[0].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(timeoutRead.Values[0].Value, assignmentValue.Value) {
		return Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"release recovery timeout authority changed",
		)
	}
	conditions := []Condition{
		{Key: taskKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: assignmentValue.Key, ModRevision: assignmentValue.ModRevision},
		{Key: assignmentIndexValue.Key, ModRevision: assignmentIndexValue.ModRevision},
		{Key: timeoutKey, ModRevision: timeoutRead.Values[0].ModRevision},
		{Key: releaseRecoveryKey(task.ID)},
		{Key: taskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline)},
		{Key: taskRecoveryProofRequiredKey(task.ID)},
	}
	conditions = append(conditions, evidenceConditions...)
	transaction, err := repository.store.Transact(ctx, conditions, []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: taskValue.Value},
		{Type: MutationPut, Key: assignmentValue.Key, Value: nextValue},
		{Type: MutationPut, Key: assignmentIndexValue.Key, Value: nextValue},
		{Type: MutationDelete, Key: timeoutKey},
		{Type: MutationPut, Key: taskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline), Value: nextValue},
		{Type: MutationPut, Key: releaseRecoveryKey(task.ID), Value: recoveryValue},
	})
	if err != nil {
		return Versioned[TaskRecord]{}, true, err
	}
	clearKeyValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return Versioned[TaskRecord]{}, false, nil
	}
	return Versioned[TaskRecord]{
		Record:       task,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, true, nil
}

func (repository *TaskRepository) releaseEffectEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) (bool, []Condition, error) {
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

func (repository *TaskRepository) releaseCandidateMutationEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) (bool, []releaseRecoveryMutationEvidence, error) {
	mutationSteps := make(map[string]int)
	ordered := make([]string, 0)
	for _, member := range procedure.GetMembers() {
		for _, stepID := range member.GetForwardStepIds() {
			if _, exists := mutationSteps[stepID]; !exists {
				mutationSteps[stepID] = len(ordered)
				ordered = append(ordered, stepID)
			}
		}
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil || snapshot.Revision != revision || snapshot.Task.EventCount != task.EventCount {
		if err != nil {
			return false, nil, err
		}
		return false, nil, corruptTaskAssignment()
	}
	evidenceByStep := make(map[string]releaseRecoveryMutationEvidence)
	for _, event := range snapshot.Events {
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration || event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return false, nil, corruptTaskAssignment()
		}
		if _, mutation := mutationSteps[event.Identity.StepID]; mutation && event.State != TaskEventStatePending {
			evidence := evidenceByStep[event.Identity.StepID]
			evidence.StepID, evidence.Running = event.Identity.StepID, true
			if event.State == TaskEventStateCompleted {
				evidence.Completed = true
			}
			evidenceByStep[event.Identity.StepID] = evidence
		}
	}
	evidence := make([]releaseRecoveryMutationEvidence, 0, len(evidenceByStep))
	for _, stepID := range ordered {
		if item, ok := evidenceByStep[stepID]; ok {
			evidence = append(evidence, item)
		}
	}
	return len(evidence) != 0, evidence, nil
}

func (repository *TaskRepository) releaseRecoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	status TaskStatus,
	result TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	read, err := repository.store.GetMany(
		ctx,
		GetManyRequest{Keys: []string{releaseRecoveryKey(task.ID)}, Revision: revision},
	)
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if err != nil {
			return releaseRecoveryAcknowledgement{}, err
		}
		return releaseRecoveryAcknowledgement{}, corruptTaskAssignment()
	}
	record, err := decodeReleaseRecoveryRecord(read.Values[0].Value)
	digest, digestErr := releaseRecoveryRecordSHA256(record)
	if err != nil || digestErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
		record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID ||
		record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		record.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return releaseRecoveryAcknowledgement{}, corruptTaskAssignment()
	}
	resolved := releaseRecoveryAcknowledgement{record: record, value: read.Values[0]}
	if result.ExecutionEpoch < assignment.ExecutionEpoch && result.ReleaseRecoveryRecordSHA256 == "" {
		replayDigest, replayErr := canonicalPrimaryReportSHA256(status, result)
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
	if status != TaskStatusCompleted || result.ReconciliationRequired {
		return resolved, nil
	}
	if record.Phase != ReleaseRecoveryPhaseProven || int(record.Cursor) != len(record.RecoveryStepIDs) {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"release recovery proof is incomplete",
		)
	}
	procedure, kinds, conditions, err := repository.recoveryProofSelectionAtRevision(ctx, task, assignment, revision)
	if err != nil || validateReleaseRecoveryProof(assignment, procedure, result, kinds) != nil {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"release recovery proof does not match selected authority",
		)
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
	primary := cloneTaskResult(&record.PrimaryResult)
	if primary == nil {
		return releaseRecoveryAcknowledgement{}, corruptTaskAssignment()
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
		Condition{Key: releaseRecoveryKey(task.ID), ModRevision: read.Values[0].ModRevision},
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
	status TaskStatus,
	result TaskResultRecord,
	agentID string,
	agentGeneration uint64,
	assignmentID string,
	revision int64,
) (TaskStatus, *TaskResultRecord, bool, error) {
	if result.ReleaseRecoveryRecordSHA256 == "" {
		return status, nil, false, nil
	}
	if status != TaskStatusCompleted || result.ReconciliationRequired ||
		!validSHA256(result.ReleaseRecoveryRecordSHA256) {
		return status, nil, true, errs.New(errs.KindStateConflict, "terminal release recovery replay status changed")
	}
	if task.Result == nil || !validTerminalTaskStatus(task.Status) || task.TerminalAssignment == nil ||
		task.TerminalAssignment.AssignmentID != assignmentID || task.TerminalAssignment.AgentID != agentID ||
		task.TerminalAssignment.AgentGeneration != agentGeneration {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay assignment changed",
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
	if !taskCandidateAbsenceEvidenceEqual(task.Result.CandidateAbsenceEvidence, result.CandidateAbsenceEvidence) {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal release recovery replay absence evidence changed",
		)
	}
	keys := []string{releaseRecoveryKey(task.ID), taskActiveOperationKey(task.OperationID)}
	steps, stepsErr := releaseHookExecutionSteps(task)
	if stepsErr != nil {
		return status, nil, true, stepsErr
	}
	if len(steps) != 0 {
		keys = append(keys, scriptSourceRootKey(task.OperationID))
	}
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: keys, Revision: revision})
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
	return task.Status, cloneTaskResult(task.Result), true, nil
}

func (repository *TaskRepository) candidateReleaseTimeoutResult(
	ctx context.Context,
	assignment TaskAssignment,
) (TaskResultRecord, bool, error) {
	record := assignment.Assignment.Record
	if assignment.Task.Record.Params[TaskReleasePublicationParam] == "" ||
		record.ExecutionMode != TaskExecutionModeForward {
		return TaskResultRecord{}, false, nil
	}
	_, procedure, err := repository.candidateReleaseDescriptorAtRevision(
		ctx, assignment.Task.Record, assignment.Task.ReadRevision,
	)
	if err != nil || validateAssignmentRestorationDescriptor(assignment.Task.Record, record, procedure) != nil {
		return TaskResultRecord{}, true, corruptTaskAssignment()
	}
	result := TaskResultRecord{
		Kind: TaskResultCompose, Diagnostic: TaskResultDiagnosticTimeoutBeforeEffect,
		ExecutionEpoch: record.ExecutionEpoch,
	}
	return result, true, nil
}

func (repository *TaskRepository) assignmentLifecycleIndexAtRevision(
	ctx context.Context,
	assignment TaskAssignmentRecord,
	assignmentValue *KeyValue,
	revision int64,
) (string, *KeyValue, bool, error) {
	deadline := assignment.Deadline
	if assignment.ExecutionMode == TaskExecutionModeRecoveryOnly {
		deadline = assignment.RecoveryDeadline
	}
	timeoutKey := taskTimeoutIndexKey(assignment.TaskID, deadline)
	proofKey := taskRecoveryProofRequiredKey(assignment.TaskID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{timeoutKey, proofKey}, Revision: revision})
	if err != nil {
		return "", nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		return "", nil, false, corruptTaskAssignment()
	}
	timeoutValue, proofValue := read.Values[0], read.Values[1]
	proofRequired := proofValue != nil
	if timeoutValue == nil == !proofRequired ||
		proofRequired && assignment.ExecutionMode != TaskExecutionModeRecoveryOnly {
		return "", nil, false, corruptTaskAssignment()
	}
	selectedKey, selectedValue := timeoutKey, timeoutValue
	if proofRequired {
		selectedKey, selectedValue = proofKey, proofValue
	}
	if selectedValue.ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(selectedValue.Value, assignmentValue.Value) {
		return "", nil, false, corruptTaskAssignment()
	}
	return selectedKey, selectedValue, proofRequired, nil
}

func (repository *TaskRepository) markReleaseRecoveryProofRequired(
	ctx context.Context,
	assignment TaskAssignmentRecord,
	revision int64,
	observedAt time.Time,
) (bool, error) {
	if assignment.ExecutionMode != TaskExecutionModeRecoveryOnly || assignment.RestorationAuthority == nil ||
		revision <= 0 ||
		!observedAt.Equal(observedAt.UTC()) ||
		observedAt.Before(assignment.RecoveryDeadline) {
		return false, errs.New(errs.KindStateConflict, "release recovery deadline authority changed")
	}
	assignmentValue, err := encodeTaskAssignment(assignment)
	if err != nil {
		return false, err
	}
	defer clear(assignmentValue)
	claimKey := taskExecutionClaimKey(assignment.Executor, assignment.AgentID, assignment.TaskID)
	indexKey := taskAssignmentIndexKey(assignment.TaskID)
	timeoutKey := taskTimeoutIndexKey(assignment.TaskID, assignment.RecoveryDeadline)
	proofKey := taskRecoveryProofRequiredKey(assignment.TaskID)
	read, err := repository.store.GetMany(ctx, GetManyRequest{Keys: []string{
		taskKey(assignment.TaskID), claimKey, indexKey, timeoutKey, proofKey, releaseRecoveryKey(assignment.TaskID),
	}, Revision: revision})
	if err != nil {
		return false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 6 || read.Values[0] == nil ||
		read.Values[1] == nil || read.Values[2] == nil || read.Values[5] == nil {
		return false, corruptTaskAssignment()
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
		return false, corruptTaskAssignment()
	}
	task, err := decodeTaskRecord(read.Values[0].Value)
	if err != nil || task.ID != assignment.TaskID || task.OperationID != assignment.RestorationAuthority.OperationID ||
		task.PlanHash != assignment.RestorationAuthority.PlanHash || task.Status != TaskStatusRunning {
		return false, corruptTaskAssignment()
	}
	recovery, err := decodeReleaseRecoveryRecord(read.Values[5].Value)
	if err != nil || recovery.TaskID != task.ID || recovery.AssignmentID != assignment.AssignmentID ||
		recovery.OperationID != task.OperationID || recovery.PlanHash != task.PlanHash ||
		recovery.RestorationAuthoritySHA256 != assignment.RestorationAuthoritySHA256 ||
		!recovery.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return false, corruptTaskAssignment()
	}
	digest, err := releaseRecoveryRecordSHA256(recovery)
	if err != nil || digest != assignment.ReleaseRecoveryRecordSHA256 {
		return false, corruptTaskAssignment()
	}
	next := assignment
	next.RecoveryExecutionDeadline = observedAt.Add(releaseRecoveryProofExecutionBudget)
	nextValue, err := encodeTaskAssignment(next)
	if err != nil {
		return false, err
	}
	defer clear(nextValue)
	transaction, err := repository.store.Transact(ctx, []Condition{
		{Key: taskKey(task.ID), ModRevision: read.Values[0].ModRevision},
		{Key: claimKey, ModRevision: read.Values[1].ModRevision},
		{Key: indexKey, ModRevision: read.Values[2].ModRevision},
		{Key: timeoutKey, ModRevision: read.Values[3].ModRevision},
		{Key: proofKey},
		{Key: releaseRecoveryKey(task.ID), ModRevision: read.Values[5].ModRevision},
	}, []Mutation{
		{Type: MutationPut, Key: taskKey(task.ID), Value: read.Values[0].Value},
		{Type: MutationPut, Key: claimKey, Value: nextValue},
		{Type: MutationPut, Key: indexKey, Value: nextValue},
		{Type: MutationDelete, Key: timeoutKey},
		{Type: MutationPut, Key: proofKey, Value: nextValue},
	})
	if err != nil {
		return false, err
	}
	clearKeyValues(transaction.FailureReads)
	return transaction.Succeeded, nil
}
