package etcd

import (
	"bytes"
	"context"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) recoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	if assignment.ComponentRecoveryProcedureSHA256 != "" {
		return repository.componentRecoveryAcknowledgementAtRevision(ctx, task, assignment, status, result, revision)
	}
	return repository.releaseRecoveryAcknowledgementAtRevision(ctx, task, assignment, status, result, revision)
}

func (repository *TaskRepository) normalizeRecoveryTerminalReplay(
	ctx context.Context,
	task TaskRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	agentID string,
	agentGeneration uint64,
	assignmentID string,
	revision int64,
) (taskjournal.TaskStatus, *taskjournal.TaskResultRecord, bool, error) {
	if task.Params[taskjournal.TaskBlueprintComponentUnitParam] == "" {
		return repository.normalizeReleaseRecoveryTerminalReplay(
			ctx, task, status, result, agentID, agentGeneration, assignmentID, revision,
		)
	}
	if result.ReleaseRecoveryRecordSHA256 == "" {
		return status, nil, false, nil
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired || task.Result == nil ||
		!taskassignments.ValidTerminalTaskStatus(task.Status) || task.TerminalAssignment == nil ||
		task.Result.ExecutionEpoch != result.ExecutionEpoch ||
		task.Result.ReleaseRecoveryRecordSHA256 != result.ReleaseRecoveryRecordSHA256 ||
		task.TerminalAssignment.AssignmentID != assignmentID || task.TerminalAssignment.AgentID != agentID ||
		task.TerminalAssignment.AgentGeneration != agentGeneration ||
		!slices.Equal(task.Result.Projects, result.Projects) ||
		!taskjournal.TaskComponentRestorationEvidenceEqual(
			task.Result.ComponentRestorationEvidence, result.ComponentRestorationEvidence,
		) {
		return status, nil, true, errs.New(errs.KindStateConflict, "terminal Component recovery replay authority changed")
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{
		taskassignments.ComponentRecoveryKey(task.ID), taskjournal.TaskActiveOperationKey(task.OperationID),
	}, Revision: revision})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if err != nil {
			return status, nil, true, err
		}
		return status, nil, true, errs.New(errs.KindStateConflict, "terminal Component recovery cleanup is unproven")
	}
	if read.Values[0] != nil || read.Values[1] != nil {
		return status, nil, true, errs.New(errs.KindStateConflict, "terminal Component recovery authority remains")
	}
	return task.Status, taskjournal.CloneTaskResult(task.Result), true, nil
}

func (repository *TaskRepository) transitionComponentAcknowledgementToRecovery(
	ctx context.Context,
	task TaskRecord,
	taskValue *etcdstore.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue, assignmentIndexValue *etcdstore.KeyValue,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (etcdstore.Versioned[TaskRecord], bool, error) {
	if task.Params[taskjournal.TaskBlueprintComponentUnitParam] == "" {
		return etcdstore.Versioned[TaskRecord]{}, false, nil
	}
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeForward ||
		!taskassignments.ValidTerminalTaskStatus(status) ||
		result.ExecutionEpoch != assignment.ExecutionEpoch || result.ReleaseRecoveryRecordSHA256 != "" ||
		assignment.ExecutionEpoch == math.MaxUint32 || assignment.ComponentRecoveryProcedureSHA256 == "" ||
		assignment.RestorationAuthority != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, errs.New(errs.KindStateConflict, "Component recovery acknowledgement authority changed")
	}
	_, procedure, childCondition, err := repository.componentRecoveryProcedureAtRevision(ctx, task, revision)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	digest, err := componentRecoveryAuthorityDigest(procedure)
	if err != nil || digest != assignment.ComponentRecoveryProcedureSHA256 {
		return etcdstore.Versioned[TaskRecord]{}, true, taskassignments.CorruptTaskAssignment()
	}
	mutationEvidence, err := repository.componentMutationEvidenceAtRevision(ctx, task, assignment, procedure, revision)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	reportDigest, err := taskassignments.CanonicalPrimaryReportSHA256(status, result)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	record := taskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: task.ID, AssignmentID: assignment.AssignmentID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, RestorationAuthoritySHA256: digest,
		PrimaryReportSHA256: reportDigest, PrimaryStatus: status, PrimaryResult: result,
		RecoveryDeadline: assignment.RecoveryDeadline, MutationEvidence: mutationEvidence,
		RecoveryStepIDs: executionplan.ComponentRecoveryStepIDs(procedure), Cursor: 0,
		Phase: taskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: revision,
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
	next.ExecutionMode, next.ExecutionEpoch = taskassignments.TaskExecutionModeRecoveryOnly, assignment.ExecutionEpoch+1
	next.ReleaseRecoveryRecordSHA256 = recoveryDigest
	nextValue, err := taskassignments.EncodeTaskAssignment(next)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	defer clear(nextValue)
	timeoutKey := taskjournal.TaskTimeoutIndexKey(task.ID, assignment.Deadline)
	timeoutRead, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{Keys: []string{timeoutKey}, Revision: revision})
	if err != nil || timeoutRead == nil || timeoutRead.ReadRevision != revision || len(timeoutRead.Values) != 1 ||
		timeoutRead.Values[0] == nil || timeoutRead.Values[0].ModRevision != assignmentValue.ModRevision ||
		!bytes.Equal(timeoutRead.Values[0].Value, assignmentValue.Value) {
		if err != nil {
			return etcdstore.Versioned[TaskRecord]{}, true, err
		}
		return etcdstore.Versioned[TaskRecord]{}, true, errs.New(errs.KindStateConflict, "Component recovery timeout authority changed")
	}
	conditions := []etcdstore.Condition{
		{Key: taskClosingReportKey(task)}, {Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: assignmentValue.Key, ModRevision: assignmentValue.ModRevision},
		{Key: assignmentIndexValue.Key, ModRevision: assignmentIndexValue.ModRevision},
		{Key: timeoutKey, ModRevision: timeoutRead.Values[0].ModRevision}, childCondition,
		{Key: taskassignments.ComponentRecoveryKey(task.ID)},
		{Key: taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline)},
		{Key: taskjournal.TaskRecoveryProofRequiredKey(task.ID)},
	}
	claim, err := repository.prepareBlueprintChildAssignmentEpoch(ctx, task, assignment.ExecutionEpoch, true)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	defer claim.Clear()
	conditions = append(conditions, claim.Conditions()...)
	mutations := []etcdstore.Mutation{
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskStorageKey(task.ID), Value: taskValue.Value},
		{Type: etcdstore.MutationPut, Key: assignmentValue.Key, Value: nextValue},
		{Type: etcdstore.MutationPut, Key: assignmentIndexValue.Key, Value: nextValue},
		{Type: etcdstore.MutationDelete, Key: timeoutKey},
		{Type: etcdstore.MutationPut, Key: taskjournal.TaskTimeoutIndexKey(task.ID, assignment.RecoveryDeadline), Value: nextValue},
		{Type: etcdstore.MutationPut, Key: taskassignments.ComponentRecoveryKey(task.ID), Value: recoveryValue},
	}
	mutations = append(mutations, claim.Mutations()...)
	transaction, err := repository.store.Transact(ctx, conditions, mutations)
	if err != nil {
		return etcdstore.Versioned[TaskRecord]{}, true, err
	}
	etcdstore.ClearValues(transaction.FailureReads)
	if !transaction.Succeeded {
		return etcdstore.Versioned[TaskRecord]{}, false, nil
	}
	return etcdstore.Versioned[TaskRecord]{Record: task, Revision: transaction.Revision, ReadRevision: transaction.Revision}, true, nil
}

func (repository *TaskRepository) componentMutationEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.ComponentRecoveryProcedure,
	revision int64,
) ([]taskassignments.ReleaseRecoveryMutationEvidence, error) {
	selected := make(map[string]struct{}, len(procedure.GetForwardStepIds()))
	files := make(map[string]struct{})
	for _, stepID := range procedure.GetForwardStepIds() {
		selected[stepID] = struct{}{}
	}
	for _, file := range procedure.GetConfigurationRestoration().GetFiles() {
		files[file.GetForwardStepId()] = struct{}{}
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil || snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		if err != nil {
			return nil, err
		}
		return nil, taskassignments.CorruptTaskAssignment()
	}
	byStep := make(map[string]taskassignments.ReleaseRecoveryMutationEvidence)
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if !taskCheckpointAssignmentMatches(checkpoint, assignment) {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if _, ok := selected[checkpoint.Identity.StepID]; !ok {
			continue
		}
		_, file := files[checkpoint.Identity.StepID]
		if checkpoint.Running || !file && checkpoint.EffectPossible {
			byStep[checkpoint.Identity.StepID] = taskassignments.ReleaseRecoveryMutationEvidence{
				StepID: checkpoint.Identity.StepID, Running: true, Completed: checkpoint.Completed,
			}
		}
	}
	for _, event := range snapshot.Events {
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration || event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if _, ok := selected[event.Identity.StepID]; !ok {
			continue
		}
		evidence := byStep[event.Identity.StepID]
		_, file := files[event.Identity.StepID]
		switch event.State {
		case taskjournal.TaskEventStateRunning:
			evidence.StepID, evidence.Running = event.Identity.StepID, true
		case taskjournal.TaskEventStateCompleted:
			evidence.StepID, evidence.Running, evidence.Completed = event.Identity.StepID, true, true
		case taskjournal.TaskEventStateFailed, taskjournal.TaskEventStateAborted, taskjournal.TaskEventStateTimedOut:
			if !file {
				evidence.StepID, evidence.Running = event.Identity.StepID, true
			}
		}
		if evidence.Running {
			byStep[event.Identity.StepID] = evidence
		}
	}
	result := make([]taskassignments.ReleaseRecoveryMutationEvidence, 0, len(byStep))
	for _, stepID := range procedure.GetForwardStepIds() {
		if evidence, ok := byStep[stepID]; ok {
			result = append(result, evidence)
		}
	}
	return result, nil
}

func componentApplicableCompensationStepIDs(
	procedure *agentpb.ComponentRecoveryProcedure,
	evidence []taskassignments.ReleaseRecoveryMutationEvidence,
) ([]string, error) {
	running := make(map[string]bool, len(evidence))
	evidenceIndex := 0
	for _, stepID := range procedure.GetForwardStepIds() {
		if evidenceIndex >= len(evidence) || evidence[evidenceIndex].StepID != stepID {
			continue
		}
		if !evidence[evidenceIndex].Running {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		running[stepID] = true
		evidenceIndex++
	}
	if evidenceIndex != len(evidence) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	result := make([]string, 0, len(procedure.GetConfigurationRestoration().GetFiles())+1)
	for _, file := range procedure.GetConfigurationRestoration().GetFiles() {
		if running[file.GetForwardStepId()] {
			result = append(result, file.GetCompensateStepId())
		}
	}
	if len(running) != 0 {
		compensate := ""
		if prior := procedure.GetPredecessor(); prior != nil {
			compensate = prior.GetCompensateStepId()
		} else {
			compensate = procedure.GetCandidateAbsence().GetCompensateStepId()
		}
		result = append(result, compensate)
	}
	return result, nil
}

func (repository *TaskRepository) componentRecoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskassignments.ComponentRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if err != nil {
			return releaseRecoveryAcknowledgement{}, err
		}
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeReleaseRecoveryRecord(read.Values[0].Value)
	digest, digestErr := taskassignments.ReleaseRecoveryRecordSHA256(record)
	if err != nil || digestErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
		record.RestorationAuthoritySHA256 != assignment.ComponentRecoveryProcedureSHA256 ||
		record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID || record.OperationID != task.OperationID ||
		record.PlanHash != task.PlanHash || !record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	resolved := releaseRecoveryAcknowledgement{record: record, value: read.Values[0]}
	if result.ExecutionEpoch < assignment.ExecutionEpoch && result.ReleaseRecoveryRecordSHA256 == "" {
		replayDigest, replayErr := taskassignments.CanonicalPrimaryReportSHA256(status, result)
		if replayErr != nil || replayDigest != record.PrimaryReportSHA256 {
			return releaseRecoveryAcknowledgement{}, errs.New(errs.KindStateConflict, "old Component recovery acknowledgement changed")
		}
		return resolved, nil
	}
	if result.ExecutionEpoch != assignment.ExecutionEpoch || result.ReleaseRecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 {
		return releaseRecoveryAcknowledgement{}, errs.New(errs.KindStateConflict, "Component recovery acknowledgement epoch changed")
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired {
		return resolved, nil
	}
	if record.Phase != taskassignments.ReleaseRecoveryPhaseProven || int(record.Cursor) != len(record.RecoveryStepIDs) {
		return releaseRecoveryAcknowledgement{}, errs.New(errs.KindStateConflict, "Component recovery proof is incomplete")
	}
	plan, procedure, childCondition, err := repository.componentRecoveryProcedureAtRevision(ctx, task, revision)
	_, evidenceErr := componentApplicableCompensationStepIDs(procedure, record.MutationEvidence)
	if err != nil || componentRecoveryAuthorityDigestMatches(procedure, assignment.ComponentRecoveryProcedureSHA256) != nil ||
		evidenceErr != nil ||
		validateComponentRecoveryProof(plan, procedure, result) != nil {
		return releaseRecoveryAcknowledgement{}, errs.New(errs.KindStateConflict, "Component recovery proof is invalid")
	}
	primary := taskjournal.CloneTaskResult(&record.PrimaryResult)
	if primary == nil {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	primary.ReconciliationRequired = false
	primary.Projects = slices.Clone(result.Projects)
	primary.ComponentRestorationEvidence = taskjournal.CloneTaskComponentRestorationEvidence(result.ComponentRestorationEvidence)
	primary.ExecutionEpoch = assignment.ExecutionEpoch
	primary.ReleaseRecoveryRecordSHA256 = assignment.ReleaseRecoveryRecordSHA256
	resolved.final, resolved.status, resolved.result = true, record.PrimaryStatus, *primary
	resolved.conditions = []etcdstore.Condition{childCondition,
		{Key: taskassignments.ComponentRecoveryKey(task.ID), ModRevision: read.Values[0].ModRevision}}
	return resolved, nil
}

func componentRecoveryAuthorityDigestMatches(procedure *agentpb.ComponentRecoveryProcedure, expected string) error {
	digest, err := componentRecoveryAuthorityDigest(procedure)
	if err != nil || digest != expected {
		return taskassignments.CorruptTaskAssignment()
	}
	return nil
}

func validateComponentRecoveryProof(
	plan *agentpb.ExecutionPlan,
	procedure *agentpb.ComponentRecoveryProcedure,
	result taskjournal.TaskResultRecord,
) error {
	evidence := result.ComponentRestorationEvidence
	if evidence == nil || !evidence.Restored || evidence.ComponentID != procedure.GetComponentId() ||
		result.CandidateAbsenceEvidence != nil || len(result.ProxyEvidence) != 0 || len(result.RecreateEvidence) != 0 ||
		len(result.BackingObservations) != 0 || result.DNSResolverCandidateObservation != nil ||
		result.DNSResolverRollbackObservation != nil {
		return taskassignments.CorruptTaskAssignment()
	}
	expectedArtifact, expectedService, expectedProject, expectedTarget := "", "", "", ""
	if prior := procedure.GetPredecessor(); prior != nil {
		expectedArtifact, expectedService, expectedTarget = prior.GetPriorArtifactId(), prior.GetServiceId(), "predecessor"
		for _, artifact := range plan.GetArtifacts() {
			if artifact.GetArtifactId() == expectedArtifact {
				expectedProject = artifact.GetProjectName()
			}
		}
	} else {
		absence := procedure.GetCandidateAbsence()
		expectedArtifact, expectedService = procedure.GetCandidateArtifactId(), absence.GetServiceId()
		expectedProject, expectedTarget = absence.GetComposeProjectName(), "candidate_absence"
	}
	if evidence.ArtifactID != expectedArtifact || evidence.ServiceID != expectedService || evidence.Target != expectedTarget {
		return taskassignments.CorruptTaskAssignment()
	}
	if expectedProject == "" || evidence.ComposeProjectName != expectedProject {
		return taskassignments.CorruptTaskAssignment()
	}
	if len(result.Projects) != 1 || result.Projects[0].ProjectName != evidence.ComposeProjectName ||
		result.Projects[0].CollisionCount != 0 {
		return taskassignments.CorruptTaskAssignment()
	}
	return nil
}
