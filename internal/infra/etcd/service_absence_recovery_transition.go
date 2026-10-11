package etcd

import (
	"bytes"
	"context"
	"math"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *TaskRepository) transitionServiceAbsenceAcknowledgementToRecovery(
	ctx context.Context,
	task TaskRecord,
	taskValue *keyvalue.KeyValue,
	assignment taskassignments.TaskAssignmentRecord,
	assignmentValue, assignmentIndexValue *keyvalue.KeyValue,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (keyvalue.Versioned[TaskRecord], bool, error) {
	if assignment.ServiceAbsenceProcedureSHA256 == "" || !result.ReconciliationRequired {
		return keyvalue.Versioned[TaskRecord]{}, false, nil
	}
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeForward ||
		!taskassignments.ValidTerminalTaskStatus(status) || result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != "" || assignment.ExecutionEpoch == math.MaxUint32 ||
		assignment.RestorationAuthority != nil || assignment.ComponentRecoveryProcedureSHA256 != "" {
		return keyvalue.Versioned[TaskRecord]{}, true, errs.New(
			errs.KindStateConflict,
			"Service absence recovery authority changed",
		)
	}
	procedure, _, sourceConditions, found, err := repository.serviceAbsenceProcedureAtRevision(ctx, task, revision)
	if err != nil || !found {
		if err != nil {
			return keyvalue.Versioned[TaskRecord]{}, true, err
		}
		return keyvalue.Versioned[TaskRecord]{}, true, taskassignments.CorruptTaskAssignment()
	}
	digest, err := serviceAbsenceAuthorityDigest(procedure)
	if err != nil || digest != assignment.ServiceAbsenceProcedureSHA256 {
		return keyvalue.Versioned[TaskRecord]{}, true, taskassignments.CorruptTaskAssignment()
	}
	mutationEvidence, err := repository.serviceAbsenceMutationEvidenceAtRevision(
		ctx,
		task,
		assignment,
		procedure,
		revision,
	)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	reportDigest, err := taskassignments.CanonicalPrimaryReportSHA256(status, result)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	record := taskassignments.ReleaseRecoveryRecord{
		Schema: 1, TaskID: task.ID, AssignmentID: assignment.AssignmentID, OperationID: task.OperationID,
		PlanHash: task.PlanHash, RestorationAuthoritySHA256: digest,
		PrimaryReportSHA256: reportDigest, PrimaryStatus: status, PrimaryResult: result,
		RecoveryDeadline: assignment.RecoveryDeadline, MutationEvidence: mutationEvidence,
		RecoveryStepIDs: executionplan.ServiceAbsenceRecoveryStepIDs(procedure), Cursor: 0,
		Phase: taskassignments.ReleaseRecoveryPhaseProbe, EvidenceRevision: revision,
	}
	recoveryValue, err := taskassignments.EncodeReleaseRecoveryRecord(record)
	if err != nil {
		return keyvalue.Versioned[TaskRecord]{}, true, err
	}
	defer clear(recoveryValue)
	recoveryDigest, err := taskassignments.ReleaseRecoveryRecordSHA256(record)
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
			"Service absence recovery timeout authority changed",
		)
	}
	conditions := []keyvalue.Condition{
		{
			Key: taskClosingReportKey(task),
		}, {Key: taskjournal.TaskStorageKey(task.ID), ModRevision: taskValue.ModRevision},
		{Key: assignmentValue.Key, ModRevision: assignmentValue.ModRevision},
		{Key: assignmentIndexValue.Key, ModRevision: assignmentIndexValue.ModRevision},
		{Key: timeoutKey, ModRevision: timeoutRead.Values[0].ModRevision},
		{Key: taskassignments.ServiceAbsenceRecoveryKey(task.ID)},
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
		{Type: keyvalue.MutationPut, Key: taskassignments.ServiceAbsenceRecoveryKey(task.ID), Value: recoveryValue},
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
		Record:       task,
		Revision:     transaction.Revision,
		ReadRevision: transaction.Revision,
	}, true, nil
}

func (repository *TaskRepository) serviceAbsenceMutationEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.ServiceAbsenceRecoveryProcedure,
	revision int64,
) ([]taskassignments.ReleaseRecoveryMutationEvidence, error) {
	forwardID := procedure.GetForwardStepIds()[0]
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil || snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		if err != nil {
			return nil, err
		}
		return nil, taskassignments.CorruptTaskAssignment()
	}
	evidence := taskassignments.ReleaseRecoveryMutationEvidence{StepID: forwardID}
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if !taskCheckpointAssignmentMatches(checkpoint, assignment) {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if checkpoint.Identity.StepID == forwardID && (checkpoint.Running || checkpoint.EffectPossible) {
			evidence.Running, evidence.Completed = true, checkpoint.Completed
		}
	}
	for _, event := range snapshot.Events {
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration || event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if event.Identity.StepID != forwardID {
			continue
		}
		switch event.State {
		case taskjournal.TaskEventStateRunning:
			evidence.Running = true
		case taskjournal.TaskEventStateCompleted:
			evidence.Running, evidence.Completed = true, true
		}
	}
	if !evidence.Running {
		return nil, nil
	}
	return []taskassignments.ReleaseRecoveryMutationEvidence{evidence}, nil
}

func (repository *TaskRepository) serviceAbsenceRecoveryAcknowledgementAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result taskjournal.TaskResultRecord,
	revision int64,
) (releaseRecoveryAcknowledgement, error) {
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskassignments.ServiceAbsenceRecoveryKey(task.ID)}, Revision: revision,
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
		record.RestorationAuthoritySHA256 != assignment.ServiceAbsenceProcedureSHA256 || record.TaskID != task.ID ||
		record.AssignmentID != assignment.AssignmentID || record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	resolved := releaseRecoveryAcknowledgement{record: record, value: read.Values[0]}
	if result.ExecutionEpoch < assignment.ExecutionEpoch && result.ReleaseRecoveryRecordSHA256 == "" {
		replayDigest, replayErr := taskassignments.CanonicalPrimaryReportSHA256(status, result)
		if replayErr != nil || replayDigest != record.PrimaryReportSHA256 {
			return releaseRecoveryAcknowledgement{}, errs.New(
				errs.KindStateConflict,
				"old Service absence recovery acknowledgement changed",
			)
		}
		return resolved, nil
	}
	if result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"Service absence recovery acknowledgement epoch changed",
		)
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired {
		return resolved, nil
	}
	procedure, intent, sourceConditions, found, err := repository.serviceAbsenceProcedureAtRevision(ctx, task, revision)
	digest, digestErr = serviceAbsenceAuthorityDigest(procedure)
	_, applicableErr := serviceAbsenceApplicableCompensation(procedure, record.MutationEvidence)
	if err != nil || !found || digestErr != nil || digest != assignment.ServiceAbsenceProcedureSHA256 ||
		applicableErr != nil ||
		record.Phase != taskassignments.ReleaseRecoveryPhaseProven ||
		int(record.Cursor) != len(record.RecoveryStepIDs) ||
		validateServiceAbsenceRecoveryProof(intent, procedure, result) != nil {
		return releaseRecoveryAcknowledgement{}, invalidServiceAbsenceProof()
	}
	primary := taskjournal.CloneTaskResult(&record.PrimaryResult)
	if primary == nil {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	primary.ReconciliationRequired = false
	primary.Projects = slices.Clone(result.Projects)
	primary.ServiceAbsenceEvidence = taskjournal.CloneTaskServiceAbsenceEvidence(result.ServiceAbsenceEvidence)
	primary.ExecutionEpoch = assignment.ExecutionEpoch
	primary.ReleaseRecoveryRecordSHA256 = assignment.ReleaseRecoveryRecordSHA256
	resolved.final, resolved.status, resolved.result = true, record.PrimaryStatus, *primary
	resolved.conditions = append(
		sourceConditions,
		keyvalue.Condition{
			Key:         taskassignments.ServiceAbsenceRecoveryKey(task.ID),
			ModRevision: read.Values[0].ModRevision,
		},
	)
	return resolved, nil
}

func validateServiceAbsenceRecoveryProof(
	intent environmentchanges.ServiceRemovalIntent,
	procedure *agentpb.ServiceAbsenceRecoveryProcedure,
	result taskjournal.TaskResultRecord,
) error {
	evidence := result.ServiceAbsenceEvidence
	if evidence == nil || !evidence.AbsenceProven || evidence.ServiceID != procedure.GetServiceId() ||
		evidence.ArtifactID != procedure.GetArtifactId() || evidence.ComposeProjectName == "" ||
		result.CandidateAbsenceEvidence != nil || result.ComponentRestorationEvidence != nil ||
		len(
			result.ProxyEvidence,
		) != 0 || len(result.RecreateEvidence) != 0 || len(result.RollingEvidence) != 0 || len(result.BackingObservations) != 0 ||
		result.DNSResolverCandidateObservation != nil || result.DNSResolverRollbackObservation != nil ||
		len(result.Projects) != 1 || result.Projects[0].ProjectName != evidence.ComposeProjectName ||
		result.Projects[0].CollisionCount != 0 {
		return taskassignments.CorruptTaskAssignment()
	}
	artifact := &agentpb.ComposeArtifact{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		intent.CurrentProjection.ComposeArtifact,
		artifact,
	) != nil ||
		artifact.GetProjectName() != evidence.ComposeProjectName {
		return taskassignments.CorruptTaskAssignment()
	}
	var names []string
	for _, member := range artifact.GetServices() {
		if member.GetServiceId() == procedure.GetServiceId() {
			names = append(names, member.GetComposeName())
		}
	}
	slices.Sort(names)
	if len(names) == 0 || !slices.Equal(names, evidence.ComposeNames) {
		return taskassignments.CorruptTaskAssignment()
	}
	return nil
}

func (repository *TaskRepository) normalizeServiceAbsenceTerminalReplay(
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
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired || task.Result == nil ||
		!taskassignments.ValidTerminalTaskStatus(task.Status) || task.TerminalAssignment == nil ||
		task.Result.ExecutionEpoch != result.ExecutionEpoch ||
		task.Result.ReleaseRecoveryRecordSHA256 != result.ReleaseRecoveryRecordSHA256 ||
		task.TerminalAssignment.AssignmentID != assignmentID || task.TerminalAssignment.AgentID != agentID ||
		task.TerminalAssignment.AgentGeneration != agentGeneration ||
		!slices.Equal(task.Result.Projects, result.Projects) ||
		!taskjournal.TaskServiceAbsenceEvidenceEqual(
			task.Result.ServiceAbsenceEvidence,
			result.ServiceAbsenceEvidence,
		) {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal Service absence recovery replay authority changed",
		)
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskassignments.ServiceAbsenceRecoveryKey(task.ID), taskjournal.TaskActiveOperationKey(task.OperationID),
	}, Revision: revision})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 2 {
		if err != nil {
			return status, nil, true, err
		}
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal Service absence recovery cleanup is unproven",
		)
	}
	if read.Values[0] != nil || read.Values[1] != nil {
		return status, nil, true, errs.New(
			errs.KindStateConflict,
			"terminal Service absence recovery authority remains",
		)
	}
	return task.Status, taskjournal.CloneTaskResult(task.Result), true, nil
}
