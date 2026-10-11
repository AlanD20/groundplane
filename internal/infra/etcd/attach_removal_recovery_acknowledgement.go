package etcd

import (
	"context"

	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (repository *TaskRepository) attachRemovalRecoveryAcknowledgementAtRevision(
	ctx context.Context, task TaskRecord, assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus, result taskjournal.TaskResultRecord, revision int64,
) (releaseRecoveryAcknowledgement, error) {
	record, value, err := repository.readAttachRemovalRecovery(ctx, task, assignment, revision)
	if err != nil {
		return releaseRecoveryAcknowledgement{}, err
	}
	resolved := releaseRecoveryAcknowledgement{value: value}
	if result.ExecutionEpoch < assignment.ExecutionEpoch && result.ReleaseRecoveryRecordSHA256 == "" {
		digest, digestErr := taskassignments.CanonicalPrimaryReportSHA256(status, result)
		if digestErr != nil || digest != record.PrimaryReportSHA256 {
			return releaseRecoveryAcknowledgement{}, errs.New(
				errs.KindStateConflict,
				"original Detach failure report changed",
			)
		}
		return resolved, nil
	}
	if result.ExecutionEpoch != assignment.ExecutionEpoch ||
		result.ReleaseRecoveryRecordSHA256 != assignment.ReleaseRecoveryRecordSHA256 {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"Detach recovery report belongs to another execution",
		)
	}
	if status != taskjournal.TaskStatusCompleted || result.ReconciliationRequired {
		return resolved, nil
	}
	if !validAttachRemovalRecoverySuccess(result) || int(record.Cursor) != len(record.Authority.StepIDs) {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"Detach cleanup has incomplete durable step proof",
		)
	}
	authority, sourceConditions, found, err := repository.attachRemovalRecoveryAuthorityAtRevision(ctx, task, revision)
	digest, digestErr := taskassignments.AttachRemovalRecoveryAuthoritySHA256(authority)
	if err != nil {
		return releaseRecoveryAcknowledgement{}, err
	}
	if !found || digestErr != nil || digest != assignment.AttachRemovalAuthoritySHA256 {
		return releaseRecoveryAcknowledgement{}, errs.New(
			errs.KindStateConflict,
			"captured Detach cleanup authority changed",
		)
	}
	primary, err := taskjournal.TaskResultFromData(&record.PrimaryResult)
	if err != nil || primary == nil {
		return releaseRecoveryAcknowledgement{}, taskassignments.CorruptTaskAssignment()
	}
	primary.ReconciliationRequired = false
	primary.ExecutionEpoch = assignment.ExecutionEpoch
	primary.ReleaseRecoveryRecordSHA256 = assignment.ReleaseRecoveryRecordSHA256
	resolved.final, resolved.status, resolved.result = true, record.PrimaryStatus, *primary
	resolved.conditions = append(sourceConditions, keyvalue.Condition{Key: value.Key, ModRevision: value.ModRevision})
	return resolved, nil
}

// A cleanup receipt is separate from the original failure. It may contain no
// invented runtime observation, release result or unrelated recovery proof.
func validAttachRemovalRecoverySuccess(result taskjournal.TaskResultRecord) bool {
	return result.Kind == taskjournal.TaskResultCompose && result.ExitCode == 0 &&
		result.Diagnostic == taskjournal.TaskResultDiagnosticNone && result.FailedStepID == "" &&
		!result.ReconciliationRequired && len(result.Projects) == 0 && len(result.ProxyEvidence) == 0 &&
		len(
			result.RecreateEvidence,
		) == 0 && len(result.RollingEvidence) == 0 && result.CandidateAbsenceEvidence == nil &&
		result.ComponentRestorationEvidence == nil && result.ServiceAbsenceEvidence == nil &&
		len(result.BackingObservations) == 0 && result.DNSResolverCandidateObservation == nil &&
		result.DNSResolverRollbackObservation == nil
}

func retainedAuthoredAttachAbsence(task TaskRecord) bool {
	return authoredBlueprintAttachRemovalTask(task) && task.Result != nil &&
		task.Result.Kind == taskjournal.TaskResultCompose && !task.Result.ReconciliationRequired &&
		task.Result.ExecutionEpoch > 1 && task.Result.ReleaseRecoveryRecordSHA256 != ""
}

func recoveredAuthoredAttachAbsence(task TaskRecord, assignment taskassignments.TaskAssignmentRecord) bool {
	return retainedAuthoredAttachAbsence(task) &&
		assignment.ExecutionMode == taskassignments.TaskExecutionModeRecoveryOnly &&
		assignment.AttachRemovalAuthoritySHA256 != "" &&
		task.Result.ExecutionEpoch == assignment.ExecutionEpoch &&
		task.Result.ReleaseRecoveryRecordSHA256 == assignment.ReleaseRecoveryRecordSHA256
}

func (repository *TaskRepository) normalizeAttachRemovalRecoveryTerminalReplay(
	ctx context.Context, task TaskRecord, status taskjournal.TaskStatus, result taskjournal.TaskResultRecord,
	agentID string, agentGeneration uint64, assignmentID string, revision int64,
) (taskjournal.TaskStatus, *taskjournal.TaskResultRecord, bool, error) {
	if result.ReleaseRecoveryRecordSHA256 == "" || !authoredBlueprintAttachRemovalTask(task) {
		return status, nil, false, nil
	}
	if status != taskjournal.TaskStatusCompleted || !validAttachRemovalRecoverySuccess(result) ||
		!retainedAuthoredAttachAbsence(task) || !taskjournal.IsTerminalTaskStatus(task.Status) ||
		task.TerminalAssignment == nil || task.TerminalAssignment.AssignmentID != assignmentID ||
		task.TerminalAssignment.AgentID != agentID || task.TerminalAssignment.AgentGeneration != agentGeneration ||
		result.ExecutionEpoch != task.Result.ExecutionEpoch || result.ReleaseRecoveryRecordSHA256 != task.Result.ReleaseRecoveryRecordSHA256 {
		return status, nil, true, errs.New(errs.KindStateConflict, "completed Detach recovery report changed")
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: []string{
		taskassignments.AttachRemovalRecoveryKey(task.ID), taskjournal.TaskActiveOperationKey(task.OperationID),
	}, Revision: revision})
	if err != nil {
		return status, nil, true, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 2 || read.Values[0] != nil ||
		read.Values[1] != nil {
		return status, nil, true, errs.New(errs.KindStateConflict, "Detach recovery cleanup is not retired")
	}
	return task.Status, taskjournal.CloneTaskResult(task.Result), true, nil
}

func (repository *TaskRepository) requireAttachRemovalRecoveryStart(
	ctx context.Context, task TaskRecord, input taskjournal.TaskEventInput, revision int64,
) error {
	if input.State != taskjournal.TaskEventStateCompleted {
		return nil
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil {
		return err
	}
	if snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		return errs.New(errs.KindStateConflict, "Detach recovery journal changed")
	}
	expected := input.Identity
	expected.Ordinal = 1
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		identity := checkpoint.Identity
		identity.Ordinal = 1
		if identity == expected && checkpoint.Running {
			return nil
		}
	}
	for _, event := range snapshot.Events {
		if event.Identity == expected && event.State == taskjournal.TaskEventStateRunning {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "Detach cleanup step has no durable start")
}
