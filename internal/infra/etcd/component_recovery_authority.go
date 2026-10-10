package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	componentplanning "github.com/AlanD20/groundplane/internal/infra/etcd/componentplanning"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) componentRecoveryProcedureAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (*agentpb.ExecutionPlan, *agentpb.ComponentRecoveryProcedure, etcdstore.Condition, error) {
	if task.Params[taskjournal.TaskBlueprintComponentUnitParam] == "" {
		return nil, nil, etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Task is not a Blueprint Component child",
		)
	}
	child, err := componentplanning.ReadChildExecution(ctx, repository.store, task.ID, revision)
	if err != nil || child.Record.ParentTaskID != task.Params[taskjournal.TaskBlueprintParentParam] ||
		child.Record.Component.Desired.ID != task.Params[taskjournal.TaskBlueprintComponentUnitParam] {
		if err != nil {
			return nil, nil, etcdstore.Condition{}, err
		}
		return nil, nil, etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Component execution authority changed",
		)
	}
	plan, err := child.Record.ExecutionPlan()
	if err != nil || plan.GetPlanId() != task.PlanID || plan.GetComponentRecoveryProcedure() == nil {
		if err != nil {
			return nil, nil, etcdstore.Condition{}, err
		}
		return nil, nil, etcdstore.Condition{}, errs.New(
			errs.KindStateConflict,
			"Blueprint Component recovery procedure is unavailable",
		)
	}
	return plan, plan.GetComponentRecoveryProcedure(), etcdstore.Condition{
		Key: componentplanning.ChildExecutionKey(task.ID), ModRevision: child.Revision,
	}, nil
}

func (repository *TaskRepository) componentRecoveryDirectiveAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.ComponentRecoveryProcedure,
	revision int64,
) (*taskassignments.ReleaseRecoveryDirective, error) {
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly ||
		componentRecoveryAuthorityDigestMatches(procedure, assignment.ComponentRecoveryProcedureSHA256) != nil {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	read, err := repository.store.GetMany(ctx, etcdstore.GetManyRequest{
		Keys: []string{taskassignments.ComponentRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil {
		return nil, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeReleaseRecoveryRecord(read.Values[0].Value)
	digest, digestErr := taskassignments.ReleaseRecoveryRecordSHA256(record)
	applicable, applicableErr := componentApplicableCompensationStepIDs(procedure, record.MutationEvidence)
	if err != nil || digestErr != nil || digest != assignment.ReleaseRecoveryRecordSHA256 ||
		applicableErr != nil ||
		record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID || record.OperationID != task.OperationID ||
		record.PlanHash != task.PlanHash || record.RestorationAuthoritySHA256 != assignment.ComponentRecoveryProcedureSHA256 ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) || record.EvidenceRevision > revision ||
		!slices.Equal(record.RecoveryStepIDs, executionplan.ComponentRecoveryStepIDs(procedure)) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	return &taskassignments.ReleaseRecoveryDirective{
		Phase: record.Phase, StepIDs: slices.Clone(record.RecoveryStepIDs), Cursor: record.Cursor,
		RecordSHA256: digest, ApplicableCompensationStepIDs: applicable,
	}, nil
}

func componentRecoveryAuthorityDigest(procedure *agentpb.ComponentRecoveryProcedure) (string, error) {
	return taskassignments.ComponentRecoveryProcedureSHA256(procedure)
}

func assignmentRecoveryKey(taskID string, assignment taskassignments.TaskAssignmentRecord) string {
	if assignment.ComponentRecoveryProcedureSHA256 != "" {
		return taskassignments.ComponentRecoveryKey(taskID)
	}
	if assignment.ServiceAbsenceProcedureSHA256 != "" {
		return taskassignments.ServiceAbsenceRecoveryKey(taskID)
	}
	return taskassignments.ReleaseRecoveryKey(taskID)
}

func assignmentRecoveryAuthorityDigest(assignment taskassignments.TaskAssignmentRecord) string {
	if assignment.ComponentRecoveryProcedureSHA256 != "" {
		return assignment.ComponentRecoveryProcedureSHA256
	}
	if assignment.ServiceAbsenceProcedureSHA256 != "" {
		return assignment.ServiceAbsenceProcedureSHA256
	}
	return assignment.RestorationAuthoritySHA256
}
