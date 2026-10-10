package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	servicerecord "github.com/AlanD20/groundplane/internal/infra/etcd/services"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/internal/infra/serviceruntimerecord"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
	"google.golang.org/protobuf/proto"
)

func (repository *TaskRepository) serviceAbsenceProcedureAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (*agentpb.ServiceAbsenceRecoveryProcedure, environmentchanges.ServiceRemovalIntent, []keyvalue.Condition, bool, error) {
	if !authoredBlueprintServiceRemovalTask(task) || len(task.Steps) != 3 {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, nil
	}
	keys := []string{
		environmentchanges.ServiceRemovalIntentKey(task.ID),
		projectionrecord.EnvironmentComposeProjectionStorageKey(task.Owner.EnvironmentID),
		servicerecord.ServiceRuntimeKey(task.Target),
		serviceruntimerecord.Key(task.Target),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(read.Values)
	intent, err := environmentchanges.DecodeServiceRemovalIntent(read.Values[0].Value)
	if err != nil || intent.Authored == nil || intent.AcknowledgedRuntime != nil ||
		intent.AcknowledgedRuntimeRevision != 0 || read.Values[3] != nil || intent.Status != taskjournal.TaskStatusPending ||
		validateServiceRemovalTaskOwner(
			task,
			intent,
		) != nil || read.Values[1].ModRevision != intent.CurrentProjectionRevision ||
		!keyvalue.ConditionMatchesRead(
			keyvalue.Condition{Key: keys[2], ModRevision: intent.RuntimeRevision}, read.Values[2],
		) {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	applied, err := projectionrecord.DecodeEnvironmentComposeProjectionStorage(read.Values[1].Value)
	if err != nil || !environmentchanges.SameServiceRemovalProjection(applied, intent.CurrentProjection) {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	artifact := &agentpb.ComposeArtifact{}
	if (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(
		intent.CurrentProjection.ComposeArtifact,
		artifact,
	) != nil ||
		artifact.GetProjectName() == "" {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	var names []string
	for _, member := range artifact.GetServices() {
		if member.GetServiceId() == intent.ServiceID {
			names = append(names, member.GetComposeName())
		}
	}
	slices.Sort(names)
	if len(names) == 0 {
		return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	for index := 1; index < len(names); index++ {
		if names[index] == names[index-1] {
			return nil, environmentchanges.ServiceRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
		}
	}
	procedure := &agentpb.ServiceAbsenceRecoveryProcedure{
		ServiceId: intent.ServiceID, ArtifactId: task.Params[taskjournal.TaskComposeArtifactParam],
		ForwardStepIds: []string{task.Steps[0].ID}, ProbeStepId: task.Steps[1].ID, CompensateStepId: task.Steps[2].ID,
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: keyvalue.RevisionOf(read.Values[index])}
	}
	return procedure, environmentchanges.CloneServiceRemovalIntent(intent), conditions, true, nil
}

func serviceAbsenceAuthorityDigest(procedure *agentpb.ServiceAbsenceRecoveryProcedure) (string, error) {
	return taskassignments.ServiceAbsenceProcedureSHA256(procedure)
}

func (repository *TaskRepository) serviceAbsenceRecoveryRequiredAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	revision int64,
) (bool, error) {
	procedure, _, _, found, err := repository.serviceAbsenceProcedureAtRevision(ctx, task, revision)
	digest, digestErr := serviceAbsenceAuthorityDigest(procedure)
	if err != nil || !found || digestErr != nil || digest != assignment.ServiceAbsenceProcedureSHA256 {
		if err != nil {
			return false, err
		}
		return false, taskassignments.CorruptTaskAssignment()
	}
	evidence, err := repository.serviceAbsenceMutationEvidenceAtRevision(ctx, task, assignment, procedure, revision)
	return len(evidence) != 0, err
}

func (repository *TaskRepository) validateCompletedServiceAbsenceReport(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result *taskjournal.TaskResultRecord,
	revision int64,
) error {
	if assignment.ServiceAbsenceProcedureSHA256 == "" || status != taskjournal.TaskStatusCompleted {
		return nil
	}
	procedure, intent, _, found, err := repository.serviceAbsenceProcedureAtRevision(ctx, task, revision)
	digest, digestErr := serviceAbsenceAuthorityDigest(procedure)
	if err != nil || !found || digestErr != nil || digest != assignment.ServiceAbsenceProcedureSHA256 ||
		result == nil || validateServiceAbsenceRecoveryProof(intent, procedure, *result) != nil {
		return invalidServiceAbsenceProof()
	}
	return nil
}

func (repository *TaskRepository) serviceAbsenceDirectiveAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.ServiceAbsenceRecoveryProcedure,
	revision int64,
) (*taskassignments.ReleaseRecoveryDirective, error) {
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskassignments.ServiceAbsenceRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if err != nil {
			return nil, err
		}
		return nil, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeReleaseRecoveryRecord(read.Values[0].Value)
	digest, digestErr := taskassignments.ReleaseRecoveryRecordSHA256(record)
	procedureDigest, procedureDigestErr := serviceAbsenceAuthorityDigest(procedure)
	applicable, applicableErr := serviceAbsenceApplicableCompensation(procedure, record.MutationEvidence)
	if err != nil || digestErr != nil || procedureDigestErr != nil || applicableErr != nil ||
		digest != assignment.ReleaseRecoveryRecordSHA256 || procedureDigest != assignment.ServiceAbsenceProcedureSHA256 ||
		record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID || record.OperationID != task.OperationID ||
		record.PlanHash != task.PlanHash || record.RestorationAuthoritySHA256 != procedureDigest ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) || record.EvidenceRevision > revision ||
		!slices.Equal(record.RecoveryStepIDs, executionplan.ServiceAbsenceRecoveryStepIDs(procedure)) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	return &taskassignments.ReleaseRecoveryDirective{
		Phase: record.Phase, StepIDs: slices.Clone(record.RecoveryStepIDs), Cursor: record.Cursor,
		RecordSHA256: digest, ApplicableCompensationStepIDs: applicable,
	}, nil
}

func serviceAbsenceApplicableCompensation(
	procedure *agentpb.ServiceAbsenceRecoveryProcedure,
	evidence []taskassignments.ReleaseRecoveryMutationEvidence,
) ([]string, error) {
	if len(evidence) == 0 {
		return nil, nil
	}
	if len(evidence) != 1 || evidence[0].StepID != procedure.GetForwardStepIds()[0] || !evidence[0].Running {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	return []string{procedure.GetCompensateStepId()}, nil
}

func invalidServiceAbsenceProof() error {
	return errs.New(errs.KindStateConflict, "Service absence recovery proof is invalid")
}
