package etcd

import (
	"context"
	"slices"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/internal/common/networkname"
	deletionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/deletions"
	"github.com/AlanD20/groundplane/internal/infra/etcd/environmentchanges"
	projectionrecord "github.com/AlanD20/groundplane/internal/infra/etcd/environmentprojection"
	"github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	"github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"github.com/AlanD20/groundplane/pkg/errs"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (repository *TaskRepository) managedNetworkRecoveryProcedureAtRevision(
	ctx context.Context,
	task TaskRecord,
	revision int64,
) (*agentpb.ManagedNetworkRecoveryProcedure, environmentchanges.ZoneRemovalIntent, []keyvalue.Condition, bool, error) {
	if taskjournal.IsReleaseNetworkChild(task.Params) {
		procedure, conditions, found, err := repository.releaseNetworkRecoveryProcedureAtRevision(ctx, task, revision)
		return procedure, environmentchanges.ZoneRemovalIntent{}, conditions, found, err
	}
	if !authoredBlueprintZoneRemovalTask(task) || len(task.Steps) != 1 {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, nil
	}
	keys := []string{
		environmentchanges.ZoneRemovalIntentKey(task.Params[taskjournal.TaskZoneRemovalOperationParam]),
		projectionrecord.EnvironmentComposeProjectionStorageKey(task.Owner.EnvironmentID),
		environmentchanges.ComponentTaskActiveEnvironmentKey(task.Owner.EnvironmentID),
		deletionrecord.TombstoneKey(string(deletionrecord.DeletionTargetZone), task.Target),
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{Keys: keys, Revision: revision})
	if err != nil {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, err
	}
	if read == nil || read.ReadRevision != revision || len(read.Values) != len(keys) ||
		read.Values[0] == nil || read.Values[1] == nil || read.Values[2] == nil || read.Values[3] == nil {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	defer keyvalue.ClearValues(read.Values)
	intent, err := environmentchanges.DecodeZoneRemovalIntent(read.Values[0].Value)
	if err != nil || intent.Authored == nil || intent.Status != taskjournal.TaskStatusPending ||
		validateZoneRemovalTaskOwner(task, intent) != nil ||
		!environmentchanges.ZoneRemovalAppliedEvidenceMatches(intent, read.Values[1]) ||
		string(read.Values[2].Value) != task.ID {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	if _, err := projectedZoneRemovalTarget(intent, revision); err != nil {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, err
	}
	tombstone, err := deletionrecord.DecodeDeletionTombstone(read.Values[3].Value)
	if err != nil || tombstone.TargetKind != deletionrecord.DeletionTargetZone ||
		tombstone.TargetID != intent.ZoneID || tombstone.TargetRevision != intent.ZoneRevision ||
		tombstone.TaskID != task.ID ||
		(tombstone.Phase != deletionrecord.DeletionPhaseHostEffects &&
			tombstone.Phase != deletionrecord.DeletionPhaseFinalizing) {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	dockerName, err := networkname.New(intent.ZoneID)
	if err != nil {
		return nil, environmentchanges.ZoneRemovalIntent{}, nil, false, taskassignments.CorruptTaskAssignment()
	}
	procedure := &agentpb.ManagedNetworkRecoveryProcedure{
		EnvironmentId: intent.EnvironmentID,
		Operation: &agentpb.ManagedNetworkRecoveryProcedure_Removal{Removal: &agentpb.ManagedNetworkRemovalRecovery{
			StepId: task.Steps[0].ID, NetworkId: intent.ZoneID, DockerName: dockerName,
		}},
	}
	conditions := make([]keyvalue.Condition, len(keys))
	for index, key := range keys {
		conditions[index] = keyvalue.Condition{Key: key, ModRevision: read.Values[index].ModRevision}
	}
	return procedure, environmentchanges.CloneZoneRemovalIntent(intent), conditions, true, nil
}

func managedNetworkRecoveryAuthorityDigest(procedure *agentpb.ManagedNetworkRecoveryProcedure) (string, error) {
	return taskassignments.ManagedNetworkRecoveryProcedureSHA256(procedure)
}

func (repository *TaskRepository) managedNetworkMutationEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.ManagedNetworkRecoveryProcedure,
	revision int64,
) ([]taskassignments.ReleaseRecoveryMutationEvidence, error) {
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil || snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		if err != nil {
			return nil, err
		}
		return nil, taskassignments.CorruptTaskAssignment()
	}
	byStep := make(map[string]taskassignments.ReleaseRecoveryMutationEvidence)
	forwardSteps := executionplan.NetworkRecoveryForwardSteps(procedure)
	for _, stepID := range forwardSteps {
		byStep[stepID] = taskassignments.ReleaseRecoveryMutationEvidence{StepID: stepID}
	}
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if !taskCheckpointAssignmentMatches(checkpoint, assignment) {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		if evidence, found := byStep[checkpoint.Identity.StepID]; found &&
			(checkpoint.Running || checkpoint.EffectPossible) {
			evidence.Running, evidence.Completed = true, checkpoint.Completed
			byStep[evidence.StepID] = evidence
		}
	}
	for _, event := range snapshot.Events {
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration || event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		evidence, found := byStep[event.Identity.StepID]
		if !found {
			continue
		}
		switch event.State {
		case taskjournal.TaskEventStateRunning:
			evidence.Running = true
		case taskjournal.TaskEventStateCompleted:
			evidence.Running, evidence.Completed = true, true
		}
		byStep[evidence.StepID] = evidence
	}
	var observed []taskassignments.ReleaseRecoveryMutationEvidence
	for _, stepID := range forwardSteps {
		if evidence := byStep[stepID]; evidence.Running {
			observed = append(observed, evidence)
		}
	}
	return observed, nil
}

func (repository *TaskRepository) managedNetworkRecoveryRequiredAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	revision int64,
) (bool, error) {
	procedure, _, _, found, err := repository.managedNetworkRecoveryProcedureAtRevision(ctx, task, revision)
	digest, digestErr := managedNetworkRecoveryAuthorityDigest(procedure)
	if err != nil || !found || digestErr != nil || digest != assignment.ManagedNetworkProcedureSHA256 {
		if err != nil {
			return false, err
		}
		return false, taskassignments.CorruptTaskAssignment()
	}
	evidence, err := repository.managedNetworkMutationEvidenceAtRevision(ctx, task, assignment, procedure, revision)
	return len(evidence) != 0, err
}

func (repository *TaskRepository) validateCompletedManagedNetworkReport(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	status taskjournal.TaskStatus,
	result *taskjournal.TaskResultRecord,
	revision int64,
) error {
	if assignment.ManagedNetworkProcedureSHA256 == "" || status != taskjournal.TaskStatusCompleted {
		return nil
	}
	procedure, _, _, found, err := repository.managedNetworkRecoveryProcedureAtRevision(ctx, task, revision)
	digest, digestErr := managedNetworkRecoveryAuthorityDigest(procedure)
	if err != nil || !found || digestErr != nil || digest != assignment.ManagedNetworkProcedureSHA256 ||
		result == nil || result.Kind != taskjournal.TaskResultCompose || result.ReconciliationRequired {
		return errs.New(errs.KindStateConflict, "managed network completion proof is invalid")
	}
	if assignment.ExecutionMode == taskassignments.TaskExecutionModeForward {
		for _, stepID := range executionplan.NetworkRecoveryForwardSteps(procedure) {
			if err := repository.validateManagedNetworkRecoveryCompletedEvent(ctx, task,
				assignment.AssignmentID, assignment.AgentID, assignment.AgentGeneration,
				assignment.ExecutionEpoch, stepID, revision); err != nil {
				return err
			}
		}
	}
	return nil
}

func (repository *TaskRepository) validateManagedNetworkRecoveryCompletedEvent(
	ctx context.Context,
	task TaskRecord,
	assignmentID, agentID string,
	agentGeneration uint64,
	executionEpoch uint32,
	stepID string,
	revision int64,
) error {
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil || snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		if err != nil {
			return err
		}
		return errs.New(errs.KindStateConflict, "managed network recovery event journal changed")
	}
	expected := taskjournal.TaskEventIdentity{
		AssignmentID: assignmentID, AgentID: agentID, AgentGeneration: agentGeneration,
		TaskID: task.ID, StepID: stepID, Attempt: executionEpoch, Ordinal: 2,
	}
	for _, event := range snapshot.Events {
		if event.Identity == expected && event.State == taskjournal.TaskEventStateCompleted {
			return nil
		}
	}
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if checkpoint.Identity == expected && checkpoint.State == taskjournal.TaskEventStateCompleted &&
			checkpoint.Completed {
			return nil
		}
	}
	return errs.New(errs.KindStateConflict, "managed network recovery completion event is unproven")
}

func (repository *TaskRepository) managedNetworkRecoveryDirectiveAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.ManagedNetworkRecoveryProcedure,
	revision int64,
) (*taskassignments.ReleaseRecoveryDirective, error) {
	if assignment.ExecutionMode != taskassignments.TaskExecutionModeRecoveryOnly {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	read, err := repository.store.GetMany(ctx, keyvalue.GetManyRequest{
		Keys: []string{taskassignments.ManagedNetworkRecoveryKey(task.ID)}, Revision: revision,
	})
	if err != nil || read == nil || read.ReadRevision != revision || len(read.Values) != 1 || read.Values[0] == nil {
		if err != nil {
			return nil, err
		}
		return nil, taskassignments.CorruptTaskAssignment()
	}
	record, err := taskassignments.DecodeManagedNetworkRecoveryRecord(read.Values[0].Value)
	recordDigest, recordDigestErr := taskassignments.ManagedNetworkRecoveryRecordSHA256(record)
	procedureDigest, procedureDigestErr := managedNetworkRecoveryAuthorityDigest(procedure)
	if err != nil || recordDigestErr != nil || procedureDigestErr != nil ||
		recordDigest != assignment.ReleaseRecoveryRecordSHA256 ||
		procedureDigest != assignment.ManagedNetworkProcedureSHA256 ||
		record.TaskID != task.ID || record.AssignmentID != assignment.AssignmentID ||
		record.OperationID != task.OperationID || record.PlanHash != task.PlanHash ||
		record.ProcedureSHA256 != procedureDigest || !slices.Equal(record.ForwardStepIDs, executionplan.NetworkRecoveryForwardSteps(procedure)) ||
		!slices.Equal(record.SettlementStepIDs, executionplan.NetworkRecoverySettlementSteps(procedure)) ||
		record.OriginalExecutionEpoch >= assignment.ExecutionEpoch ||
		!record.RecoveryDeadline.Equal(assignment.RecoveryDeadline) || record.EvidenceRevision > revision {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	phase := taskassignments.ReleaseRecoveryPhaseCompensate
	if procedure.GetPreparation() != nil {
		phase = taskassignments.ReleaseRecoveryPhaseProbe
	}
	return &taskassignments.ReleaseRecoveryDirective{
		Phase:   phase,
		StepIDs: executionplan.NetworkRecoverySettlementSteps(procedure), Cursor: 0, RecordSHA256: recordDigest,
	}, nil
}

func ValidateManagedNetworkRecoveryDirective(
	procedure *agentpb.ManagedNetworkRecoveryProcedure,
	directive *taskassignments.ReleaseRecoveryDirective,
) error {
	phase := taskassignments.ReleaseRecoveryPhaseCompensate
	if procedure.GetPreparation() != nil {
		phase = taskassignments.ReleaseRecoveryPhaseProbe
	}
	if procedure == nil || directive == nil || directive.Phase != phase ||
		directive.Cursor != 0 || !slices.Equal(directive.StepIDs, executionplan.NetworkRecoverySettlementSteps(procedure)) ||
		len(directive.ApplicableCompensationStepIDs) != 0 {
		return errs.New(errs.KindInternal, "managed network recovery directive is invalid")
	}
	return nil
}
