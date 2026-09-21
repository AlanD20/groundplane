package etcd

import (
	"context"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"

	"github.com/AlanD20/groundplane/internal/common/executionplan"
	"github.com/AlanD20/groundplane/proto/agentpb"
)

func releaseRestorationStepIDs(
	procedure *agentpb.CandidateReleaseProcedure,
	candidates []taskassignments.ReleaseRestorationCandidate,
) ([]string, error) {
	if err := validateSelectedRestorationTargets(procedure, candidates); err != nil {
		return nil, err
	}
	return executionplan.RecoveryStepIDs(procedure), nil
}

func releaseApplicableCompensationStepIDs(
	procedure *agentpb.CandidateReleaseProcedure,
	candidates []taskassignments.ReleaseRestorationCandidate,
	evidence []taskassignments.ReleaseRecoveryMutationEvidence,
) ([]string, error) {
	if err := validateSelectedRestorationTargets(procedure, candidates); err != nil {
		return nil, err
	}
	byStep, err := releaseMutationEvidenceByStep(procedure, evidence)
	if err != nil {
		return nil, err
	}
	configuration := procedure.GetConfigurationRestoration()
	result := make([]string, 0, len(configuration.GetFiles())+len(procedure.GetMembers()))
	for _, file := range configuration.GetFiles() {
		if byStep[file.GetForwardStepId()].Running {
			result = append(result, file.GetCompensateStepId())
		}
	}
	for index := len(procedure.GetMembers()) - 1; index >= 0; index-- {
		member := procedure.GetMembers()[index]
		completed := false
		for _, stepID := range member.GetForwardStepIds() {
			if byStep[stepID].Completed {
				completed = true
				break
			}
		}
		if !completed {
			continue
		}
		switch candidates[index].Target {
		case taskassignments.ReleaseRestorationServingPredecessor:
			result = append(result, member.GetServingPredecessor().GetCompensateStepId())
		case taskassignments.ReleaseRestorationCandidateAbsence:
			result = append(result, member.GetCandidateAbsence().GetCompensateStepId())
		default:
			return nil, taskassignments.CorruptTaskAssignment()
		}
	}
	return result, nil
}

func releaseMutationEvidenceByStep(
	procedure *agentpb.CandidateReleaseProcedure,
	evidence []taskassignments.ReleaseRecoveryMutationEvidence,
) (map[string]taskassignments.ReleaseRecoveryMutationEvidence, error) {
	ordered, _, _ := releaseForwardMutationSteps(procedure)
	result := make(map[string]taskassignments.ReleaseRecoveryMutationEvidence, len(evidence))
	evidenceIndex := 0
	for _, stepID := range ordered {
		if evidenceIndex >= len(evidence) || evidence[evidenceIndex].StepID != stepID {
			continue
		}
		item := evidence[evidenceIndex]
		if !item.Running {
			return nil, taskassignments.CorruptTaskAssignment()
		}
		result[stepID] = item
		evidenceIndex++
	}
	if evidenceIndex != len(evidence) {
		return nil, taskassignments.CorruptTaskAssignment()
	}
	return result, nil
}

func releaseForwardMutationSteps(
	procedure *agentpb.CandidateReleaseProcedure,
) ([]string, map[string]struct{}, map[string]struct{}) {
	ordered := make([]string, 0)
	selected := make(map[string]struct{})
	fileSteps := make(map[string]struct{})
	appendStep := func(stepID string) {
		if _, exists := selected[stepID]; exists {
			return
		}
		selected[stepID] = struct{}{}
		ordered = append(ordered, stepID)
	}
	for _, file := range procedure.GetConfigurationRestoration().GetFiles() {
		appendStep(file.GetForwardStepId())
		fileSteps[file.GetForwardStepId()] = struct{}{}
	}
	for _, member := range procedure.GetMembers() {
		for _, stepID := range member.GetForwardStepIds() {
			appendStep(stepID)
		}
	}
	return ordered, selected, fileSteps
}

func (repository *TaskRepository) releaseCandidateMutationEvidenceAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
	procedure *agentpb.CandidateReleaseProcedure,
	revision int64,
) (bool, []taskassignments.ReleaseRecoveryMutationEvidence, error) {
	ordered, mutationSteps, fileSteps := releaseForwardMutationSteps(procedure)
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil || snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence {
		if err != nil {
			return false, nil, err
		}
		return false, nil, taskassignments.CorruptTaskAssignment()
	}
	evidenceByStep := make(map[string]taskassignments.ReleaseRecoveryMutationEvidence)
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if !taskCheckpointAssignmentMatches(checkpoint, assignment) {
			return false, nil, taskassignments.CorruptTaskAssignment()
		}
		if _, mutation := mutationSteps[checkpoint.Identity.StepID]; !mutation {
			continue
		}
		_, file := fileSteps[checkpoint.Identity.StepID]
		running := checkpoint.Running || !file && checkpoint.EffectPossible
		if running {
			evidenceByStep[checkpoint.Identity.StepID] = taskassignments.ReleaseRecoveryMutationEvidence{
				StepID: checkpoint.Identity.StepID, Running: true, Completed: checkpoint.Completed,
			}
		}
	}
	for _, event := range snapshot.Events {
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration || event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return false, nil, taskassignments.CorruptTaskAssignment()
		}
		if _, mutation := mutationSteps[event.Identity.StepID]; !mutation {
			continue
		}
		evidence := evidenceByStep[event.Identity.StepID]
		_, configurationFile := fileSteps[event.Identity.StepID]
		switch event.State {
		case taskjournal.TaskEventStateRunning:
			evidence.StepID, evidence.Running = event.Identity.StepID, true
		case taskjournal.TaskEventStateCompleted:
			evidence.StepID, evidence.Running, evidence.Completed = event.Identity.StepID, true, true
		case taskjournal.TaskEventStateFailed, taskjournal.TaskEventStateAborted, taskjournal.TaskEventStateTimedOut:
			// A file write is applicable only after its durable Running event.
			// Native forward evidence keeps its existing non-pending semantics.
			if !configurationFile {
				evidence.StepID, evidence.Running = event.Identity.StepID, true
			}
		}
		if evidence.Running {
			evidenceByStep[event.Identity.StepID] = evidence
		}
	}
	evidence := make([]taskassignments.ReleaseRecoveryMutationEvidence, 0, len(evidenceByStep))
	for _, stepID := range ordered {
		if item, ok := evidenceByStep[stepID]; ok {
			evidence = append(evidence, item)
		}
	}
	return len(evidence) != 0, evidence, nil
}
