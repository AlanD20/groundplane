package etcd

import (
	"context"
	"slices"
)

// Component RUNNING is durable mutation evidence. Workload restoration never
// proves restoration of a separately executed Component action.
func (repository *TaskRepository) releaseComponentEffectAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment TaskAssignmentRecord,
	revision int64,
) (bool, error) {
	if len(task.ComponentActionStepIDs) == 0 {
		return false, nil
	}
	snapshot, err := repository.ListTaskEvents(ctx, task.ID, revision)
	if err != nil {
		return false, err
	}
	if snapshot.Revision != revision || snapshot.Task.NextEventSequence != task.NextEventSequence ||
		!slices.Equal(snapshot.Task.ComponentActionStepIDs, task.ComponentActionStepIDs) {
		return false, corruptTaskAssignment()
	}
	for _, event := range snapshot.Events {
		if !slices.Contains(task.ComponentActionStepIDs, event.Identity.StepID) {
			continue
		}
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration ||
			event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return false, corruptTaskAssignment()
		}
		if event.State != TaskEventStatePending {
			return true, nil
		}
	}
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if !slices.Contains(task.ComponentActionStepIDs, checkpoint.Identity.StepID) {
			continue
		}
		if !taskCheckpointAssignmentMatches(checkpoint, assignment) {
			return false, corruptTaskAssignment()
		}
		if checkpoint.EffectPossible {
			return true, nil
		}
	}
	return false, nil
}
