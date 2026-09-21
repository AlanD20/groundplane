package etcd

import (
	"context"
	taskassignments "github.com/AlanD20/groundplane/internal/infra/etcd/taskassignments"
	taskjournal "github.com/AlanD20/groundplane/internal/infra/etcd/taskjournal"
	"slices"
)

// Component RUNNING is durable mutation evidence. Workload restoration never
// proves restoration of a separately executed Component action.
func (repository *TaskRepository) releaseComponentEffectAtRevision(
	ctx context.Context,
	task TaskRecord,
	assignment taskassignments.TaskAssignmentRecord,
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
		return false, taskassignments.CorruptTaskAssignment()
	}
	for _, event := range snapshot.Events {
		if !slices.Contains(task.ComponentActionStepIDs, event.Identity.StepID) {
			continue
		}
		if event.Identity.AssignmentID != assignment.AssignmentID || event.Identity.AgentID != assignment.AgentID ||
			event.Identity.AgentGeneration != assignment.AgentGeneration ||
			event.Identity.Attempt == 0 ||
			event.Identity.Attempt > assignment.ExecutionEpoch {
			return false, taskassignments.CorruptTaskAssignment()
		}
		if event.State != taskjournal.TaskEventStatePending {
			return true, nil
		}
	}
	for _, checkpoint := range snapshot.Task.EventCheckpoints {
		if !slices.Contains(task.ComponentActionStepIDs, checkpoint.Identity.StepID) {
			continue
		}
		if !taskCheckpointAssignmentMatches(checkpoint, assignment) {
			return false, taskassignments.CorruptTaskAssignment()
		}
		if checkpoint.EffectPossible {
			return true, nil
		}
	}
	return false, nil
}
