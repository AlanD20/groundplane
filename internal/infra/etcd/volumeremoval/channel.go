package volumeremoval

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// ResumeAssigned admits read-only recovery as well as mutation requests under
// the same current Task, assignment, operation and ownership fences.
func (repository *EnvironmentVolumeRemovalRuntimeRepository) ResumeAssigned(
	ctx context.Context, assignment EnvironmentVolumeRemovalAssignment, stepID, planHash string,
) (EnvironmentVolumeRemovalResumeState, error) {
	state, err := repository.Resume(ctx, assignment.OperationID)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	task, _, err := repository.loadAssignedTask(
		ctx,
		assignment,
		state.Runtime.ReadRevision,
		state.Runtime.Record,
		state.Attempt,
	)
	if err != nil {
		return EnvironmentVolumeRemovalResumeState{}, err
	}
	if stepID != state.Runtime.Record.StepID || planHash != task.PlanHash {
		return EnvironmentVolumeRemovalResumeState{}, errs.New(
			errs.KindStateConflict,
			"Volume checkpoint plan or step changed",
		)
	}
	return state, nil
}
