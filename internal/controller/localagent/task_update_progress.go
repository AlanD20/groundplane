package localagent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// The lifecycle may successfully reconcile back to the previous image after a
// candidate failure. Record that candidate failure, not a successful activation,
// while preserving the lifecycle's result for the existing recovery state machine.
func (updates *TaskUpdates) reconcileWithProgress(ctx context.Context, request UpdateRequest,
	goal UpdateGoal, failure *taskprogress.Failure, progress *controllertask.Progress, stage string,
) (UpdateResult, error) {
	if err := progress.Start(stage); err != nil {
		return "", err
	}
	result, effectErr := updates.agents.RecoverUpdate(ctx, request, goal, failure)
	observedErr := effectErr
	if effectErr == nil {
		if goal == UpdateFinish && result == UpdateReadyPrevious {
			observedErr = agentUpdateRolledBack()
		} else if goal == UpdateFinish && result != UpdateReadyDesired || goal == UpdateRestore && result != UpdateReadyPrevious {
			observedErr = errs.New(errs.KindStateConflict, "agent update reconciliation returned an unexpected image")
		}
	}
	if err := progress.Finish(stage, observedErr); err != nil {
		return "", err
	}
	return result, effectErr
}
