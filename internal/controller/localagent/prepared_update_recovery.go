package localagent

import (
	"context"

	upgrade "github.com/AlanD20/groundplane/internal/common/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/common/taskprogress"
)

// RecoverPreparedUpdate is the native Controller handoff continuation. Its
// operation-scoped authority is reconstructed from the current native journal;
// an ordinary Agent update cannot opt into the retained-Entry admission path.
func (manager *Manager) RecoverPreparedUpdate(
	ctx context.Context,
	request UpdateRequest,
	goal UpdateGoal,
	failure *taskprogress.Failure,
	authority PreparedUpdateAuthority,
) (UpdateResult, error) {
	return manager.recoverUpdate(ctx, request, goal, failure, &authority)
}

func (manager *Manager) requirePreparedReplacementRollback(
	ctx context.Context,
	request UpdateRequest,
	current StoredRecord,
	authority PreparedUpdateAuthority,
) error {
	if err := authority.Validate(request, UpdateRestore); err != nil {
		return err
	}
	if !authority.AllowsCandidateRollback() {
		return preparedUpdateConflict()
	}
	maximum := current.Record.Config.MaxConcurrentTasks * current.Record.Config.MaxParallelOperationsPerTask
	if err := manager.tasks.RequireReplacementRollbackReady(
		ctx,
		authority.taskID,
		request.AgentID,
		request.StartingGeneration,
		current.Record.Generation,
		maximum,
	); err != nil {
		return safePortError(ctx, err, "local agent retained recovery is not ready for replacement rollback")
	}
	return nil
}

func preparedUpdateSettleOnly(
	stored StoredRecord,
	request UpdateRequest,
	goal UpdateGoal,
	authority PreparedUpdateAuthority,
) (bool, error) {
	switch authority.phase {
	case upgrade.PhaseHealthy:
		if goal != UpdateFinish || stored.Record.Generation != request.StartingGeneration+1 ||
			stored.Record.Image != request.DesiredImage || stored.Record.Phase != PhaseReady {
			return false, preparedUpdateConflict()
		}
		return true, nil
	case upgrade.PhaseRecovered:
		originalReady := stored.Record.Generation == request.StartingGeneration &&
			stored.Record.Image == request.PreviousImage && stored.Record.Phase == PhaseReady
		rollbackReady := stored.Record.Generation == request.StartingGeneration+2 &&
			stored.Record.Image == request.PreviousImage && stored.Record.Phase == PhaseReady
		if goal != UpdateRestore || !originalReady && !rollbackReady {
			return false, preparedUpdateConflict()
		}
		return true, nil
	default:
		return false, nil
	}
}
