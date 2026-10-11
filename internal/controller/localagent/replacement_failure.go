package localagent

import (
	"context"

	"github.com/AlanD20/groundplane/internal/common/taskprogress"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// ReplacementFailure reads only the restored generation belonging to this
// pinned update. An older failure cannot become the cause of a newer Task.
func (manager *Manager) ReplacementFailure(ctx context.Context, request UpdateRequest) (*taskprogress.Failure, error) {
	if err := validateUpdateRequest(request); err != nil {
		return nil, err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return nil, err
	}
	if err := validateRecoveryRecord(stored, request); err != nil {
		return nil, err
	}
	if stored.Record.Phase != PhaseReady {
		return nil, errs.New(errs.KindStateConflict, "agent update restoration is not ready")
	}
	if stored.Record.Generation != request.StartingGeneration+2 || stored.Record.Image != request.PreviousImage {
		return nil, nil
	}
	return taskprogress.CloneFailure(stored.Record.ReplacementFailure), nil
}
