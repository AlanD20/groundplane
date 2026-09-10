package localagent

import (
	"context"
	"math"

	"github.com/AlanD20/groundplane/pkg/errs"
)

type UpdateGoal string

const (
	UpdateFinish  UpdateGoal = "finish"
	UpdateRestore UpdateGoal = "restore"
)

type UpdateResult string

const (
	UpdateReadyDesired  UpdateResult = "ready_desired"
	UpdateReadyPrevious UpdateResult = "ready_previous"
)

// RecoverUpdate resumes Task-pinned replacement or restores its predecessor,
// including a candidate that reached Ready before native Controller rollback.
// The caller owns an all-generation Task admission hold, restored before the
// channel starts and retained across errors until this method proves readiness.
// A timeout is not permission to discard that durable recovery authority.
func (manager *Manager) RecoverUpdate(
	ctx context.Context,
	request UpdateRequest,
	goal UpdateGoal,
) (UpdateResult, error) {
	if ctx == nil {
		return "", errs.New(errs.KindInternal, "local agent recovery context is required")
	}
	if err := validateUpdateRequest(request); err != nil {
		return "", err
	}
	if request.StartingGeneration > math.MaxUint64-2 || goal != UpdateFinish && goal != UpdateRestore {
		return "", errs.New(errs.KindValidationFailed, "local agent recovery goal is invalid")
	}
	// Recovery chooses its own direction; ordinary enter would first finish a
	// retained candidate even when this Task now requires predecessor recovery.
	select {
	case manager.gate <- struct{}{}:
		defer manager.leave()
	case <-ctx.Done():
		return "", ctx.Err()
	}
	if manager.pending != nil && manager.pending.request != request {
		return "", errs.New(errs.KindStateConflict, "local agent belongs to another unresolved update")
	}
	stored, err := manager.fenceRecoveryAttempt(ctx, request)
	if err != nil {
		return "", err
	}
	if manager.pending != nil {
		// The revision barrier resolved the lost attempt. A later generation
		// must own the irreversible fence before the attempt's hold is dropped.
		if stored.Record.Generation > request.StartingGeneration {
			if err := manager.sessions.FenceThrough(ctx, request.AgentID, stored.Record.Generation-1); err != nil {
				return "", err
			}
		}
		manager.pending.resume()
		manager.pending = nil
	}
	var executionErr error
	if goal == UpdateFinish {
		executionErr = manager.update(ctx, request)
	} else {
		executionErr = manager.restoreUpdate(ctx, request, stored)
	}
	if ctx.Err() != nil {
		return "", ctx.Err()
	}
	current, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return "", err
	}
	if err := validateRecoveryRecord(current, request); err != nil {
		return "", err
	}
	result := recoveredImage(current.Record, request, goal)
	if current.Record.Phase != PhaseReady || result == "" {
		if executionErr != nil {
			return "", executionErr
		}
		return "", errs.New(errs.KindStateConflict, "local agent update recovery has not settled")
	}
	// Persisted Ready from an earlier Controller process is not proof of a
	// currently authenticated Agent; re-materialize idempotently and await it.
	if err := manager.convergeRuntime(ctx, current.Record); err != nil {
		return "", err
	}
	if err := manager.waitReplacementReady(ctx, current.Record); err != nil {
		return "", err
	}
	return result, nil
}

func (manager *Manager) fenceRecoveryAttempt(ctx context.Context, request UpdateRequest) (StoredRecord, error) {
	current, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return StoredRecord{}, err
	}
	if err := validateRecoveryRecord(current, request); err != nil {
		return StoredRecord{}, err
	}
	resolved, err := manager.repository.FenceReplacementAttempt(
		ctx, request.AgentID, current.Record.Generation, current.Revision,
	)
	if err != nil {
		return StoredRecord{}, err
	}
	if err := validateRecoveryRecord(resolved, request); err != nil {
		return StoredRecord{}, err
	}
	if resolved.Revision <= current.Revision {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent recovery barrier returned stale authority")
	}
	return resolved, nil
}

func validateRecoveryRecord(stored StoredRecord, request UpdateRequest) error {
	if err := validateStored(stored); err != nil {
		return err
	}
	record := stored.Record
	if record.ID != request.AgentID || record.Phase != PhaseReady && record.Phase != PhaseUpdating {
		return errs.New(errs.KindStateConflict, "local agent recovery identity changed")
	}
	if record.Generation == request.StartingGeneration && record.Image == request.PreviousImage &&
		record.Phase == PhaseReady ||
		record.Generation == request.StartingGeneration+1 && record.Image == request.DesiredImage ||
		record.Generation == request.StartingGeneration+2 && record.Image == request.PreviousImage {
		return nil
	}
	return errs.New(errs.KindStateConflict, "local agent recovery generation changed")
}

func (manager *Manager) restoreUpdate(ctx context.Context, request UpdateRequest, stored StoredRecord) error {
	if stored.Record.Generation == request.StartingGeneration+1 {
		if err := manager.tasks.RequireIdle(
			ctx, request.AgentID, stored.Record.Generation, stored.Record.Config.MaxConcurrentTasks,
		); err != nil {
			return err
		}
		return manager.rollbackUpdate(ctx, request, stored)
	}
	if stored.Record.Phase == PhaseUpdating {
		return manager.resumeReplacement(ctx, stored)
	}
	return nil
}

func recoveredImage(record Record, request UpdateRequest, goal UpdateGoal) UpdateResult {
	if goal == UpdateFinish && record.Generation == request.StartingGeneration+1 &&
		record.Image == request.DesiredImage {
		return UpdateReadyDesired
	}
	if record.Image == request.PreviousImage && (record.Generation == request.StartingGeneration+2 ||
		goal == UpdateRestore && record.Generation == request.StartingGeneration) {
		return UpdateReadyPrevious
	}
	return ""
}
