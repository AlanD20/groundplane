package localagent

import (
	"context"
	"fmt"
	"time"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// Update rotates the idle singleton to one Task-pinned digest and generation.
// A Task replay resumes the durable updating generation or accepts its already
// ready result without rotating authority a second time.
func (manager *Manager) Update(ctx context.Context, request UpdateRequest) error {
	if err := manager.enter(ctx); err != nil {
		return err
	}
	defer manager.leave()
	err := manager.update(ctx, request)
	if manager.pending != nil && manager.pending.published {
		if err == nil || manager.pendingSettled(ctx) {
			manager.pending = nil
		}
	}
	return err
}

func (manager *Manager) update(ctx context.Context, request UpdateRequest) error {
	if err := validateUpdateRequest(request); err != nil {
		return err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return err
	}
	if stored.Record.ID != request.AgentID {
		return agentNotFound(request.AgentID)
	}

	if stored.Record.Generation == request.StartingGeneration+1 &&
		stored.Record.Image == request.DesiredImage {
		switch stored.Record.Phase {
		case PhaseReady:
			return nil
		case PhaseUpdating:
			return manager.completeUpdateOrRollback(ctx, request, stored)
		default:
			return errs.New(errs.KindStateConflict, "local agent replacement phase changed")
		}
	}
	if stored.Record.Generation == request.StartingGeneration+2 &&
		stored.Record.Image == request.PreviousImage {
		switch stored.Record.Phase {
		case PhaseReady:
			return agentUpdateRolledBack()
		case PhaseUpdating:
			if err := manager.resumeReplacement(ctx, stored); err != nil {
				return safePortError(ctx, err, "local agent update rollback failed")
			}
			return agentUpdateRolledBack()
		default:
			return errs.New(errs.KindStateConflict, "local agent rollback phase changed")
		}
	}
	if stored.Record.Generation != request.StartingGeneration ||
		stored.Record.Image != request.PreviousImage || stored.Record.Phase != PhaseReady {
		return errs.New(errs.KindStateConflict, "local agent no longer matches the update Task")
	}

	resume, err := manager.sessions.PauseAssignments(ctx, stored.Record.ID, stored.Record.Generation)
	if err != nil {
		return safePortError(ctx, err, "local agent assignment pause failed")
	}
	resumePreparation := true
	defer func() {
		if resumePreparation {
			resume()
		}
	}()
	if err := manager.tasks.RequireIdle(
		ctx,
		stored.Record.ID,
		stored.Record.Generation,
		stored.Record.Config.MaxConcurrentTasks,
	); err != nil {
		return safePortError(ctx, err, "local agent must be idle before update")
	}
	credential, err := manager.runtime.GenerateCredential(ctx, stored.Record.ID)
	if err != nil {
		return safePortError(ctx, err, "local agent update credential generation failed")
	}
	defer clear(credential.EncryptedToken)
	if err := validateCredential(credential, false); err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("local agent runtime returned an invalid replacement credential: %w", err),
		)
	}
	updatedAt := manager.clock.Now()
	if !isNonzeroUTC(updatedAt) {
		return errs.New(errs.KindInternal, "local agent clock returned a non-UTC time")
	}
	// A failed transaction response may have committed. Retain the hold until
	// durable replacement/reconciliation irrevocably fences the old generation.
	resumePreparation = false
	replacement, err := manager.repository.BeginReplacement(
		ctx,
		stored,
		request.DesiredImage,
		cloneCredential(credential),
		updatedAt,
	)
	if err != nil {
		originalErr := err
		manager.pending = &pendingReplacement{
			request:  request,
			revision: stored.Revision,
			digest:   credential.Digest,
			resume:   resume,
		}
		resolutionContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		resolved, published, resolveErr := manager.resolvePending(resolutionContext)
		cancel()
		if resolveErr != nil || !published {
			return safePortError(ctx, originalErr, "local agent durable replacement failed")
		}
		replacement = resolved
	}
	if err := validateStored(replacement); err != nil {
		return err
	}
	if replacement.Record.Generation != stored.Record.Generation+1 ||
		replacement.Record.Image != request.DesiredImage {
		return errs.New(errs.KindInternal, "local agent repository returned an invalid replacement")
	}
	if replacement.Record.Phase == PhaseReady {
		return nil
	}
	if replacement.Record.Phase != PhaseUpdating {
		return errs.New(errs.KindInternal, "local agent replacement phase is invalid")
	}
	if err := manager.sessions.Revoke(ctx, stored.Record.ID, stored.Record.Generation); err != nil {
		return safePortError(ctx, err, "local agent previous session revocation failed")
	}
	resumePreparation = true // Revoke now owns the irreversible generation fence.
	if err := manager.sessions.WaitOffline(ctx, stored.Record.ID, stored.Record.Generation); err != nil {
		return safePortError(ctx, err, "local agent previous session offline wait failed")
	}
	return manager.completeUpdateOrRollback(ctx, request, replacement)
}

func (manager *Manager) completeUpdateOrRollback(
	ctx context.Context,
	request UpdateRequest,
	replacement StoredRecord,
) error {
	replacementErr := manager.resumeReplacement(ctx, replacement)
	if replacementErr == nil {
		return nil
	}
	if ctx.Err() != nil {
		return replacementErr
	}
	current, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return safePortError(ctx, err, "local agent replacement state lookup failed")
	}
	if err := validateStored(current); err != nil {
		return err
	}
	if current.Record.ID != request.AgentID ||
		current.Record.Generation != request.StartingGeneration+1 ||
		current.Record.Image != request.DesiredImage {
		return errs.New(errs.KindStateConflict, "local agent replacement state changed before rollback")
	}
	if current.Record.Phase == PhaseReady {
		return nil
	}
	if current.Record.Phase != PhaseUpdating {
		return errs.New(errs.KindStateConflict, "local agent replacement cannot be rolled back")
	}
	return manager.rollbackUpdate(ctx, request, current)
}

func (manager *Manager) rollbackUpdate(
	ctx context.Context,
	request UpdateRequest,
	current StoredRecord,
) error {
	credential, err := manager.runtime.GenerateCredential(ctx, current.Record.ID)
	if err != nil {
		return safePortError(ctx, err, "local agent rollback credential generation failed")
	}
	defer clear(credential.EncryptedToken)
	if err := validateCredential(credential, false); err != nil {
		return errs.Wrap(
			errs.KindInternal,
			fmt.Errorf("local agent runtime returned an invalid rollback credential: %w", err),
		)
	}
	updatedAt := manager.clock.Now()
	if !isNonzeroUTC(updatedAt) {
		return errs.New(errs.KindInternal, "local agent clock returned a non-UTC time")
	}
	rollback, err := manager.repository.BeginReplacement(
		ctx,
		current,
		request.PreviousImage,
		cloneCredential(credential),
		updatedAt,
	)
	if err != nil {
		return safePortError(ctx, err, "local agent durable rollback failed")
	}
	if err := validateStored(rollback); err != nil {
		return err
	}
	if rollback.Record.Generation != request.StartingGeneration+2 ||
		rollback.Record.Image != request.PreviousImage || rollback.Record.Phase != PhaseUpdating {
		return errs.New(errs.KindInternal, "local agent repository returned an invalid rollback")
	}
	if err := manager.sessions.Revoke(ctx, current.Record.ID, current.Record.Generation); err != nil {
		return safePortError(ctx, err, "local agent replacement session revocation failed")
	}
	if err := manager.sessions.WaitOffline(ctx, current.Record.ID, current.Record.Generation); err != nil {
		return safePortError(ctx, err, "local agent replacement session offline wait failed")
	}
	if err := manager.resumeReplacement(ctx, rollback); err != nil {
		return safePortError(ctx, err, "local agent update rollback failed")
	}
	return agentUpdateRolledBack()
}

func agentUpdateRolledBack() error {
	return errs.New(errs.KindStateConflict, "agent update failed and the previous image was restored")
}

func (manager *Manager) resumeReplacement(ctx context.Context, stored StoredRecord) error {
	if stored.Record.Phase != PhaseUpdating {
		return errs.New(errs.KindStateConflict, "local agent is not updating")
	}
	if stored.Record.Generation <= initialGeneration {
		return errs.New(errs.KindInternal, "local agent replacement generation has no predecessor")
	}
	if err := manager.sessions.FenceThrough(
		ctx,
		stored.Record.ID,
		stored.Record.Generation-1,
	); err != nil {
		return safePortError(ctx, err, "local agent prior session fence failed")
	}
	if err := manager.convergeRuntime(ctx, stored.Record); err != nil {
		return err
	}
	readyContext, cancelReady := context.WithCancel(ctx)
	defer cancelReady()
	ready, err := manager.sessions.Ready(readyContext, stored.Record.ID, stored.Record.Generation)
	if err != nil {
		return safePortError(ctx, err, "local agent replacement readiness subscription failed")
	}
	if ready == nil {
		return errs.New(errs.KindInternal, "local agent replacement readiness subscription is nil")
	}
	timer := manager.clock.NewTimer(ReadyTimeout)
	if timer == nil {
		return errs.New(errs.KindInternal, "local agent replacement readiness timer is nil")
	}
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C():
		return errs.New(
			errs.KindTaskTimedOut,
			"agent replacement did not report authenticated Ready within 120 seconds",
		)
	case <-ready:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	updated, err := manager.repository.MarkReplacementReady(
		ctx,
		stored.Record.ID,
		stored.Record.Generation,
		stored.Revision,
	)
	if err != nil {
		return safePortError(ctx, err, "local agent replacement ready transition failed")
	}
	if err := validateStored(updated); err != nil {
		return err
	}
	if updated.Record.Phase != PhaseReady || !updated.Record.ReadyAt.Equal(stored.Record.ReadyAt) {
		return errs.New(errs.KindInternal, "local agent repository did not preserve replacement identity")
	}
	return nil
}
