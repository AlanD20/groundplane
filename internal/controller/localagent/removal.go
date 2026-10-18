package localagent

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

func (manager *Manager) resumeDelete(ctx context.Context, stored StoredRecord) error {
	record := stored.Record
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.tasks.AbortActive(
		ctx,
		record.ID,
		record.Generation,
		record.Config.MaxConcurrentTasks,
		RemovedTaskReason,
	); err != nil {
		return safePortError(ctx, err, "local agent active task abortion failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if record.Phase != PhaseDeleting {
		deleting, err := manager.repository.BeginDelete(ctx, record.ID, record.Generation, stored.Revision)
		if err != nil {
			return safePortError(ctx, err, "local agent durable revocation failed")
		}
		if err := validateStored(deleting); err != nil {
			return err
		}
		if deleting.Record.Phase != PhaseDeleting || deleting.Record.Credential.Digest != "" {
			return errs.New(errs.KindInternal, "local agent repository did not durably revoke the credential")
		}
		stored = deleting
		record = deleting.Record
	}
	// Revocation is durable before the session fence. If publication was
	// ambiguous, recovery can revision-fence it while forward work stays closed.
	if err := manager.sessions.StopAssignments(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent assignment fencing failed")
	}

	if err := manager.sessions.Revoke(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent session revocation failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.sessions.WaitOffline(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent offline wait failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.container.Remove(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent container removal failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.runtime.Remove(ctx, record.ID); err != nil {
		return safePortError(ctx, err, "local agent runtime removal failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := manager.repository.Delete(ctx, record.ID, record.Generation, stored.Revision); err != nil {
		if isAgentNotFound(err) {
			return nil
		}
		return safePortError(ctx, err, "local agent durable deletion failed")
	}
	return nil
}

// Remove never follows a reused label or a replacement generation.
// It is also the cleanup owner for an interrupted removal of this exact Agent.
func (manager *Manager) Remove(ctx context.Context, agentID, image string, generation uint64) error {
	if err := manager.enter(ctx); err != nil {
		return err
	}
	defer manager.leave()
	stored, err := manager.repository.GetSingleton(ctx)
	if isAgentNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateStored(stored); err != nil {
		return err
	}
	if stored.Record.ID != agentID || stored.Record.Image != image || stored.Record.Generation != generation {
		return errs.New(errs.KindStateConflict, "Agent removal no longer matches its captured image and generation")
	}
	return manager.resumeDelete(ctx, stored)
}

// RecoverRemoval either completes already-revoked removal or safely releases a
// pre-revocation fence. It never begins a previously unstarted deletion.
func (manager *Manager) RecoverRemoval(ctx context.Context, agentID, image string, generation uint64) (bool, error) {
	if err := manager.enter(ctx); err != nil {
		return false, err
	}
	defer manager.leave()
	stored, err := manager.repository.GetSingleton(ctx)
	if isAgentNotFound(err) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if err := validateStored(stored); err != nil {
		return false, err
	}
	if stored.Record.ID != agentID || stored.Record.Image != image || stored.Record.Generation != generation {
		return false, errs.New(errs.KindStateConflict, "Agent removal recovery identity changed")
	}
	if stored.Record.Phase == PhaseDeleting {
		return true, manager.resumeDelete(ctx, stored)
	}
	if stored.Record.Phase != PhaseReady {
		return false, errs.New(errs.KindStateConflict, "Agent pre-removal state has not reached Ready")
	}
	if err := manager.convergeRuntime(ctx, stored.Record); err != nil {
		return false, err
	}
	if err := manager.waitReplacementReady(ctx, stored.Record); err != nil {
		return false, err
	}
	if err := manager.tasks.ResumeForward(ctx, agentID, generation); err != nil {
		return false, err
	}
	return false, nil
}

// Bound removal recovery independently from the caller's cancelled forward pass.
const RemovalRecoveryTimeout = 130 * time.Second
