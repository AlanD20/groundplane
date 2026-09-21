package localagent

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// Remove fences work and credentials before deleting runtime state. The durable
// record is always deleted last, making every preceding failure resumable.
func (manager *Manager) Remove(ctx context.Context, agentID string) error {
	if err := manager.enter(ctx); err != nil {
		return err
	}
	defer manager.leave()

	if err := validateAgentID(agentID); err != nil {
		return err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if isAgentNotFound(err) {
		return nil
	}
	if err != nil {
		return safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return err
	}
	if stored.Record.ID != agentID {
		return agentNotFound(agentID)
	}
	return manager.resumeDelete(ctx, stored)
}

func (manager *Manager) resumeDelete(ctx context.Context, stored StoredRecord) error {
	record := stored.Record
	if err := manager.sessions.StopAssignments(ctx, record.ID, record.Generation); err != nil {
		return safePortError(ctx, err, "local agent assignment fencing failed")
	}
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
