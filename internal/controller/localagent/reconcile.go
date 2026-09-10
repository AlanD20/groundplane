package localagent

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// pendingReplacement retains only non-secret identity and one operation-owned
// release until unknown publication has been revision-fenced or recovered.
// All access is under the Manager lifecycle gate, including reconciliation.
type pendingReplacement struct {
	request   UpdateRequest
	revision  int64
	digest    string
	resume    func()
	published bool
}

// Reconcile resumes durable runtime state and any current-process unresolved
// publication before another lifecycle mutation is admitted.
func (manager *Manager) Reconcile(ctx context.Context) error {
	if err := manager.enter(ctx); err != nil {
		return err
	}
	defer manager.leave()
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
	switch stored.Record.Phase {
	case PhaseProvisioning:
		_, err = manager.provision(ctx, stored)
		return err
	case PhaseReady:
		return manager.convergeRuntime(ctx, stored.Record)
	case PhaseUpdating:
		return manager.resumeReplacement(ctx, stored)
	case PhaseDeleting:
		return manager.resumeDelete(ctx, stored)
	default:
		return errs.New(errs.KindInternal, "local agent record has an invalid lifecycle phase")
	}
}

func (manager *Manager) resolvePending(ctx context.Context) (StoredRecord, bool, error) {
	pending := manager.pending
	resolved, err := manager.repository.FenceReplacementAttempt(
		ctx,
		pending.request.AgentID,
		pending.request.StartingGeneration,
		pending.revision,
	)
	if err != nil {
		return StoredRecord{}, false, err
	}
	if err := validateStored(resolved); err != nil {
		return StoredRecord{}, false, err
	}
	if resolved.Record.ID != pending.request.AgentID || resolved.Revision <= pending.revision {
		return StoredRecord{}, false, errs.New(
			errs.KindInternal,
			"local agent replacement barrier returned stale authority",
		)
	}
	if resolved.Record.Generation == pending.request.StartingGeneration &&
		resolved.Record.Image == pending.request.PreviousImage && resolved.Record.Phase == PhaseReady {
		pending.resume()
		manager.pending = nil
		return resolved, false, nil
	}
	if resolved.Record.Generation != pending.request.StartingGeneration+1 ||
		resolved.Record.Image != pending.request.DesiredImage || resolved.Record.Credential.Digest != pending.digest ||
		(resolved.Record.Phase != PhaseUpdating && resolved.Record.Phase != PhaseReady) {
		return StoredRecord{}, false, errs.New(
			errs.KindStateConflict,
			"local agent replacement resolution changed identity",
		)
	}
	if err := manager.sessions.FenceThrough(ctx, pending.request.AgentID, pending.request.StartingGeneration); err != nil {
		return StoredRecord{}, false, err
	}
	pending.resume()
	pending.published = true
	return resolved, true, nil
}

func (manager *Manager) recoverPending(ctx context.Context) error {
	pending := manager.pending
	if pending == nil {
		return nil
	}
	if !pending.published {
		_, published, err := manager.resolvePending(ctx)
		if err != nil {
			return err
		}
		if !published {
			return nil
		}
	}
	err := manager.update(ctx, pending.request)
	if err == nil || manager.pendingSettled(ctx) {
		manager.pending = nil
		return nil
	}
	return err
}

// A readiness failure may already have completed rollback. Preserve unresolved
// recovery on read failure/cancellation, but do not block unrelated later work
// after either authenticated generation is durably ready.
func (manager *Manager) pendingSettled(ctx context.Context) bool {
	pending := manager.pending
	stored, err := manager.repository.GetSingleton(ctx)
	if err != nil || validateStored(stored) != nil || stored.Record.ID != pending.request.AgentID ||
		stored.Record.Phase != PhaseReady {
		return false
	}
	return stored.Record.Generation == pending.request.StartingGeneration+1 &&
		stored.Record.Image == pending.request.DesiredImage ||
		stored.Record.Generation == pending.request.StartingGeneration+2 &&
			stored.Record.Image == pending.request.PreviousImage
}
