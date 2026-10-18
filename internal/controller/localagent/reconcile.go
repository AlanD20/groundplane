package localagent

import (
	"context"

	"github.com/AlanD20/groundplane/pkg/errs"
)

// pendingReplacement retains only non-secret identity and one operation-owned
// release until unknown publication has been revision-fenced or recovered.
// All access is under the Manager lifecycle gate, including reconciliation.
type pendingReplacement struct {
	request  UpdateRequest
	revision int64
	digest   string
	resume   func()
}

// Reconcile maintains only the installed Ready runtime. Transitional effects
// belong to the captured native operation and cannot be advanced by this loop.
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
	case PhaseReady:
		return manager.convergeRuntime(ctx, stored.Record)
	case PhaseProvisioning, PhaseUpdating, PhaseDeleting:
		// Only the captured native operation may decide completion versus
		// cleanup. The background runtime loop cannot advance those effects.
		return nil
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
	return resolved, true, nil
}
