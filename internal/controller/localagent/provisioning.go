package localagent

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

func (manager *Manager) provision(ctx context.Context, stored StoredRecord) (StoredRecord, error) {
	if stored.Record.Phase != PhaseProvisioning {
		return StoredRecord{}, errs.New(errs.KindStateConflict, "local agent is not provisioning")
	}
	if err := manager.convergeRuntime(ctx, stored.Record); err != nil {
		return StoredRecord{}, err
	}
	readyContext, cancelReady := context.WithCancel(ctx)
	defer cancelReady()
	ready, err := manager.sessions.Ready(readyContext, stored.Record.ID, stored.Record.Generation)
	if err != nil {
		return StoredRecord{}, safePortError(ctx, err, "local agent readiness subscription failed")
	}
	if ready == nil {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent readiness subscription is nil")
	}
	timer := manager.clock.NewTimer(ReadyTimeout)
	if timer == nil {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent readiness timer is nil")
	}
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return StoredRecord{}, ctx.Err()
	case <-timer.C():
		return StoredRecord{}, errs.New(
			errs.KindTaskTimedOut,
			"agent did not report authenticated Ready within 120 seconds",
		)
	case <-ready:
	}
	if err := ctx.Err(); err != nil {
		return StoredRecord{}, err
	}
	updated, err := manager.repository.MarkReady(
		ctx,
		stored.Record.ID,
		stored.Record.Generation,
		stored.Revision,
		manager.clock.Now(),
	)
	if err != nil {
		return StoredRecord{}, safePortError(ctx, err, "local agent ready transition failed")
	}
	if err := validateStored(updated); err != nil {
		return StoredRecord{}, err
	}
	if updated.Record.Phase != PhaseReady {
		return StoredRecord{}, errs.New(errs.KindInternal, "local agent repository did not mark the record ready")
	}
	return updated, nil
}

func (manager *Manager) convergeRuntime(ctx context.Context, record Record) error {
	material := RuntimeMaterial{
		AgentID:        record.ID,
		Generation:     record.Generation,
		Config:         cloneConfig(record.Config),
		EncryptedToken: append([]byte(nil), record.Credential.EncryptedToken...),
	}
	if err := manager.runtime.Materialize(ctx, material); err != nil {
		return safePortError(ctx, err, "local agent runtime materialization failed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	desired := ContainerDesired{
		AgentID:    record.ID,
		Image:      record.Image,
		Generation: record.Generation,
	}
	if err := manager.container.Converge(ctx, desired); err != nil {
		return safePortError(ctx, err, "local agent container convergence failed")
	}
	return ctx.Err()
}
