package localagent

import (
	"context"
	"errors"
	"github.com/AlanD20/groundplane/pkg/errs"
	"time"
)

// ListHealth returns the singleton durable Agent with matching live-session
// telemetry, or an empty list when no Agent has been enrolled.
func (manager *Manager) ListHealth(ctx context.Context) ([]Health, error) {
	if ctx == nil {
		return nil, errs.New(errs.KindInternal, "local agent context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if errors.Is(err, errs.New(errs.KindAgentNotFound, "")) {
		return []Health{}, nil
	}
	if err != nil {
		return nil, safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return nil, err
	}
	return []Health{manager.projectHealth(stored.Record)}, nil
}

// Health projects online state from a matching generation's authenticated Ready
// heartbeat. Transport keepalive and a mismatched generation never count.
func (manager *Manager) Health(ctx context.Context, agentID string) (Health, error) {
	if ctx == nil {
		return Health{}, errs.New(errs.KindInternal, "local agent context is required")
	}
	if err := ctx.Err(); err != nil {
		return Health{}, err
	}
	if err := validateAgentID(agentID); err != nil {
		return Health{}, err
	}
	stored, err := manager.repository.GetSingleton(ctx)
	if err != nil {
		return Health{}, safePortError(ctx, err, "local agent durable record lookup failed")
	}
	if err := validateStored(stored); err != nil {
		return Health{}, err
	}
	if stored.Record.ID != agentID {
		return Health{}, agentNotFound(agentID)
	}

	return manager.projectHealth(stored.Record), nil
}

func (manager *Manager) projectHealth(record Record) Health {
	health := Health{Agent: projectAgent(record)}
	snapshot, ok := manager.sessions.Snapshot(record.ID)
	if !ok || snapshot.Generation != record.Generation {
		return health
	}
	health.LastReady = snapshot.LastReady
	health.Capacity = snapshot.Capacity
	health.Version = snapshot.Version
	if !snapshot.LastReady.IsZero() {
		health.StaleAfter = snapshot.LastReady.Add(StaleWindow(record.Config.PullIntervalSeconds))
	}
	health.Online = record.Phase != PhaseDeleting && snapshot.Online && !snapshot.Revoked &&
		!snapshot.LastReady.IsZero() && !manager.clock.Now().After(health.StaleAfter)
	health.Healthy = record.Phase == PhaseReady && health.Online
	return health
}

// StaleWindow is the locked Ready-heartbeat deadline shared by health
// projection and stale-assignment recovery.
func StaleWindow(pullIntervalSeconds int32) time.Duration {
	window := 3 * time.Duration(pullIntervalSeconds) * time.Second
	if window < 30*time.Second {
		return 30 * time.Second
	}
	return window
}
