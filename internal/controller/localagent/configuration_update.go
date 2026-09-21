package localagent

import (
	"context"
	"github.com/AlanD20/groundplane/pkg/errs"
)

// UpdateConfig replaces the complete Controller-owned runtime config. The
// repository atomically commits desired state and exact replay evidence before
// host materialization. A replay never requires the target Agent to still exist.
func (manager *Manager) UpdateConfig(
	ctx context.Context,
	agentID string,
	config Config,
	idempotencyKey string,
) ([]byte, error) {
	if err := manager.enter(ctx); err != nil {
		return nil, err
	}
	defer manager.leave()
	if err := validateAgentID(agentID); err != nil {
		return nil, err
	}
	if config.PullIntervalSeconds <= 0 || config.MaxConcurrentTasks <= 0 {
		return nil, errs.New(errs.KindValidationFailed, "agent runtime config limits must be positive")
	}
	if !validLabels(config.Labels) {
		return nil, errs.New(errs.KindValidationFailed, "agent labels must contain valid NUL-free UTF-8")
	}
	result, err := manager.repository.UpdateConfig(
		ctx,
		agentID,
		cloneConfig(config),
		idempotencyKey,
	)
	if err != nil {
		return nil, safePortError(ctx, err, "local agent durable config replacement failed")
	}
	if len(result.ResponseBody) == 0 {
		return nil, errs.New(errs.KindInternal, "local agent config replacement response is empty")
	}
	if !result.Applied {
		return append([]byte(nil), result.ResponseBody...), nil
	}
	updated := result.Stored
	if err := validateStored(updated); err != nil {
		return nil, err
	}
	if updated.Record.ID != agentID || updated.Record.Phase == PhaseDeleting ||
		!equalConfig(updated.Record.Config, config) {
		return nil, errs.New(
			errs.KindInternal,
			"local agent repository replaced config on the wrong lifecycle record",
		)
	}
	if err := manager.runtime.Materialize(ctx, RuntimeMaterial{
		AgentID:        updated.Record.ID,
		Generation:     updated.Record.Generation,
		Config:         cloneConfig(updated.Record.Config),
		EncryptedToken: append([]byte(nil), updated.Record.Credential.EncryptedToken...),
	}); err != nil {
		return nil, safePortError(ctx, err, "local agent runtime materialization failed")
	}
	return append([]byte(nil), result.ResponseBody...), nil
}

func equalConfig(left Config, right Config) bool {
	if left.PullIntervalSeconds != right.PullIntervalSeconds ||
		left.MaxConcurrentTasks != right.MaxConcurrentTasks || len(left.Labels) != len(right.Labels) {
		return false
	}
	for key, value := range left.Labels {
		if right.Labels[key] != value {
			return false
		}
	}
	return true
}
