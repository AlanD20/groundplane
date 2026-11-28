package agent

import (
	"context"

	"github.com/AlanD20/groundplane/proto/agentpb"
)

func (c *Client) openExecutionWorkers(
	ctx context.Context,
	config *agentpb.AgentConfig,
	exchange *nativeExchange,
) (*executionWorkers, error) {
	workers, err := newExecutionWorkers(
		ctx,
		runtimeConcurrency(config),
		exchange.identity,
		c.releaseExecutor,
		c.releaseRecoveryExecutor,
		c.scriptExecutor,
		c.hierarchyExecutor,
		c.attachRemovalExecutor,
		c.volumeExecutor,
		c.networkExecutor,
		c.componentExecutor,
		c.entryExecutor,
		c.attachProvisionExecutor,
		c.lifecycleExecutor,
		c.removalRecovery,
		c.backupExecutor,
		c.environmentExecutor,
		c.backingExecutor,
		c.resolverExecutor,
		exchange,
		c.processOwnership,
	)
	if err != nil {
		return nil, err
	}
	workers.publicIngress = c.publicIngress
	workers.remoteImage = c.remoteImage
	c.executionWorkers = workers
	return workers, nil
}
