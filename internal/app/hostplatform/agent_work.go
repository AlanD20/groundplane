package hostplatform

import (
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentoperations"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
)

func (platform *Platform) newAgentWork(
	queue *workqueue.Repository,
	diagnostics *executiondiagnostics.Repository,
) (*agentmanagement.Work, error) {
	inputs, err := agentoperations.New(platform.dependencies.Store)
	if err != nil {
		return nil, err
	}
	return agentmanagement.NewWork(
		queue,
		inputs,
		platform.dependencies.Agents,
		platform.agents,
		platform.dependencies.Channel.Registry,
		diagnostics,
	)
}
