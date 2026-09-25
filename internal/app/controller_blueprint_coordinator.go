package app

import (
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/blueprintcoordinator"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/blueprintunits"
)

func newBlueprintCoordinatorRuntime(
	store etcd.EnvironmentBlueprintStore,
	tasks *etcd.TaskRepository,
	planner blueprintcoordinator.Planner,
	children blueprintcoordinator.ChildPreparer,
	agents *agentchannel.Registry,
	interval time.Duration,
	logger *slog.Logger,
) (*blueprintcoordinator.Runner, error) {
	ledger, err := blueprintunits.NewRepository(store)
	if err != nil {
		return nil, err
	}
	return blueprintcoordinator.New(tasks, ledger, planner, children, agents, interval, logger)
}
