package app

import (
	"log/slog"

	"github.com/AlanD20/groundplane/internal/common/config"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/runnerallocation"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	corerunner "github.com/AlanD20/groundplane/internal/core/runner"
	dockerrunner "github.com/AlanD20/groundplane/internal/infra/docker/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/runnerjournal"
)

const runnerJournalRoot = "/var/lib/groundplane/runner-journal"

func newRunnerLifecycleExecutor(
	logger *slog.Logger,
	repository *etcd.RunnerRepository,
	broker *runnercapability.TokenBroker,
	policy corerunner.IsolationPolicy,
	pools config.AllocationPools,
) (*runnercapability.Executor, *runnercapability.Maintenance, error) {
	host, err := dockerrunner.NewLocal(commandrunner.New(logger))
	if err != nil {
		return nil, nil, err
	}
	journal, err := runnerjournal.New(runnerJournalRoot)
	if err != nil {
		return nil, nil, err
	}
	lifecycle, err := runnercapability.NewLifecycle(journal, host)
	if err != nil {
		return nil, nil, err
	}
	executor, err := runnercapability.NewExecutor(
		repository,
		lifecycle,
		journal,
		broker,
		runnerallocation.RunnerAllocationConfigFromPools(pools),
		policy,
	)
	if err != nil {
		return nil, nil, err
	}
	maintenance, err := runnercapability.NewMaintenance(repository, journal, host, executor, logger)
	return executor, maintenance, err
}
