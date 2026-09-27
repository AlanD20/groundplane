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
) (*runnercapability.Executor, error) {
	host, err := dockerrunner.NewLocal(commandrunner.New(logger))
	if err != nil {
		return nil, err
	}
	journal, err := runnerjournal.New(runnerJournalRoot)
	if err != nil {
		return nil, err
	}
	lifecycle, err := runnercapability.NewLifecycle(journal, host)
	if err != nil {
		return nil, err
	}
	return runnercapability.NewExecutor(
		repository,
		lifecycle,
		broker,
		runnerallocation.RunnerAllocationConfigFromPools(pools),
		policy,
	)
}
