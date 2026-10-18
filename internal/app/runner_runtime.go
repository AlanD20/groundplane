package app

import (
	"log/slog"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	dockerrunner "github.com/AlanD20/groundplane/internal/infra/docker/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/executiondiagnostics"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerwork"
	"github.com/AlanD20/groundplane/internal/infra/runnerjournal"
)

const runnerJournalRoot = "/var/lib/groundplane/runner-journal"

func newRunnerLifecycleExecutor(
	logger *slog.Logger,
	repository *etcd.RunnerRepository,
	broker *runnercapability.TokenBroker,
	inputs *runnerwork.Repository,
	diagnostics *executiondiagnostics.Repository,
) (*runnercapability.Executor, *runnercapability.Maintenance, error) {
	host, err := dockerrunner.NewLocal(commandrunner.New(logger))
	if err != nil {
		return nil, nil, err
	}
	journal, err := runnerjournal.New(runnerJournalRoot)
	if err != nil {
		return nil, nil, err
	}
	lifecycle, err := runnercapability.NewLifecycle(journal, host, diagnostics)
	if err != nil {
		return nil, nil, err
	}
	executor, err := runnercapability.NewExecutor(
		repository,
		inputs,
		lifecycle,
		journal,
		broker,
	)
	if err != nil {
		return nil, nil, err
	}
	maintenance, err := runnercapability.NewMaintenance(repository, host, executor, logger)
	return executor, maintenance, err
}
