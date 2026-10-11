package app

import (
	"log/slog"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	dockerrunner "github.com/AlanD20/groundplane/internal/infra/docker/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerchecks"
	"github.com/AlanD20/groundplane/internal/infra/runnerjournal"
)

func newControllerRunnerChecks(store etcd.EnvironmentBlueprintStore, runners *etcd.RunnerRepository,
	tasks *etcd.TaskRepository, evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
	lifecycle *runnercapability.Executor, logger *slog.Logger,
) (*runnercapability.ConnectionCheckService, *runnercapability.ConnectionCheckExecutor, error) {
	records, err := runnerchecks.NewRepository(store)
	if err != nil {
		return nil, nil, err
	}
	plans, err := runnerjournal.New(runnerJournalRoot)
	if err != nil {
		return nil, nil, err
	}
	probe, err := dockerrunner.NewConnectionChecker(commandrunner.New(logger))
	if err != nil {
		return nil, nil, err
	}
	executor, err := runnercapability.NewConnectionCheckExecutor(records, tasks, runners, probe, lifecycle)
	if err != nil {
		return nil, nil, err
	}
	service, err := runnercapability.NewConnectionCheckService(runners, plans, records, evidence, coordinator)
	if err != nil {
		return nil, nil, err
	}
	return service, executor, nil
}
