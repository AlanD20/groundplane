package runnerassembly

import (
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/githubrunner"
	"log/slog"

	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	runnercapability "github.com/AlanD20/groundplane/internal/controller/runner"
	dockerrunner "github.com/AlanD20/groundplane/internal/infra/docker/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/runnerupdates"
)

func newSoftware(store etcd.EnvironmentBlueprintStore, runners *etcd.RunnerRepository,
	evidence *etcd.IdempotencyRepository, coordinator *requestidempotency.Coordinator,
	lifecycle *runnercapability.Executor, logger *slog.Logger,
) (*runnercapability.SoftwareService, *runnercapability.SoftwareExecutor, error) {
	records, err := runnerupdates.NewRepository(store)
	if err != nil {
		return nil, nil, err
	}
	probe, err := dockerrunner.NewSoftwareUpdater(commandrunner.New(logger))
	if err != nil {
		return nil, nil, err
	}
	queue, err := workqueue.NewRepository(store)
	if err != nil {
		return nil, nil, err
	}
	executor, err := runnercapability.NewSoftwareExecutor(records, runners, probe, lifecycle, queue)
	if err != nil {
		return nil, nil, err
	}
	service, err := runnercapability.NewSoftwareService(runners, records, evidence, coordinator, githubrunner.NewCatalog())
	if err != nil {
		return nil, nil, err
	}
	return service, executor, nil
}
