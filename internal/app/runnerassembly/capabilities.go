// Package runnerassembly wires the Runner-specific operator capabilities.
package runnerassembly

import (
	"log/slog"

	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/runner"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

type Capabilities struct {
	Checks           *runner.ConnectionCheckService
	CheckExecutor    *runner.ConnectionCheckExecutor
	Software         *runner.SoftwareService
	SoftwareExecutor *runner.SoftwareExecutor
}

func New(
	store etcd.EnvironmentBlueprintStore,
	records *etcd.RunnerRepository,
	evidence *etcd.IdempotencyRepository,
	coordinator *requestidempotency.Coordinator,
	lifecycle *runner.Executor,
	logger *slog.Logger,
) (Capabilities, error) {
	checks, checkExecutor, err := newChecks(store, records, evidence, coordinator, lifecycle, logger)
	if err != nil {
		return Capabilities{}, err
	}
	software, softwareExecutor, err := newSoftware(store, records, evidence, coordinator, lifecycle, logger)
	if err != nil {
		return Capabilities{}, err
	}
	return Capabilities{
		Checks:           checks,
		CheckExecutor:    checkExecutor,
		Software:         software,
		SoftwareExecutor: softwareExecutor,
	}, nil
}
