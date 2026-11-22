// Package servicereadplatform composes shared Service reads and optional
// monitoring runtimes. Neither collection nor history grants execution authority.
package servicereadplatform

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/environment"
	"github.com/AlanD20/groundplane/internal/controller/monitoring"
	"github.com/AlanD20/groundplane/internal/controller/servicelogs"
	"github.com/AlanD20/groundplane/internal/controller/serviceobservation"
	"github.com/AlanD20/groundplane/internal/controller/serviceread"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	environmentetcd "github.com/AlanD20/groundplane/internal/infra/etcd/environment"
	"github.com/AlanD20/groundplane/pkg/errs"
)

type Dependencies struct {
	Hierarchy   *etcd.HierarchyRepository
	Services    *etcd.ServiceRepository
	Zones       *etcd.ZoneRepository
	Releases    *etcd.ReleaseLedger
	Agents      *agentregistration.Repository
	Channel     *agentchannel.Registry
	Backing     monitoring.ReadBacking
	Consumers   monitoring.ReadConsumers
	Logger      *slog.Logger
	HistoryPath string
}

type Composition struct {
	Environments *environment.Reader
	Services     *serviceread.Reader
	Observations *serviceobservation.Reader
	Logs         *servicelogs.Service
	Monitoring   *monitoring.Reader
	telemetry    *telemetryRuntime
}

func New(ctx context.Context, dependencies Dependencies) (*Composition, error) {
	if ctx == nil || dependencies.Logger == nil {
		return nil, errs.New(errs.KindInternal, "Service read composition context and logger are required")
	}
	reads, err := serviceread.New(dependencies.Hierarchy, dependencies.Services)
	if err != nil {
		return nil, err
	}
	observer, err := serviceobservation.New(
		dependencies.Services,
		dependencies.Releases,
		dependencies.Channel,
		time.Now,
	)
	if err != nil {
		return nil, err
	}
	observations, err := serviceobservation.NewReader(dependencies.Agents, observer)
	if err != nil {
		return nil, err
	}
	environments := environment.NewEtcdReader(environmentetcd.NewRepository(dependencies.Hierarchy, dependencies.Zones))
	runtime, reader, err := newTelemetry(ctx, dependencies, observations)
	if err != nil {
		return nil, err
	}
	return &Composition{Environments: environments, Services: reads, Observations: observations,
		Logs:       servicelogs.New(environments, reads, dependencies.Releases, dependencies.Channel),
		Monitoring: reader, telemetry: runtime}, nil
}

func (composition *Composition) Run(ctx context.Context) { composition.telemetry.Run(ctx) }

func (composition *Composition) Close() error {
	if composition == nil || composition.telemetry == nil {
		return nil
	}
	return composition.telemetry.Close()
}
