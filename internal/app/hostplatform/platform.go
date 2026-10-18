// Package hostplatform owns the native host runtime and its Controller Task composition.
package hostplatform

import (
	"context"
	"crypto/rand"
	"errors"
	channeltransport "github.com/AlanD20/groundplane/internal/controller/agentchannel/transport"
	"github.com/AlanD20/groundplane/internal/controller/agentmanagement"
	agentruntime "github.com/AlanD20/groundplane/internal/controller/localagent/runtime"
	agentregistration "github.com/AlanD20/groundplane/internal/infra/etcd/agentregistration"
	etcdstore "github.com/AlanD20/groundplane/internal/infra/etcd/keyvalue"
	"log/slog"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/version"

	"github.com/AlanD20/groundplane/internal/controller/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/etcdconfig"
	"github.com/AlanD20/groundplane/internal/controller/host"
	requestidempotency "github.com/AlanD20/groundplane/internal/controller/idempotency"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/controller/secretvalue"
	"github.com/AlanD20/groundplane/internal/controller/softwarepreparation"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"github.com/AlanD20/groundplane/internal/infra/controllerrelease"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/internal/infra/docker/etcdcontainer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/etcd/etcdconfiguration"
	"github.com/AlanD20/groundplane/internal/infra/etcd/workqueue"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/internal/infra/systemd"
)

type Dependencies struct {
	Config             config.ControllerConfig
	Key                agentruntime.ControllerKey
	Protector          *secretvalue.Protector
	Store              etcdstore.Store
	Agents             *agentregistration.Repository
	Tasks              *etcd.TaskRepository
	Intents            *requestidempotency.Coordinator
	Idempotency        *etcd.IdempotencyRepository
	Channel            *channeltransport.Runtime
	EtcdEndpoints      []string
	EtcdLifecycle      *etcdcontainer.Manager
	StorageMaintenance *etcd.StorageMaintenance
	Tick               time.Duration
	Logger             *slog.Logger
}

// Platform owns native release handles and the Agent container
// client for the same process lifetime. It composes platform modules only.
type Platform struct {
	dependencies   Dependencies
	agents         *localagent.Manager
	AgentMutations *agentmanagement.MutationService
	AgentReads     *agentmanagement.ReadService
	Host           *host.HostService
	reconciliation reconciliationScheduler
	Upgrades       *controllerupgrade.Service
	EtcdConfig     *etcdconfig.Service
	preparations   *softwarepreparation.Service
	controllerWork *controllerupgrade.Work
	Readiness      *controllerupgrade.Readiness
	container      *agentcontainer.Manager
	releases       *controllerrelease.Store
}

func (platform *Platform) Close() error {
	var releaseErr error
	if platform.releases != nil {
		releaseErr = platform.releases.Close()
	}
	return errors.Join(platform.container.Close(), releaseErr)
}

func New(
	ctx context.Context,
	dependencies Dependencies,
) (_ *Platform, result error) {
	cfg := dependencies.Config
	configIdempotency, err := agentruntime.NewConfigIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	repository, err := agentruntime.NewRepository(dependencies.Agents, configIdempotency)
	if err != nil {
		return nil, err
	}
	sessions, err := agentruntime.NewSessions(dependencies.Channel.Registry)
	if err != nil {
		return nil, err
	}
	work, err := agentruntime.NewTasks(dependencies.Tasks, dependencies.Channel.Registry)
	if err != nil {
		return nil, err
	}
	cipher, err := agentruntime.NewCredentialCipher(dependencies.Key)
	if err != nil {
		return nil, err
	}
	credentials, err := agentcredential.New(rand.Reader, cipher, cipher)
	if err != nil {
		return nil, err
	}
	runtime, err := agentruntime.NewCredentials(credentials, cfg.Log, cfg.Storage.VolumeRoot)
	if err != nil {
		return nil, err
	}
	container, err := agentcontainer.New(ctx, cfg.Storage.VolumeRoot)
	if err != nil {
		return nil, err
	}
	platform := &Platform{container: container, dependencies: dependencies}
	defer func() {
		if result != nil {
			_ = platform.Close()
		} // Constructor failure releases only its owned handles.
	}()
	containerAdapter, err := agentruntime.NewContainer(container)
	if err != nil {
		return nil, err
	}
	platform.agents, err = localagent.New(localagent.Dependencies{
		Repository: repository, Runtime: runtime, Container: containerAdapter,
		Sessions: sessions, Tasks: work, Clock: localagent.SystemClock{},
	})
	if err != nil {
		return nil, err
	}
	platform.Readiness, err = controllerupgrade.NewReadiness(dependencies.Store)
	if err != nil {
		return nil, err
	}
	unit, err := systemd.NewControllerUpgrade(commandrunner.New(dependencies.Logger))
	if err != nil {
		return nil, err
	}
	process, err := controllerrelease.ExecutingDigest(ctx)
	if err != nil {
		return nil, err
	}
	releases, available, err := controllerrelease.OpenOptional(ctx)
	if err != nil {
		return nil, err
	}
	platform.releases = releases
	var catalog controllerupgrade.ReleaseCatalog
	var native *controllerupgrade.Coordinator
	if available {
		catalog = releases
		native, err = controllerupgrade.NewCoordinator(controllerupgrade.Dependencies{
			Journal: releases, Unit: unit, Agents: platform.agents, Work: work,
			Sessions: dependencies.Channel.Registry, Readiness: platform.Readiness,
			ProcessDigest: process, Logger: dependencies.Logger,
		})
		if err != nil {
			return nil, err
		}
	}
	controllerRecords, controllerWork, err := platform.newControllerSoftware(ctx, native)
	if err != nil {
		return nil, err
	}
	platform.controllerWork = controllerWork
	platform.Upgrades, err = controllerupgrade.NewService(controllerupgrade.ServiceDependencies{
		Catalog: catalog, Unit: unit, Agents: platform.agents, Tasks: dependencies.Tasks, Records: controllerRecords,
		Evidence: dependencies.Idempotency, Intents: dependencies.Intents,
		ProcessDigest: process, BootstrapAgentImage: cfg.Agent.Image,
	})
	if err != nil {
		return nil, err
	}
	defaults := localagent.Config{PullIntervalSeconds: cfg.Agent.Runtime.PullIntervalSeconds,
		MaxConcurrentTasks: cfg.Agent.Runtime.MaxConcurrentTasks, Labels: cfg.Agent.Runtime.Labels}
	enrollmentIdempotency, err := agentmanagement.NewEnrollmentIdempotency(
		dependencies.Intents,
		dependencies.Idempotency,
	)
	if err != nil {
		return nil, err
	}
	enrollments, err := agentmanagement.NewEnrollmentService(
		platform.Upgrades,
		defaults,
		dependencies.Tasks,
		enrollmentIdempotency,
	)
	if err != nil {
		return nil, err
	}
	updateIdempotency, err := agentmanagement.NewUpdateIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	updates, err := agentmanagement.NewUpdateService(platform.agents, dependencies.Tasks, updateIdempotency)
	if err != nil {
		return nil, err
	}
	removalIdempotency, err := agentmanagement.NewRemovalIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	removals, err := agentmanagement.NewRemovalService(platform.agents, dependencies.Tasks, removalIdempotency)
	if err != nil {
		return nil, err
	}
	platform.AgentMutations, err = agentmanagement.NewMutationService(enrollments, updates, removals)
	if err != nil {
		return nil, err
	}
	platform.reconciliation, err = agentruntime.NewReconciliation(
		platform.agents,
		dependencies.Tick,
		dependencies.Logger,
	)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	platform.AgentReads, err = agentmanagement.NewReadService(platform.agents, dependencies.Tasks, hostname)
	if err != nil {
		return nil, err
	}
	hostDependencies := host.HostDependencies{
		System: host.NewSystemSource(hoststats.New(), container),
		Etcd: host.NewEtcdSource(
			dependencies.EtcdEndpoints,
			etcd.ProbeEndpoints,
			dependencies.EtcdLifecycle,
		),
		Agent:             host.NewAgentSource(platform.agents, defaults),
		Updates:           platform.Upgrades,
		ControllerService: host.ControllerUnit, ControllerVersion: version.Value,
		ControllerSource: version.Provenance(),
	}
	if dependencies.StorageMaintenance != nil {
		hostDependencies.Maintenance = dependencies.StorageMaintenance
	}
	platform.Host, err = host.NewHostService(hostDependencies)
	if err != nil {
		return nil, err
	}
	dependencies.Channel.OnReady = platform.Readiness.MarkChannelReady
	etcdInputs, err := etcdconfiguration.New(dependencies.Store)
	if err != nil {
		return nil, err
	}
	queue, err := workqueue.NewRepository(dependencies.Store)
	if err != nil {
		return nil, err
	}
	platform.EtcdConfig, err = etcdconfig.New(
		dependencies.EtcdLifecycle.Configuration(), dependencies.EtcdLifecycle,
		dependencies.Tasks, etcdInputs, queue, dependencies.Idempotency, dependencies.Intents,
	)
	if err != nil {
		return nil, err
	}
	return platform, nil
}

type reconciliationScheduler interface {
	Run(context.Context)
}

// Run owns the managed Agent reconciliation loop for the host lifetime.
func (platform *Platform) Run(ctx context.Context) {
	platform.reconciliation.Run(ctx)
}
