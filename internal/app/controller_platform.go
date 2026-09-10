package app

import (
	"context"
	"crypto/rand"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/AlanD20/groundplane/internal/common/config"
	commandrunner "github.com/AlanD20/groundplane/internal/common/runner"
	"github.com/AlanD20/groundplane/internal/common/version"
	"github.com/AlanD20/groundplane/internal/controller"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/controllerupgrade"
	"github.com/AlanD20/groundplane/internal/controller/idempotentintent"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/agentcredential"
	"github.com/AlanD20/groundplane/internal/infra/controllerrelease"
	"github.com/AlanD20/groundplane/internal/infra/docker/agentcontainer"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
	"github.com/AlanD20/groundplane/internal/infra/hoststats"
	"github.com/AlanD20/groundplane/internal/infra/systemd"
)

type controllerPlatformDependencies struct {
	Config        config.ControllerConfig
	Key           controllerKey
	Store         etcd.Store
	Agents        *etcd.LocalAgentRepository
	Tasks         *etcd.TaskRepository
	Intents       *idempotentintent.Coordinator
	Idempotency   *etcd.IdempotencyRepository
	Channel       *agentChannelRuntime
	EtcdEndpoints []string
	Tick          time.Duration
	Logger        *slog.Logger
}

// controllerPlatform owns native release handles and the Agent container
// client for the same process lifetime. It composes platform modules only.
type controllerPlatform struct {
	agents         *localagent.Manager
	mutations      *agentMutationService
	reads          *localAgentReadService
	host           *controller.HostService
	reconciliation controllerScheduler
	upgrades       *controllerupgrade.Service
	native         controllertask.UpdateExecutor
	readiness      *controllerupgrade.Readiness
	container      *agentcontainer.Manager
	releases       *controllerrelease.Store
}

func (platform *controllerPlatform) Close() error {
	var releaseErr error
	if platform.releases != nil {
		releaseErr = platform.releases.Close()
	}
	return errors.Join(platform.container.Close(), releaseErr)
}

func newControllerPlatform(
	ctx context.Context,
	dependencies controllerPlatformDependencies,
) (_ *controllerPlatform, result error) {
	cfg := dependencies.Config
	configIdempotency, err := newDurableLocalAgentConfigIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	repository, err := newLocalAgentRepositoryAdapter(dependencies.Agents, configIdempotency)
	if err != nil {
		return nil, err
	}
	sessions, err := newLocalAgentSessionsAdapter(dependencies.Channel.registry)
	if err != nil {
		return nil, err
	}
	work, err := newLocalAgentTasksAdapter(dependencies.Tasks, dependencies.Channel.registry)
	if err != nil {
		return nil, err
	}
	cipher, err := newCredentialCipher(dependencies.Key)
	if err != nil {
		return nil, err
	}
	credentials, err := agentcredential.New(rand.Reader, cipher, cipher)
	if err != nil {
		return nil, err
	}
	runtime, err := newLocalAgentRuntimeAdapter(credentials, cfg.Log, cfg.Storage.VolumeRoot)
	if err != nil {
		return nil, err
	}
	container, err := agentcontainer.New(ctx)
	if err != nil {
		return nil, err
	}
	platform := &controllerPlatform{container: container}
	defer func() {
		if result != nil {
			_ = platform.Close()
		} // Constructor failure releases only its owned handles.
	}()
	containerAdapter, err := newLocalAgentContainerAdapter(container)
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
	platform.readiness, err = controllerupgrade.NewReadiness(dependencies.Store)
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
	if available {
		catalog = releases
		native, err := controllerupgrade.NewCoordinator(controllerupgrade.Dependencies{
			Journal: releases, Unit: unit, Agents: platform.agents, Work: work,
			Sessions: dependencies.Channel.registry, Readiness: platform.readiness,
			ProcessDigest: process, Logger: dependencies.Logger,
		})
		if err != nil {
			return nil, err
		}
		if err := native.RestoreJournal(ctx, dependencies.Tasks); err != nil {
			return nil, err
		}
		platform.native = native
	}
	platform.upgrades, err = controllerupgrade.NewService(controllerupgrade.ServiceDependencies{
		Catalog: catalog, Unit: unit, Agents: platform.agents, Tasks: dependencies.Tasks,
		Evidence: dependencies.Idempotency, Intents: dependencies.Intents,
		ProcessDigest: process, BootstrapAgentImage: cfg.Agent.Image,
	})
	if err != nil {
		return nil, err
	}
	defaults := localagent.Config{PullIntervalSeconds: cfg.Agent.Runtime.PullIntervalSeconds,
		MaxConcurrentTasks: cfg.Agent.Runtime.MaxConcurrentTasks, Labels: cfg.Agent.Runtime.Labels}
	enrollmentIdempotency, err := newDurableAgentEnrollmentIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	enrollments, err := newAgentEnrollmentService(
		platform.upgrades,
		defaults,
		dependencies.Tasks,
		enrollmentIdempotency,
	)
	if err != nil {
		return nil, err
	}
	updateIdempotency, err := newDurableAgentUpdateIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	updates, err := newAgentUpdateService(platform.upgrades, platform.agents, dependencies.Tasks, updateIdempotency)
	if err != nil {
		return nil, err
	}
	removalIdempotency, err := newDurableAgentRemovalIdempotency(dependencies.Intents, dependencies.Idempotency)
	if err != nil {
		return nil, err
	}
	removals, err := newAgentRemovalService(platform.agents, dependencies.Tasks, removalIdempotency)
	if err != nil {
		return nil, err
	}
	platform.mutations, err = newAgentMutationService(enrollments, updates, removals)
	if err != nil {
		return nil, err
	}
	platform.reconciliation, err = newLocalAgentReconciliation(platform.agents, dependencies.Tick, dependencies.Logger)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	platform.reads, err = newLocalAgentReadService(platform.agents, dependencies.Tasks, hostname)
	if err != nil {
		return nil, err
	}
	platform.host, err = controller.NewHostService(controller.HostDependencies{
		System: &hostSystemSnapshotSource{system: hoststats.New(), docker: container},
		Etcd: &hostEtcdSnapshotSource{
			endpoints: append([]string(nil), dependencies.EtcdEndpoints...),
			probe:     etcd.ProbeEndpoints,
		},
		Agent:             &hostAgentSnapshotSource{health: platform.agents, fallback: defaults},
		ControllerService: hostControllerUnit, ControllerVersion: version.Value,
	})
	if err != nil {
		return nil, err
	}
	dependencies.Channel.onReady = platform.readiness.MarkChannelReady
	return platform, nil
}
