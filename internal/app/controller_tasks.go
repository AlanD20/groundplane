package app

import (
	"context"
	taskdispatch "github.com/AlanD20/groundplane/internal/controller/controllertask/dispatch"
	"log/slog"
	"time"

	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	"github.com/AlanD20/groundplane/internal/controller/agentchannel"
	"github.com/AlanD20/groundplane/internal/controller/backupkey"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
	"github.com/AlanD20/groundplane/internal/infra/etcd"
)

func newControllerSoftwareRuntime(
	bootstrap controllerBootstrapComposition,
	authority controllerAuthorityComposition,
	platform *controllerPlatform,
	backingZones, images controllertask.Handler,
	runners *controllerRunnerComposition,
) (*softwareplatform.Composition, controllertask.Handler, error) {
	handler, err := taskdispatch.NewResourceHandler(
		platform.agents,
		backingZones,
		authority.runnerRecords,
		images,
		runners.lifecycle,
	)
	if err != nil {
		return nil, nil, err
	}
	software, err := softwareplatform.New(softwareplatform.Dependencies{
		Store: bootstrap.store, Tasks: authority.tasks, Evidence: authority.idempotency, Intents: authority.intentCoordinator,
		Agents: platform.agents, AgentUpdates: platform.mutations, ControllerUpdates: platform.upgrades,
		Releases: platform.releases, WorkspaceRoot: bootstrap.config.Storage.VolumeRoot,
		Interval: bootstrap.tick, Logger: bootstrap.logger,
	})
	if err != nil {
		return nil, nil, err
	}
	dispatcher, err := taskdispatch.NewSoftwarePreparationDispatcher(
		platform.etcdConfig.Dispatcher(handler),
		software.Preparations,
	)
	if err != nil {
		return nil, nil, err
	}
	return software, dispatcher, nil
}

// Native Task composition establishes recovery admission before NewController
// returns; no scheduler or Agent listener is running during this barrier.
func newControllerTaskRuntime(
	ctx context.Context,
	tasks *etcd.TaskRepository,
	handler controllertask.Handler,
	keys *backupkey.Service,
	hierarchy taskdispatch.HierarchyExecutor,
	agents *localagent.Manager,
	sessions *agentchannel.Registry,
	native controllertask.UpdateExecutor,
	interval time.Duration,
	logger *slog.Logger,
) (*controllertask.Runner, error) {
	updates, err := localagent.NewTaskUpdates(agents, sessions, logger)
	if err != nil {
		return nil, err
	}
	platformUpdates, err := controllertask.NewUpdateDispatcher(updates, native)
	if err != nil {
		return nil, err
	}
	rotations, err := controllertask.NewDispatcher(handler, keys)
	if err != nil {
		return nil, err
	}
	deletions, err := taskdispatch.NewHierarchyDispatcher(rotations, hierarchy)
	if err != nil {
		return nil, err
	}
	runtime, err := controllertask.New(tasks, deletions, platformUpdates, interval, logger)
	if err != nil {
		return nil, err
	}
	if err := runtime.Restore(ctx); err != nil {
		return nil, err
	}
	return runtime, nil
}
