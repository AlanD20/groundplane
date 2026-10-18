package hostplatform

import (
	"context"
	taskdispatch "github.com/AlanD20/groundplane/internal/controller/controllertask/dispatch"

	"github.com/AlanD20/groundplane/internal/app/softwareplatform"
	"github.com/AlanD20/groundplane/internal/controller/controllertask"
	"github.com/AlanD20/groundplane/internal/controller/localagent"
)

func (platform *Platform) newSoftwareRuntime(
	dependencies ExecutionDependencies,
) (*softwareplatform.Composition, controllertask.Handler, error) {
	handler, err := taskdispatch.NewResourceHandler(
		platform.agents,
	)
	if err != nil {
		return nil, nil, err
	}
	software, err := softwareplatform.New(softwareplatform.Dependencies{
		Store: platform.dependencies.Store, Tasks: platform.dependencies.Tasks, Evidence: platform.dependencies.Idempotency, Intents: platform.dependencies.Intents,
		Agents: platform.agents, AgentUpdates: platform.AgentMutations, ControllerUpdates: platform.Upgrades,
		Releases: platform.releases, WorkspaceRoot: platform.dependencies.Config.Storage.VolumeRoot,
		Interval: platform.dependencies.Tick, Logger: platform.dependencies.Logger,
	})
	if err != nil {
		return nil, nil, err
	}
	platform.preparations = software.Preparations
	return software, handler, nil
}

// Native Task composition establishes recovery admission before NewController
// returns; no scheduler or Agent listener is running during this barrier.
func (platform *Platform) newTaskRuntime(
	ctx context.Context,
	handler controllertask.Handler,
	dependencies ExecutionDependencies,
) (*controllertask.Runner, error) {
	updates, err := localagent.NewTaskUpdates(
		platform.agents,
		platform.dependencies.Channel.Registry,
		platform.dependencies.Logger,
	)
	if err != nil {
		return nil, err
	}
	runtime, err := controllertask.New(
		platform.dependencies.Tasks,
		handler,
		updates,
		platform.dependencies.Tick,
		platform.dependencies.Logger,
	)
	if err != nil {
		return nil, err
	}
	if err := runtime.Restore(ctx); err != nil {
		return nil, err
	}
	return runtime, nil
}
